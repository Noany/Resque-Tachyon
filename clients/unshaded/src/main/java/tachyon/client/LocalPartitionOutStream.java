package tachyon.client;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.thrift.BenefitInfo;
import tachyon.thrift.PartitionInfo;
import tachyon.util.CommonUtils;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

/**
 * Created by zengdan on 15-11-17.
 */
public class LocalPartitionOutStream extends BlockOutStream{
    private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

    //private final int mBlockIndex;
    private final long mBlockCapacityByte;
    private final long[] mBlockIds;
    //private final long mBlockOffset;
    private final Closer[] mClosers;
    private final String[] mLocalFilePaths;
    private final RandomAccessFile[] mLocalFiles;
    private final FileChannel[] mLocalFileChannels;
    private final ByteBuffer mBuffer;
    // The size of the write buffer in bytes.
    private final long mBufferBytes;
    private final int numBlocks;

    private int mCurrentBlockIndex = -1;
    private long mAvailableBytes = 0;
    private long mInFileBytes = 0;
    private long mWrittenBytes = 0;

    private boolean mCanWrite = false;
    private boolean mClosed = false;


    /**
     * @param file the file the block belongs to
     * @param opType the OutStream's write type
     * @param totalSize the initial size bytes that will be allocated to the block
     * @param tachyonConf the TachyonConf instance for this file output stream.
     * @throws IOException
     */
    LocalPartitionOutStream(TachyonFile file, WriteType opType, long totalSize,
                        int id, int index, BenefitInfo benefit, TachyonConf tachyonConf) throws IOException {
        super(file, opType, tachyonConf);

        // BlockOutStream.get() already checks for the local worker, but this verifies the local worker
        // in case LocalBlockOutStream is constructed directly.
        Preconditions.checkState(mTachyonFS.hasLocalWorker());

        if (!opType.isCache()) {
            throw new IOException("LocalBlockOutStream only supports WriteType.CACHE. opType: " + opType);
        }

        mBlockCapacityByte = mFile.getBlockSizeByte();
        numBlocks = (int)(totalSize/mBlockCapacityByte) + (totalSize%mBlockCapacityByte == 0 ? 0 : 1);
        mBlockIds = new long[numBlocks];
        mLocalFiles = new RandomAccessFile[numBlocks];
        mLocalFileChannels = new FileChannel[numBlocks];

        PartitionInfo partitionInfo = new PartitionInfo(id, index, benefit, mBlockCapacityByte*numBlocks);


        for (int i = 0; i < numBlocks; i++) {
            long blockId = mFile.getBlockId(i);
            mBlockIds[i] = blockId;
            partitionInfo.addToBlockIds(blockId);
        }

        List<String> paths = mTachyonFS.getLocalPartitionTemporaryPath(partitionInfo);
        mLocalFilePaths = paths.toArray(new String[paths.size()]);
        Preconditions.checkState(mLocalFilePaths.length == numBlocks);

        mClosers = new Closer[numBlocks];
        /*
        for (int i = 0; i < numBlocks; i++) {
            mClosers[i] = Closer.create();
            mLocalFiles[i] = mClosers[i].register(new RandomAccessFile(mLocalFilePaths[i], "rw"));
            mLocalFileChannels[i] = mClosers[i].register(mLocalFiles[i].getChannel());
            // change the permission of the temporary file in order that the worker can move it.
            CommonUtils.changeLocalFileToFullPermission(mLocalFilePaths[i]);
            // use the sticky bit, only the client and the worker can write to the block
            CommonUtils.setLocalFileStickyBit(mLocalFilePaths[i]);
            LOG.info(mLocalFilePaths[i] + " was created! tachyonFile: " + file + ", blockIndex: " + i
                    + ", blockCapacityByte: " + mBlockCapacityByte);

        }
        */

        mAvailableBytes = mBlockCapacityByte;
        mBufferBytes = mTachyonConf.getBytes(Constants.USER_FILE_BUFFER_BYTES, Constants.MB);
        mBuffer = ByteBuffer.allocate(Ints.checkedCast(mBufferBytes));

        mCanWrite = true;
    }

    private synchronized void appendCurrentBuffer(byte[] buf, int offset, int length)
            throws IOException {
        if (mAvailableBytes < length) {
            long bytesRequested = mTachyonFS.requestSpace(mFile.getBlockId(mCurrentBlockIndex), length - mAvailableBytes);
            if (bytesRequested + mAvailableBytes >= length) {
                mAvailableBytes += bytesRequested;
            } else {
                mCanWrite = false;
                throw new IOException(String.format("No enough space on local worker: fileId(%d)"
                        + " blockId(%d) requestSize(%d)", mFile.mFileId, mBlockIds[mCurrentBlockIndex], length - mAvailableBytes));
            }
        }

        MappedByteBuffer out = mLocalFileChannels[mCurrentBlockIndex].map(FileChannel.MapMode.READ_WRITE, mInFileBytes, length);
        out.put(buf, offset, length);
        CommonUtils.cleanDirectBuffer(out);
        mInFileBytes += length;
        mAvailableBytes -= length;
        mTachyonFS.getClientMetrics().incBytesWrittenLocal(length);
    }

    @Override
    public void cancel() throws IOException {
        if (!mClosed) {
            mClosers[mCurrentBlockIndex].close();
            mClosed = true;
            for (int i = 0; i < numBlocks; i++) {
                mTachyonFS.cancelBlock(mBlockIds[i]);
                LOG.info(String.format("Canceled output of block. blockId(%d) path(%s)", mBlockIds[i],
                        mLocalFilePaths[i]));
            }

        }
    }

    /**
     * @return true if the stream can write and is not closed, otherwise false
     */
    public boolean canWrite() {
        return !mClosed && mCanWrite;
    }

    @Override
    public void close() throws IOException {
        if (!mClosed) {
            flush();
            mClosers[mCurrentBlockIndex].close();
            if (mWrittenBytes%mBlockCapacityByte > 0) {
                mTachyonFS.cacheBlock(mBlockIds[mCurrentBlockIndex]);
                mTachyonFS.getClientMetrics().incBlocksWrittenLocal(1);
            }else{
                mTachyonFS.cancelBlock(mBlockIds[mCurrentBlockIndex]);
            }
            for (int i = mCurrentBlockIndex + 1; i < numBlocks; i++) {
                mTachyonFS.cancelBlock(mBlockIds[i]);
            }
            mClosed = true;
        }
    }

    @Override
    public void flush() throws IOException {
        if (mBuffer.position() > 0) {
            appendCurrentBuffer(mBuffer.array(), 0, mBuffer.position());
            mBuffer.clear();
        }
    }

    /**
     * @return the remaining space of the block, in bytes
     */
    @Override
    public long getRemainingSpaceBytes() {
        return mBlockCapacityByte*(numBlocks - Math.max(mCurrentBlockIndex, 0)) - mWrittenBytes;
    }

    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        } else if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) > b.length)
                || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException(String.format("Buffer length (%d), offset(%d), len(%d)",
                    b.length, off, len));
        }

        if (!canWrite()) {
            throw new IOException("Can not write cache.");
        }

        if (mWrittenBytes + len > mBlockCapacityByte*numBlocks) {
            throw new IOException("Out of capacity.");
        }


        try {
            int tLen = len;
            int tOff = off;
            while (tLen > 0) {
                if (mCurrentBlockIndex == -1 || mAvailableBytes == 0) {
                    if (mCurrentBlockIndex != -1) {
                        mClosers[mCurrentBlockIndex].close();
                        System.out.println("Cache Block for blockId " + mBlockIds[mCurrentBlockIndex]);
                        mTachyonFS.cacheBlock(mBlockIds[mCurrentBlockIndex]);
                        System.out.println("Cached Block for blockId " + mBlockIds[mCurrentBlockIndex]);
                        mTachyonFS.getClientMetrics().incBlocksWrittenLocal(1);
                    }
                    mCurrentBlockIndex += 1;
                    mAvailableBytes = mBlockCapacityByte;
                    //mWrittenBytes = 0;
                    mInFileBytes = 0;

                    mClosers[mCurrentBlockIndex] = Closer.create();
                    mLocalFiles[mCurrentBlockIndex] = mClosers[mCurrentBlockIndex].register(new RandomAccessFile(mLocalFilePaths[mCurrentBlockIndex], "rw"));
                    mLocalFileChannels[mCurrentBlockIndex] = mClosers[mCurrentBlockIndex].register(mLocalFiles[mCurrentBlockIndex].getChannel());
                    // change the permission of the temporary file in order that the worker can move it.
                    CommonUtils.changeLocalFileToFullPermission(mLocalFilePaths[mCurrentBlockIndex]);
                    // use the sticky bit, only the client and the worker can write to the block
                    CommonUtils.setLocalFileStickyBit(mLocalFilePaths[mCurrentBlockIndex]);
                    LOG.info(mLocalFilePaths[mCurrentBlockIndex] + " was created! tachyonFile: " + mFile + ", blockIndex: " + mCurrentBlockIndex
                            + ", blockCapacityByte: " + mBlockCapacityByte);
                }
                long currentBlockLeftBytes = mAvailableBytes;
                int curLen = currentBlockLeftBytes >= tLen ? tLen : (int)currentBlockLeftBytes;

                if (mBuffer.position() > 0 && mBuffer.position() + curLen > mBufferBytes) {
                    // Write the non-empty buffer if the new write will overflow it.
                    appendCurrentBuffer(mBuffer.array(), 0, mBuffer.position());
                    mBuffer.clear();
                }

                if (curLen > mBufferBytes / 2) {
                    flush();
                    appendCurrentBuffer(b, tOff, curLen);
                } else if (curLen > 0) {
                    // Write the data to the buffer, and not directly to the mapped file.
                    mBuffer.put(b, tOff, curLen);
                }

                mWrittenBytes += len;

                tLen -= curLen;
                tOff += curLen;
            }
        } catch (IOException e) {
            if (mWriteType.isMustCache()) {
                LOG.error(e.getMessage(), e);
                throw new IOException("Fail to cache: " + mWriteType + ", message: " + e.getMessage(), e);
            } else {
                LOG.warn("Fail to cache for: ", e);
            }
        }
    }

    @Override
    public void write(int b) throws IOException {
        if (!canWrite()) {
            throw new IOException("Can not write cache.");
        }
        if (mWrittenBytes + 1 > mBlockCapacityByte*numBlocks) {
            throw new IOException("Out of capacity.");
        }

        try {
            if (mCurrentBlockIndex == -1 || mAvailableBytes == 0) {
                if (mCurrentBlockIndex != -1) {
                    mClosers[mCurrentBlockIndex].close();
                    LOG.info("Cache Block for blockId " + mBlockIds[mCurrentBlockIndex]);
                    mTachyonFS.cacheBlock(mBlockIds[mCurrentBlockIndex]);
                    LOG.info("Cached Block for blockId " + mBlockIds[mCurrentBlockIndex]);
                    mTachyonFS.getClientMetrics().incBlocksWrittenLocal(1);
                }
                mCurrentBlockIndex += 1;
                mAvailableBytes = mBlockCapacityByte;
                //mWrittenBytes = 0;
                mInFileBytes = 0;

                mClosers[mCurrentBlockIndex] = Closer.create();
                mLocalFiles[mCurrentBlockIndex] = mClosers[mCurrentBlockIndex].register(new RandomAccessFile(mLocalFilePaths[mCurrentBlockIndex], "rw"));
                mLocalFileChannels[mCurrentBlockIndex] = mClosers[mCurrentBlockIndex].register(mLocalFiles[mCurrentBlockIndex].getChannel());
                // change the permission of the temporary file in order that the worker can move it.
                CommonUtils.changeLocalFileToFullPermission(mLocalFilePaths[mCurrentBlockIndex]);
                // use the sticky bit, only the client and the worker can write to the block
                CommonUtils.setLocalFileStickyBit(mLocalFilePaths[mCurrentBlockIndex]);
                LOG.info(mLocalFilePaths[mCurrentBlockIndex] + " was created! tachyonFile: " + mFile + ", blockIndex: " + mCurrentBlockIndex
                        + ", blockCapacityByte: " + mBlockCapacityByte);
            }

            if (mBuffer.position() >= mBufferBytes) {
                appendCurrentBuffer(mBuffer.array(), 0, mBuffer.position());
                mBuffer.clear();
            }

            CommonUtils.putIntByteBuffer(mBuffer, b);
            mWrittenBytes ++;
        } catch (IOException e) {
            if (mWriteType.isMustCache()) {
                LOG.error(e.getMessage(), e);
                throw new IOException("Fail to cache: " + mWriteType + ", message: " + e.getMessage(), e);
            } else {
                LOG.warn("Fail to cache for: ", e);
            }
        }
    }
}
