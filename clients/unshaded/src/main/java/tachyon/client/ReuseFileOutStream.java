package tachyon.client;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.thrift.BenefitInfo;
import tachyon.underfs.UnderFileSystem;
import tachyon.util.CommonUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zengdan on 15-11-17.
 */
public class ReuseFileOutStream extends OutStream{
    private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

    private final long mBlockCapacityByte;

    //private int mCurrentStreamIndex = -1;
    //private BlockOutStream[] mBlockOutStreams;

    private LocalPartitionOutStream mPartitionOutStream;

    private OutputStream mCheckpointOutputStream = null;
    private String mUnderFsFile = null;

    private boolean mClosed = false;
    private boolean mCancel = false;

    /**
     * @param file the output file
     * @param opType the OutStream's write type
     * @param ufsConf the under file system configuration
     * @param tachyonConf the TachyonConf instance for this file output stream.
     * @throws IOException
     */
    ReuseFileOutStream(TachyonFile file, WriteType opType, long totalSize, int id, int index, BenefitInfo benefit, Object ufsConf, TachyonConf tachyonConf)
            throws IOException {
        super(file, opType, tachyonConf);

        mBlockCapacityByte = file.getBlockSizeByte();
        //mBlockOutStreams = new BlockOutStream[(int)(totalSize/mBlockCapacityByte) + 1];

        if (mWriteType.isCache()) {
            Preconditions.checkState(file.mTachyonFS.hasLocalWorker());
            if (totalSize > 0) {
                mPartitionOutStream = new LocalPartitionOutStream(mFile, mWriteType, totalSize, id, index, benefit, mTachyonConf);
            }
        }

        if (mWriteType.isThrough()) {
            mUnderFsFile = CommonUtils.concatPath(mTachyonFS.createAndGetUserUfsTempFolder(ufsConf),
                    mFile.mFileId);
            UnderFileSystem underfsClient = UnderFileSystem.get(mUnderFsFile, ufsConf, tachyonConf);
            if (mBlockCapacityByte > Integer.MAX_VALUE) {
                throw new IOException("BLOCK_CAPACITY (" + mBlockCapacityByte + ") can not bigger than "
                        + Integer.MAX_VALUE);
            }
            mCheckpointOutputStream = underfsClient.create(mUnderFsFile, (int) mBlockCapacityByte);
        }
    }

    @Override
    public void cancel() throws IOException {
        mCancel = true;
        close();
    }

    @Override
    public void close() throws IOException {
        if (mClosed) {
            return;
        }

        Boolean canComplete = false;
        if (mWriteType.isThrough()) {
            if (mCancel) {
                mCheckpointOutputStream.close();
                UnderFileSystem underFsClient = UnderFileSystem.get(mUnderFsFile, mTachyonConf);
                underFsClient.delete(mUnderFsFile, false);
            } else {
                mCheckpointOutputStream.flush();
                mCheckpointOutputStream.close();
                mTachyonFS.addCheckpoint(mFile.mFileId);
                canComplete = true;
            }
        }

        if (mWriteType.isCache()) {
            try {
                if (mCancel) {
                    //for (BlockOutStream bos : mBlockOutStreams) {
                    //    bos.cancel();
                    //}
                    mPartitionOutStream.cancel();
                } else {
                    //for (BlockOutStream bos : mBlockOutStreams) {
                    //    bos.close();
                    //}
                    mPartitionOutStream.close();
                    canComplete = true;
                }
            } catch (IOException ioe) {
                if (mWriteType.isMustCache()) {
                    LOG.error(ioe.getMessage(), ioe);
                    throw new IOException("Fail to cache: " + mWriteType + ", message: " + ioe.getMessage(),
                            ioe);
                } else {
                    LOG.warn("Fail to cache for: ", ioe);
                }
            }
        }

        if (canComplete) {
            if (mWriteType.isAsync()) {
                mTachyonFS.asyncCheckpoint(mFile.mFileId);
            }
            mTachyonFS.completeFile(mFile.mFileId);
        }
        mClosed = true;
    }

    @Override
    public void flush() throws IOException {
        // TODO We only flush the checkpoint output stream. Flush for RAMFS block streams.
        if (mWriteType.isThrough()) {
            mCheckpointOutputStream.flush();
        }
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
            throw new IndexOutOfBoundsException();
        }

        if (mPartitionOutStream == null) {
            if (off == 0 && len == 0) {
                return;
            } else {
                throw new IOException("Try to write in empty file stream.");
            }
        }

        if (mWriteType.isCache()) {
            mPartitionOutStream.write(b, off, len);
        }

        if (mWriteType.isThrough()) {
            mCheckpointOutputStream.write(b, off, len);
            mTachyonFS.getClientMetrics().incBytesWrittenUfs(len);
        }
    }

    @Override
    public void write(int b) throws IOException {
        if (mWriteType.isCache()) {
            mPartitionOutStream.write(b);
        }

        if (mWriteType.isThrough()) {
            mCheckpointOutputStream.write(b);
            mTachyonFS.getClientMetrics().incBytesWrittenUfs(1);
        }
    }
}
