/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.examples;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.Version;
import tachyon.client.OutStream;
import tachyon.client.TachyonByteBuffer;
import tachyon.client.TachyonFile;
import tachyon.client.TachyonFS;
import tachyon.client.WriteType;
import tachyon.conf.TachyonConf;
import tachyon.metrics.MetricsSystem;
import tachyon.thrift.PartitionInfo;
import tachyon.util.CommonUtils;
import tachyon.worker.WorkerSource;
import tachyon.worker.block.BlockDataManager;
import tachyon.worker.block.BlockServiceHandler;
import tachyon.worker.block.BlockWorker;

public class BasicOperations implements Callable<Boolean> {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final TachyonURI mMasterLocation;
  private final TachyonURI mFilePath;
  private final WriteType mWriteType;
  private final int mNumbers = 20;


  public BasicOperations(TachyonURI masterLocation, TachyonURI filePath, WriteType writeType) {
    mMasterLocation = masterLocation;
    mFilePath = filePath;
    mWriteType = writeType;
  }

  @Override
  public Boolean call() throws Exception {
    System.setProperty("tachyon.user.quota.unit.bytes", "536870912");
    TachyonFS tachyonClient = TachyonFS.get(mMasterLocation, new TachyonConf());
    /*
    createFile(tachyonClient);
    writeFile(tachyonClient);
    return readFile(tachyonClient);
    */

    ///*
    TachyonURI path1 = new TachyonURI("/global_spark_tachyon/1/operator_1_0");
    if (!tachyonClient.exist(path1)) {
      createFile(tachyonClient, path1);
    }
    writeFile(tachyonClient, path1, 1, 0, 2.0);

    TachyonURI path2 = new TachyonURI("/global_spark_tachyon/2/operator_2_0");
    if (!tachyonClient.exist(path2)) {
      createFile(tachyonClient, path2);
    }
    writeFile(tachyonClient, path2, 2, 0, 4.0);
    //*/

    TachyonURI path3 = new TachyonURI("/global_spark_tachyon/4/operator_4_0");
    if (!tachyonClient.exist(path3)) {
      createFile(tachyonClient, path3);
    }
    writeFile(tachyonClient, path3, 4, 0, 2.0);
    return true;
  }

  //zengdan
  private void createFile(TachyonFS tachyonClient, TachyonURI path) throws IOException {
    LOG.debug("Creating file...");
    long startTimeMs = CommonUtils.getCurrentMs();
    int fileId = tachyonClient.createFile(path);
    CommonUtils.printTimeTakenMs(startTimeMs, LOG, "createFile with fileId " + fileId);
  }

  private void createFile(TachyonFS tachyonClient) throws IOException {
    LOG.debug("Creating file...");
    long startTimeMs = CommonUtils.getCurrentMs();
    int fileId = tachyonClient.createFile(mFilePath);
    CommonUtils.printTimeTakenMs(startTimeMs, LOG, "createFile with fileId " + fileId);
  }

  private void writeFile(TachyonFS tachyonClient) throws IOException {
    ByteBuffer buf = ByteBuffer.allocate(mNumbers * 4);
    buf.order(ByteOrder.nativeOrder());
    for (int k = 0; k < mNumbers; k ++) {
      buf.putInt(k);
    }

    buf.flip();
    LOG.debug("Writing data...");
    buf.flip();

    long startTimeMs = CommonUtils.getCurrentMs();
    TachyonFile file = tachyonClient.getFile(mFilePath);
    OutStream os = file.getOutStream(mWriteType);
    os.write(buf.array());
    os.close();
    CommonUtils.printTimeTakenMs(startTimeMs, LOG, "writeFile to file " + mFilePath);
  }

  private void writeFile(TachyonFS tachyonClient, TachyonURI path, int id, int index, double benefit) throws IOException {
    ByteBuffer buf = ByteBuffer.allocate(mNumbers * 4);
    buf.order(ByteOrder.nativeOrder());
    for (int k = 0; k < mNumbers; k ++) {
      buf.putInt(k);
    }

    buf.flip();
    LOG.debug("Writing data...");
    buf.flip();

    long startTimeMs = CommonUtils.getCurrentMs();
    TachyonFile file = tachyonClient.getFile(path);
    //OutStream os = file.getOutStream(mWriteType);zengdan
    OutStream os = file.getOutStream(mWriteType, buf.array().length, id, index, benefit);
    os.write(buf.array());
    os.close();

    CommonUtils.printTimeTakenMs(startTimeMs, LOG, "writeFile to file " + path);
  }

  private boolean readFile(TachyonFS tachyonClient) throws IOException {
    boolean pass = true;
    LOG.debug("Reading data...");

    final long startTimeMs = CommonUtils.getCurrentMs();
    TachyonFile file = tachyonClient.getFile(mFilePath);
    TachyonByteBuffer buf = file.readByteBuffer(0);
    if (buf == null) {
      file.recache();
      buf = file.readByteBuffer(0);
    }
    buf.mData.order(ByteOrder.nativeOrder());
    for (int k = 0; k < mNumbers; k ++) {
      pass = pass && (buf.mData.getInt() == k);
    }
    buf.close();

    CommonUtils.printTimeTakenMs(startTimeMs, LOG, "readFile file " + mFilePath);
    return pass;
  }

  public static void main(String[] args) throws IllegalArgumentException{
    /*
    if (args.length != 3) {
      System.out.println("java -cp target/tachyon-" + Version.VERSION
          + "-jar-with-dependencies.jar "
          + "tachyon.examples.BasicOperations <TachyonMasterAddress> <FilePath> <WriteType>");
      System.exit(-1);
    }

    Utils.runExample(new BasicOperations(new TachyonURI(args[0]), new TachyonURI(args[1]),
        WriteType.valueOf(args[2])));
    */

    String[] params = new String[3];
    params[0] = "tachyon://localhost:19998";
    params[1] = "/global_spark_tachyon/1/operator_1_0";
    params[2] = "TRY_CACHE";
    Utils.runExample(new BasicOperations(new TachyonURI(params[0]), new TachyonURI(params[1]),
            WriteType.valueOf(params[2])));

  }
}
