/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.msq.shuffle;

import com.google.common.base.Preconditions;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.ReadableInputStreamFrameChannel;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.msq.indexing.InputChannelFactory;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.storage.StorageConnector;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Provides input channels connected to durable storage.
 */
public class DurableStorageInputChannelFactory implements InputChannelFactory
{
  private final StorageConnector storageConnector;
  private final ExecutorService remoteInputStreamPool;
  private final String controllerTaskId;

  public DurableStorageInputChannelFactory(
      final String controllerTaskId,
      final StorageConnector storageConnector,
      final ExecutorService remoteInputStreamPool
  )
  {
    this.controllerTaskId = Preconditions.checkNotNull(controllerTaskId, "controllerTaskId");
    this.storageConnector = Preconditions.checkNotNull(storageConnector, "storageConnector");
    this.remoteInputStreamPool = Preconditions.checkNotNull(remoteInputStreamPool, "remoteInputStreamPool");
  }

  /**
   * Creates an instance that is the standard production implementation. Closeable items are registered with
   * the provided Closer.
   */
  public static DurableStorageInputChannelFactory createStandardImplementation(
      final String controllerTaskId,
      final StorageConnector storageConnector,
      final Closer closer
  )
  {
    final ExecutorService remoteInputStreamPool =
        Executors.newCachedThreadPool(Execs.makeThreadFactory(controllerTaskId + "-remote-fetcher-%d"));
    closer.register(remoteInputStreamPool::shutdownNow);
    return new DurableStorageInputChannelFactory(controllerTaskId, storageConnector, remoteInputStreamPool);
  }

  @Override
  public ReadableFrameChannel openChannel(StageId stageId, int workerNumber, int partitionNumber) throws IOException
  {

    try {
      final String remotePartitionPath = findSuccessfulPartitionOutput(
          controllerTaskId,
          workerNumber,
          stageId.getStageNumber(),
          partitionNumber
      );
      if (remotePartitionPath == null) {
        throw new ISE(
            "Cannot find a successful write of a worker in the location [%s]",
            DurableStorageOutputChannelFactory.getPartitionOutputsFolderName(
                controllerTaskId,
                workerNumber,
                stageId.getStageNumber(),
                partitionNumber
            )
        );
      }
      RetryUtils.retry(() -> {
        if (!storageConnector.pathExists(remotePartitionPath)) {
          throw new ISE(
              "Could not find remote output of worker task[%d] stage[%d] partition[%d]",
              workerNumber,
              stageId.getStageNumber(),
              partitionNumber
          );
        }
        return Boolean.TRUE;
      }, (throwable) -> true, 10);
      final InputStream inputStream = storageConnector.read(remotePartitionPath);

      return ReadableInputStreamFrameChannel.open(
          inputStream,
          remotePartitionPath,
          remoteInputStreamPool
      );
    }
    catch (Exception e) {
      throw new IOE(
          e,
          "Could not find remote output of worker task[%d] stage[%d] partition[%d]",
          workerNumber,
          stageId.getStageNumber(),
          partitionNumber
      );
    }
  }

  @Nullable
  public String findSuccessfulPartitionOutput(
      final String controllerTaskId,
      final int workerNo,
      final int stageNumber,
      final int partitionNumber
  ) throws IOException
  {
    List<String> fileNames = storageConnector.lsFiles(
        DurableStorageOutputChannelFactory.getPartitionOutputsFolderName(
            controllerTaskId,
            workerNo,
            stageNumber,
            partitionNumber
        )
    );
    Optional<String> maybeFileName = fileNames.stream()
                                              .filter(fileName -> fileName.endsWith(DurableStorageOutputChannelFactory.SUCCESSFUL_FILE_SUFFIX))
                                              .findAny();
    if (!maybeFileName.isPresent()) {
      return null;
    }
    String fileName = maybeFileName.get();
    // Following will remove the trailing marker and get the file with the actual contents
    return fileName.substring(0, fileName.lastIndexOf(DurableStorageOutputChannelFactory.SUCCESSFUL_FILE_SUFFIX));
  }
}
