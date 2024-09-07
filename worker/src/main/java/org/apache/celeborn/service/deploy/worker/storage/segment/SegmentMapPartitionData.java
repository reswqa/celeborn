/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.celeborn.service.deploy.worker.storage.segment;

import static org.apache.commons.crypto.utils.Utils.checkState;

import java.io.IOException;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.meta.DiskFileInfo;
import org.apache.celeborn.service.deploy.worker.memory.BufferRecycler;
import org.apache.celeborn.service.deploy.worker.storage.MapPartitionData;
import org.apache.celeborn.service.deploy.worker.storage.MapPartitionDataReader;

public class SegmentMapPartitionData extends MapPartitionData {

  public static final Logger logger = LoggerFactory.getLogger(SegmentMapPartitionData.class);

  private final boolean requireSubpartitionId;

  public SegmentMapPartitionData(
      int minReadBuffers,
      int maxReadBuffers,
      HashMap<String, ExecutorService> storageFetcherPool,
      int threadsPerMountPoint,
      DiskFileInfo fileInfo,
      Consumer<Long> recycleStream,
      int minBuffersToTriggerRead,
      boolean requireSubpartitionId)
      throws IOException {
    super(
        minReadBuffers,
        maxReadBuffers,
        storageFetcherPool,
        threadsPerMountPoint,
        fileInfo,
        recycleStream,
        minBuffersToTriggerRead);
    this.requireSubpartitionId = requireSubpartitionId;
  }

  @Override
  public void setupDataPartitionReader(
      int startSubIndex, int endSubIndex, long streamId, Channel channel) {
    SegmentMapPartitionDataReader mapDataPartitionReader =
        new SegmentMapPartitionDataReader(
            startSubIndex,
            endSubIndex,
            getDiskFileInfo(),
            streamId,
            requireSubpartitionId,
            channel,
            readExecutor,
            () -> recycleStream.accept(streamId));
    logger.debug("[{}] add reader, streamId: {}", this, streamId);
    readers.put(streamId, mapDataPartitionReader);
  }

  @Override
  public synchronized void readBuffers() {
    hasReadingTask.set(false);

    if (isReleased) {
      // some read executor task may already be submitted to the thread pool
      return;
    }

    try {
      PriorityQueue<MapPartitionDataReader> sortedReaders =
          new PriorityQueue<>(
              readers.values().stream()
                  .filter(MapPartitionDataReader::shouldReadData)
                  .collect(Collectors.toList()));
      for (MapPartitionDataReader reader : sortedReaders) {
        reader.open(dataFileChanel, indexChannel, indexSize);
        checkState(reader instanceof SegmentMapPartitionDataReader);
        ((SegmentMapPartitionDataReader) reader).updateSegmentId();
      }
      while (bufferQueue.bufferAvailable() && !sortedReaders.isEmpty()) {
        BufferRecycler bufferRecycler = new BufferRecycler(this::recycle);
        MapPartitionDataReader reader = sortedReaders.poll();
        try {
          reader.readData(bufferQueue, bufferRecycler);
        } catch (Throwable e) {
          logger.error("reader exception, reader: {}, message: {}", reader, e.getMessage(), e);
          reader.recycleOnError(e);
        }
      }
    } catch (Throwable e) {
      logger.error("Fatal: failed to read partition data. {}", e.getMessage(), e);
      for (MapPartitionDataReader reader : readers.values()) {
        reader.recycleOnError(e);
      }
    }
  }

  @Override
  public String toString() {
    return String.format("SegmentMapDataPartition{filePath=%s}", diskFileInfo.getFilePath());
  }

  public void notifyRequiredSegmentId(int segmentId, long streamId, int subPartitionId) {
    MapPartitionDataReader streamReader = getStreamReader(streamId);
    if (streamReader == null) {
      return;
    }
    checkState(streamReader instanceof SegmentMapPartitionDataReader);
    ((SegmentMapPartitionDataReader) streamReader)
        .notifyRequiredSegmentId(segmentId, subPartitionId);
    // After notifying the required segment id, we need to try to send data again.
    readExecutor.submit(
        () -> {
          try {
            streamReader.sendData();
          } catch (Throwable throwable) {
            logger.error("Failed to read data.", throwable);
          }
        });
  }
}
