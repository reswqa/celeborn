/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.celeborn.plugin.flink;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.shuffle.EmptyShuffleMasterSnapshot;
import org.apache.flink.runtime.shuffle.JobShuffleContext;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.runtime.shuffle.PartitionWithMetrics;
import org.apache.flink.runtime.shuffle.ProducerDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.shuffle.ShuffleMasterContext;
import org.apache.flink.runtime.shuffle.ShuffleMasterSnapshot;
import org.apache.flink.runtime.shuffle.ShuffleMasterSnapshotContext;
import org.apache.flink.runtime.shuffle.TaskInputsOutputsDescriptor;

public class RemoteShuffleMaster implements ShuffleMaster<RemoteShuffleDescriptor> {

  private final RemoteShuffleMasterDelegation delegation;

  public RemoteShuffleMaster(
      ShuffleMasterContext shuffleMasterContext,
      SimpleResultPartitionAdapter simpleResultPartitionAdapter) {
    this.delegation =
        new RemoteShuffleMasterDelegation(shuffleMasterContext, simpleResultPartitionAdapter);
  }

  @Override
  public void close() throws Exception {
    delegation.close();
  }

  @Override
  public void registerJob(JobShuffleContext context) {
    delegation.registerJob(context);
  }

  @Override
  public void unregisterJob(JobID jobID) {
    delegation.unregisterJob(jobID);
  }

  @Override
  public CompletableFuture<RemoteShuffleDescriptor> registerPartitionWithProducer(
      JobID jobID, PartitionDescriptor partitionDescriptor, ProducerDescriptor producerDescriptor) {
    return delegation.registerPartitionWithProducer(jobID, partitionDescriptor, producerDescriptor);
  }

  @Override
  public void releasePartitionExternally(ShuffleDescriptor shuffleDescriptor) {
    delegation.releasePartitionExternally(shuffleDescriptor);
  }

  @Override
  public MemorySize computeShuffleMemorySizeForTask(
      TaskInputsOutputsDescriptor taskInputsOutputsDescriptor) {
    return delegation.computeShuffleMemorySizeForTask(taskInputsOutputsDescriptor);
  }

  @Override
  public CompletableFuture<Collection<PartitionWithMetrics>> getPartitionWithMetrics(
      JobID jobId, Duration timeout, Set<ResultPartitionID> expectedPartitions) {
    return CompletableFuture.completedFuture(Collections.emptyList());
  }

  @Override
  public boolean supportsBatchSnapshot() {
    return true;
  }

  @Override
  public void snapshotState(
      CompletableFuture<ShuffleMasterSnapshot> snapshotFuture,
      ShuffleMasterSnapshotContext context) {
    snapshotFuture.complete(EmptyShuffleMasterSnapshot.getInstance());
  }
}
