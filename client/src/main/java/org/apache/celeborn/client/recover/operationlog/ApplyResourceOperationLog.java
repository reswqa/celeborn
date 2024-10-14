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

package org.apache.celeborn.client.recover.operationlog;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.celeborn.common.meta.ShufflePartitionLocationInfo;
import org.apache.celeborn.common.meta.WorkerInfo;
import org.apache.celeborn.common.protocol.PartitionType;

/** Log of LifecycleManager reserved slots for shuffle. */
public class ApplyResourceOperationLog implements OperationLog {

  private static final long serialVersionUID = 6915840311690244134L;
  private int shuffleId;
  private boolean isSegmentGranularityVisible;
  private PartitionType partitionType;
  private int numMappers;
  private ConcurrentHashMap<WorkerInfo, ShufflePartitionLocationInfo> shufflePartitionLocationInfo;

  public ApplyResourceOperationLog(
      int shuffleId,
      boolean isSegmentGranularityVisible,
      PartitionType partitionType,
      int numMappers,
      ConcurrentHashMap<WorkerInfo, ShufflePartitionLocationInfo> shufflePartitionLocationInfo) {
    this.shuffleId = shuffleId;
    this.isSegmentGranularityVisible = isSegmentGranularityVisible;
    this.partitionType = partitionType;
    this.numMappers = numMappers;
    this.shufflePartitionLocationInfo = shufflePartitionLocationInfo;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public boolean isSegmentGranularityVisible() {
    return isSegmentGranularityVisible;
  }

  public ConcurrentHashMap<WorkerInfo, ShufflePartitionLocationInfo>
      getShufflePartitionLocationInfo() {
    return shufflePartitionLocationInfo;
  }

  public PartitionType getPartitionType() {
    return partitionType;
  }

  public int getNumMappers() {
    return numMappers;
  }

  @Override
  public Type getType() {
    return Type.APPLY_RESOURCE;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("ApplyResourceOperationLog{");
    sb.append("shuffleId=").append(shuffleId);
    sb.append("isSegmentGranularityVisible=").append(isSegmentGranularityVisible);
    sb.append(", partitionType=").append(partitionType);
    sb.append(", numMappers=").append(numMappers);
    sb.append(", shufflePartitionLocationInfo=").append(shufflePartitionLocationInfo);
    sb.append('}');
    return sb.toString();
  }
}
