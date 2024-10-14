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

import org.apache.celeborn.common.protocol.PartitionType;

/** Log of CommitHandler generate new commit epoch. */
public class ShuffleEpochOperationLog implements OperationLog {
  private static final long serialVersionUID = -7294892670742426755L;
  private PartitionType partitionType;
  private long epoch;

  public ShuffleEpochOperationLog(PartitionType partitionType, long epoch) {
    this.partitionType = partitionType;
    this.epoch = epoch;
  }

  public long getEpoch() {
    return epoch;
  }

  public PartitionType getPartitionType() {
    return partitionType;
  }

  @Override
  public Type getType() {
    return Type.SHUFFLE_EPOCH;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("ShuffleEpochOperationLog{");
    sb.append("partitionType=").append(partitionType);
    sb.append(", epoch=").append(epoch);
    sb.append('}');
    return sb.toString();
  }
}
