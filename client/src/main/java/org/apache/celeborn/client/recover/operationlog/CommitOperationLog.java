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

import java.util.Set;

import org.apache.celeborn.common.protocol.PartitionLocation;

/** Log of CommitHandler commit files. */
public class CommitOperationLog implements OperationLog {
  private static final long serialVersionUID = -12321153364862850L;
  private int shuffleId;
  private int partitionId;
  private Set<PartitionLocation> partitionLocations;

  public CommitOperationLog(
      int shuffleId, int partitionId, Set<PartitionLocation> partitionLocations) {
    this.shuffleId = shuffleId;
    this.partitionId = partitionId;
    this.partitionLocations = partitionLocations;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public Set<PartitionLocation> getPartitionLocations() {
    return partitionLocations;
  }

  @Override
  public Type getType() {
    return Type.COMMIT_RESOURCE;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("CommitOperationLog{");
    sb.append("shuffleId=").append(shuffleId);
    sb.append(", partitionId=").append(partitionId);
    sb.append(", partitionLocations=").append(partitionLocations);
    sb.append('}');
    return sb.toString();
  }
}
