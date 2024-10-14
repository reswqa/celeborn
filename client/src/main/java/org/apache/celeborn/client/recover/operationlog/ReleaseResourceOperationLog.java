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

/** Log of LifecycleManager release ResultPartition. */
public class ReleaseResourceOperationLog implements OperationLog {
  private static final long serialVersionUID = 1019092740813732143L;
  private int shuffleId;
  private int partitionId;

  public ReleaseResourceOperationLog(int shuffleId, int partitionId) {
    this.shuffleId = shuffleId;
    this.partitionId = partitionId;
  }

  @Override
  public Type getType() {
    return Type.RELEASE_RESOURCE;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("ReleaseResourceOperationLog{");
    sb.append("shuffleId=").append(shuffleId);
    sb.append(", partitionId=").append(partitionId);
    sb.append('}');
    return sb.toString();
  }
}
