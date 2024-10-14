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

package org.apache.celeborn.plugin.flink.recover.operationlog;

import static org.apache.celeborn.client.recover.operationlog.OperationLog.Type.SHUFFLE_TASK_INFO;

import org.apache.celeborn.client.recover.operationlog.OperationLog;
import org.apache.celeborn.plugin.flink.ShuffleTaskInfo;

/** Log of {@link ShuffleTaskInfo} */
public class ShuffleTaskInfoOperationLog implements OperationLog {
  private static final long serialVersionUID = 3173444626707691367L;
  private String taskShuffleId;
  private int taskShuffleIdIndex;
  private int nextShuffleIdIndex;
  private int currentPartitionIndex;

  public ShuffleTaskInfoOperationLog(
      String taskShuffleId,
      int taskShuffleIdIndex,
      int nextShuffleIdIndex,
      int currentPartitionIndex) {
    this.taskShuffleId = taskShuffleId;
    this.taskShuffleIdIndex = taskShuffleIdIndex;
    this.nextShuffleIdIndex = nextShuffleIdIndex;
    this.currentPartitionIndex = currentPartitionIndex;
  }

  @Override
  public Type getType() {
    return SHUFFLE_TASK_INFO;
  }

  public String getTaskShuffleId() {
    return taskShuffleId;
  }

  public int getTaskShuffleIdIndex() {
    return taskShuffleIdIndex;
  }

  public int getNextShuffleIdIndex() {
    return nextShuffleIdIndex;
  }

  public int getCurrentPartitionIndex() {
    return currentPartitionIndex;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("ShuffleTaskInfoOperationLog{");
    sb.append("taskShuffleId='").append(taskShuffleId).append('\'');
    sb.append(", taskShuffleIdIndex=").append(taskShuffleIdIndex);
    sb.append(", nextShuffleIdIndex=").append(nextShuffleIdIndex);
    sb.append(", currentPartitionIndex=").append(currentPartitionIndex);
    sb.append('}');
    return sb.toString();
  }
}
