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

import static org.apache.celeborn.client.recover.RecoverableStore.ID_PERSISTENT_STEP;
import static org.apache.celeborn.client.recover.operationlog.OperationLog.Type.SHUFFLE_TASK_INFO;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.flink.annotation.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.recover.DummyRecoverableStore;
import org.apache.celeborn.client.recover.RecoverableStore;
import org.apache.celeborn.client.recover.Restoreable;
import org.apache.celeborn.client.recover.operationlog.OperationLog;
import org.apache.celeborn.common.util.JavaUtils;
import org.apache.celeborn.plugin.flink.recover.operationlog.ShuffleTaskInfoOperationLog;

public class ShuffleTaskInfo implements Restoreable {
  private static final Logger LOG = LoggerFactory.getLogger(ShuffleTaskInfo.class);

  private int currentShuffleIndex = 0;
  // map attemptId index
  private ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, AtomicInteger>>
      shuffleIdMapAttemptIdIndex = JavaUtils.newConcurrentHashMap();
  // task shuffle id -> celeborn shuffle id
  private final ConcurrentHashMap<String, Integer> taskShuffleIdToShuffleId =
      JavaUtils.newConcurrentHashMap();
  // celeborn shuffle id -> task shuffle id
  private ConcurrentHashMap<Integer, String> shuffleIdToTaskShuffleId =
      JavaUtils.newConcurrentHashMap();

  private ConcurrentHashMap<Integer, AtomicInteger> shuffleIdPartitionIdIndex =
      JavaUtils.newConcurrentHashMap();

  private RecoverableStore recoverableStore;

  public ShuffleTaskInfo(RecoverableStore recoverableStore) {
    this.recoverableStore = recoverableStore;
  }

  @VisibleForTesting
  public ShuffleTaskInfo() {
    this.recoverableStore = new DummyRecoverableStore();
  }

  public int getShuffleId(String taskShuffleId) {
    synchronized (taskShuffleIdToShuffleId) {
      if (taskShuffleIdToShuffleId.containsKey(taskShuffleId)) {
        return taskShuffleIdToShuffleId.get(taskShuffleId);
      } else {
        taskShuffleIdToShuffleId.put(taskShuffleId, currentShuffleIndex);
        shuffleIdToTaskShuffleId.put(currentShuffleIndex, taskShuffleId);
        shuffleIdMapAttemptIdIndex.put(currentShuffleIndex, JavaUtils.newConcurrentHashMap());
        shuffleIdPartitionIdIndex.put(currentShuffleIndex, new AtomicInteger(0));
        int tempShuffleIndex = currentShuffleIndex;
        currentShuffleIndex = currentShuffleIndex + 1;
        recoverableStore.writeOperation(
            new ShuffleTaskInfoOperationLog(
                taskShuffleId, tempShuffleIndex, currentShuffleIndex, ID_PERSISTENT_STEP));
        return tempShuffleIndex;
      }
    }
  }

  public int genAttemptId(int shuffleId, int mapId) {
    AtomicInteger currentAttemptIndex =
        shuffleIdMapAttemptIdIndex
            .get(shuffleId)
            .computeIfAbsent(mapId, (id) -> new AtomicInteger(0));
    return currentAttemptIndex.getAndIncrement();
  }

  public int genPartitionId(int shuffleId) {
    if (!recoverableStore.supportRecoverable()) {
      return shuffleIdPartitionIdIndex.get(shuffleId).getAndIncrement();
    } else {
      synchronized (shuffleIdPartitionIdIndex.get(shuffleId)) {
        int currentPartitionId = shuffleIdPartitionIdIndex.get(shuffleId).getAndIncrement();
        if (currentPartitionId > 0 && currentPartitionId % ID_PERSISTENT_STEP == 0) {
          int nextIndex = (int) Math.ceil((currentPartitionId + 1.0) / ID_PERSISTENT_STEP);
          ShuffleTaskInfoOperationLog operationLog =
              new ShuffleTaskInfoOperationLog(
                  shuffleIdToTaskShuffleId.get(shuffleId),
                  shuffleId,
                  currentShuffleIndex,
                  nextIndex * ID_PERSISTENT_STEP);
          recoverableStore.writeOperation(operationLog);
          LOG.info("GenPartitionId, write operation log {} to recoverable store.", operationLog);
        }

        return currentPartitionId;
      }
    }
  }

  public void removeExpiredShuffle(int shuffleId) {
    if (shuffleIdToTaskShuffleId.containsKey(shuffleId)) {
      shuffleIdPartitionIdIndex.remove(shuffleId);
      shuffleIdMapAttemptIdIndex.remove(shuffleId);
      String taskShuffleId = shuffleIdToTaskShuffleId.remove(shuffleId);
      taskShuffleIdToShuffleId.remove(taskShuffleId);
    }
  }

  public ShuffleResourceDescriptor genShuffleResourceDescriptor(
      String taskShuffleId, int mapId, String taskAttemptId) {
    int shuffleId = this.getShuffleId(taskShuffleId);
    int attemptId = this.genAttemptId(shuffleId, mapId);
    int partitionId = this.genPartitionId(shuffleId);
    LOG.info(
        "Assign for ({}, {}, {}) resource ({}, {}, {}, {})",
        taskShuffleId,
        mapId,
        taskAttemptId,
        shuffleId,
        mapId,
        attemptId,
        partitionId);
    return new ShuffleResourceDescriptor(shuffleId, mapId, attemptId, partitionId);
  }

  @Override
  public void replay(OperationLog operationLog) {
    if (operationLog.getType() == SHUFFLE_TASK_INFO) {
      ShuffleTaskInfoOperationLog shuffleTaskInfoOperationLog =
          (ShuffleTaskInfoOperationLog) operationLog;
      taskShuffleIdToShuffleId.put(
          shuffleTaskInfoOperationLog.getTaskShuffleId(),
          shuffleTaskInfoOperationLog.getTaskShuffleIdIndex());
      shuffleIdToTaskShuffleId.put(
          shuffleTaskInfoOperationLog.getTaskShuffleIdIndex(),
          shuffleTaskInfoOperationLog.getTaskShuffleId());
      shuffleIdMapAttemptIdIndex.put(
          shuffleTaskInfoOperationLog.getTaskShuffleIdIndex(), JavaUtils.newConcurrentHashMap());
      shuffleIdPartitionIdIndex.put(
          shuffleTaskInfoOperationLog.getTaskShuffleIdIndex(),
          new AtomicInteger(shuffleTaskInfoOperationLog.getCurrentPartitionIndex()));
      currentShuffleIndex =
          Math.max(shuffleTaskInfoOperationLog.getNextShuffleIdIndex(), currentShuffleIndex);
    }
  }
}
