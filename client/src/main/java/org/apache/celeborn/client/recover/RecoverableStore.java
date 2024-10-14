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

package org.apache.celeborn.client.recover;

import java.io.IOException;

import org.apache.celeborn.client.recover.operationlog.OperationLog;

/**
 * Used to record RemoteShuffleMaster/CelebornTierMasterAgent/LifecycleManager actions into
 * persistent log, and used to recover LifecycleManager upon flink JobManager recovery.
 */
public interface RecoverableStore {

  /**
   * Used to reduce the number persistent logs.
   *
   * <p>For example, CommitHandler#commitEpoch, which represents the commit times of the
   * LifecycleManager, should be unique in a job and does not need to be continuous, even if the job
   * has failed and is being recovered. In this case, we only record the maximum number of
   * CommitHandler#commitEpoch (which may not have been reached), and after job recovery,
   * CommitHandler#commitEpoch will be set to the recorded epoch number. e.g. When epoch = 0, record
   * epoch = 10000 in the persistent log, and after recovery, CommitHandler#commitEpoch will be set
   * to 10000; When epoch = 1,2,3...9999, do not record to persistent log, and after recovery,
   * CommitHandler#commitEpoch will be set to 10000; When epoch = 10000, record epoch = 20000 in the
   * persistent log, and after recovery, CommitHandler#commitEpoch will be set to 20000;
   */
  int ID_PERSISTENT_STEP = 10000;

  /** Record registerJob. */
  void registerAppIdOperation(OperationLog operationLog);

  /** Mark the recover in celeborn has been completed. */
  void setRecoverFinished(boolean recoverFinished);

  /** Write operation log into persistent. */
  void writeOperation(OperationLog operationLog);

  /** Read operationLog from internal storage. */
  OperationLog readOperation() throws IOException;

  void stop();

  /** Whether the job can recover from this RecoverableStore. */
  default boolean supportRecoverable() {
    return true;
  }

  void clear();
}
