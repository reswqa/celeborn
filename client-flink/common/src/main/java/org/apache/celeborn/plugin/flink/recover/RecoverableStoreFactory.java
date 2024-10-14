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

package org.apache.celeborn.plugin.flink.recover;

import java.io.IOException;

import org.apache.flink.configuration.Configuration;

import org.apache.celeborn.client.recover.DummyRecoverableStore;
import org.apache.celeborn.client.recover.RecoverableStore;

public class RecoverableStoreFactory {

  // Flink job recovery enabled configuration, introduced in flink 1.20
  public static final String FLINK_JOB_RECOVERY_ENABLED = "execution.batch.job-recovery.enabled";

  public static RecoverableStore createOperationLogStore(
      String jobId, Configuration config, RecoverableStoreShuffleContext storeShuffleContext)
      throws IOException {
    if (config.getBoolean(FLINK_JOB_RECOVERY_ENABLED, false)) {
      return new FileSystemRecoverableStore(jobId, config, storeShuffleContext);
    } else {
      return new DummyRecoverableStore();
    }
  }
}
