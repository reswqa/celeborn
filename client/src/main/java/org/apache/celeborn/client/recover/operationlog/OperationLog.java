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

import java.io.Serializable;

/** A class that represents an action that happens during the job execution. */
public interface OperationLog extends Serializable {

  enum Type {
    APP_REGISTER,
    APPLY_RESOURCE,
    SHUFFLE_TASK_INFO,
    COMMIT_RESOURCE,
    RELEASE_RESOURCE,
    ADD_PARTITION,
    REMOVE_PARTITION,
    SHUFFLE_EPOCH,
    UNREGISTER_JOB
  }

  Type getType();

  default boolean needFlush() {
    return true;
  }
}
