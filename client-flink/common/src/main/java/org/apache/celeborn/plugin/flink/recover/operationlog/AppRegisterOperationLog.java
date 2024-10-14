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

import org.apache.celeborn.client.recover.operationlog.OperationLog;

/** Log of flink register application. */
public class AppRegisterOperationLog implements OperationLog {
  private static final long serialVersionUID = 8523222822394450248L;
  private String celebornAppId;

  public AppRegisterOperationLog(String celebornAppId) {
    this.celebornAppId = celebornAppId;
  }

  @Override
  public Type getType() {
    return Type.APP_REGISTER;
  }

  public String getCelebornAppId() {
    return celebornAppId;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("AppRegisterOperationLog{");
    sb.append("celebornAppId='").append(celebornAppId).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
