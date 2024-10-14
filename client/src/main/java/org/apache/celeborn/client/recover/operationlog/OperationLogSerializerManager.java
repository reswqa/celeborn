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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.celeborn.client.recover.SerializationUtil;

/**
 * Util class, used in: serialize OperationLog to DataOutputStream; deserialize OperationLog from
 * DataInputStream.
 */
public class OperationLogSerializerManager {

  public static void serialize(OperationLog operationLog, DataOutputStream stream)
      throws IOException {
    SerializationUtil.serializeObject(stream, operationLog);
    if (operationLog.needFlush()) {
      stream.flush();
    }
  }

  public static <T extends OperationLog> T deserialize(DataInputStream dataInputStream)
      throws IOException {
    try {
      return (T) SerializationUtil.deserializeObject(dataInputStream);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}
