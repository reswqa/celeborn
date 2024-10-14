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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

/** Referring to org.apache.flink.util.InstantiationUtil. */
public class SerializationUtil {

  public static byte[] serializeObject(Object o) throws IOException {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(o);
      oos.flush();
      return baos.toByteArray();
    }
  }

  public static void serializeObject(OutputStream out, Object o) throws IOException {
    ObjectOutputStream oos =
        out instanceof ObjectOutputStream ? (ObjectOutputStream) out : new ObjectOutputStream(out);
    oos.writeObject(o);
  }

  public static <T> T deserializeObject(byte[] bytes) throws IOException, ClassNotFoundException {
    return deserializeObject(new ByteArrayInputStream(bytes));
  }

  @SuppressWarnings("unchecked")
  public static <T> T deserializeObject(InputStream in) throws IOException, ClassNotFoundException {

    final ClassLoader old = Thread.currentThread().getContextClassLoader();
    // not using resource try to avoid AutoClosable's close() on the given stream
    try {
      ObjectInputStream oois = new ObjectInputStream(in);
      return (T) oois.readObject();
    } finally {
      Thread.currentThread().setContextClassLoader(old);
    }
  }
}
