/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.teknek.arizona.spark.connector.util

import java.nio.ByteBuffer

object ByteBufferUtil {

  /** Copies the remaining bytes of the buffer into the given array, starting from offset zero.
    * The array must have capacity to store all of the remaining bytes of the buffer.
    * The buffer's position remains untouched. */
  def copyBuffer(src: ByteBuffer, dest: Array[Byte]): Array[Byte] = {
    if (src.hasArray) {
      val length: Int = src.remaining
      val offset: Int =  src.arrayOffset + src.position()
      System.arraycopy(src.array, offset, dest, 0, length)
    } else {
      src.duplicate.get(dest)
    }
    dest
  }

  /** Converts a byte buffer into an array.
    * The buffer's position remains untouched. */
  def toArray(buffer: ByteBuffer): Array[Byte] = {
    if (buffer.hasArray && 
          buffer.arrayOffset + buffer.position() == 0 &&
          buffer.remaining == buffer.array.length) {
      buffer.array
    } else {
      val dest = new Array[Byte](buffer.remaining)
      copyBuffer(buffer, dest)
    }
  }
}
