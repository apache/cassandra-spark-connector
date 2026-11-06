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

package io.teknek.arizona.spark.connector.writer

import java.io.{OutputStream, ObjectOutputStream}
import java.nio.ByteBuffer


import _root_.io.teknek.arizona.spark.connector.util.ByteBufferUtil
import scala.jdk.CollectionConverters._


/** Estimates amount of memory required to serialize Java/Scala objects */
object ObjectSizeEstimator {

  private def makeSerializable(obj: Any): AnyRef = {
    obj match {
      case bb: ByteBuffer => ByteBufferUtil.toArray(bb)
      case list: java.util.List[_] => list.asScala.map(makeSerializable)
      case list: List[_] => list.map(makeSerializable)
      case set: java.util.Set[_] => set.asScala.map(makeSerializable)
      case set: Set[_] => set.map(makeSerializable)
      case map: java.util.Map[_, _] => map.asScala.map { case (k, v) => (makeSerializable(k), makeSerializable(v)) }
      case map: Map[_, _] => map.map { case (k, v) => (makeSerializable(k), makeSerializable(v)) }
      case other => other.asInstanceOf[AnyRef]
    }
  }

  /** Records only how many bytes were written but the actual data is discarded */
  private class CountingOutputStream extends OutputStream {
    private var _length = 0
    override def write(b: Int) = _length += 1
    override def write(b: Array[Byte]) = _length += b.length
    override def write(b: Array[Byte], off: Int, len: Int) = _length += len
    def length = _length
  }

  /** Serializes passed objects and reports their total size */
  def measureSerializedSize(objects: Seq[Any]): Int = {
    val countingStream = new CountingOutputStream
    val objectStream = new ObjectOutputStream(countingStream)
    for (obj <- objects)
      objectStream.writeObject(makeSerializable(obj))
    objectStream.close()
    countingStream.length
  }

}
