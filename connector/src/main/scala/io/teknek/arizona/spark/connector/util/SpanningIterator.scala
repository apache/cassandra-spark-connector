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

import scala.collection.mutable.ArrayBuffer

/** An iterator that groups items having the same value of the given function (key).
  * To be included in the same group, items with the same key must be next to each other
  * in the original collection.
  *
  * `SpanningIterator` buffers internally one group at a time and the wrapped iterator
  * is consumed in a lazy way.
  *
  * Example:
  * {{{
  *   val collection = Seq(1 -> "a", 1 -> "b", 1 -> "c", 2 -> "d", 2 -> "e")
  *   val iterator = new SpanningIterator(collection.iterator, (x: (Int, String)) => x._1)
  *   val result = iterator.toSeq  // Seq(1 -> Seq("a", "b", "c"), 2 -> Seq("d", "e"))
  * }}}
  */
class SpanningIterator[K, T](iterator: Iterator[T], f: T => K) extends Iterator[(K, Seq[T])] {

  private[this] val items = new BufferedIterator2(iterator)

  override def hasNext = items.hasNext

  override def next(): (K, Seq[T]) = {
    val key = f(items.head)
    val buffer = new ArrayBuffer[T]
    items.appendWhile(r => f(r) == key, buffer)
    (key, buffer.toSeq)
  }
}
