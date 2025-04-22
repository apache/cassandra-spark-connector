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

package com.datastax.spark.connector.util

import scala.collection.mutable.ArrayBuffer

/** An iterator that preforms a mergeJoin among ordered iterators joining on a given key.
  * The input iterators are assumed to be ordered so we can do a greedy merge join.
  * Since our iterators are lazy we cannot check that they are ordered before starting.
  *
  * Example:
  * {{{
  *   val list1 = Seq( (1, "a"), (2, "a") , (3, "a") )
  *   val list2 = Seq( (1, "b"), (2, "b") , (3, "b") )
  *   val iterator = new MultiMergeJoinIterator(
  *     Seq(list1.iterator,list2.iterator),
  *     (x: (Int, String)) => x._1,
  *   )
  *   val result = iterator.toSeq
  *   // (Seq((1, "a")), Seq((1, "b"))),
  *   // (Seq((2, "a")), Seq((2, "b"))),
  *   // (Seq((3, "a")), Seq((3, "b")))
  * }}}
  */
class MultiMergeJoinIterator[T, K](
  iterators: Seq[Iterator[T]],
  keyExtract: T => K )(
implicit
  order : Ordering[K])
extends Iterator[Seq[Seq[T]]] {

  private[this] val items = iterators.map(i => new BufferedIterator2(i))

  override def hasNext = items.exists(_.hasNext)

 /**
    * We need to determine which iterator is behind since we are assuming
    * sorted order. We got smallest key from it and then pull all elements with that smallest key from all other iterators.
    */
  override def next(): Seq[Seq[T]] = {

    def nextValidKey: K = items.map(_.headOption).filter(_ != null).map(keyExtract).min(Ordering[K])

    val key =  nextValidKey
    items.map (i => {
      var buffer = new ArrayBuffer[T]
      i.appendWhile(l =>  keyExtract(l) == key, buffer)
      buffer.toSeq
    })
  }
}
