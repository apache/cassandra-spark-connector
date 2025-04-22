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

/** An iterator that preforms a mergeJoin between two ordered iterators joining on a given key.
  * The input iterators are assumed to be ordered so we can do a greedy merge join.
  * Since our iterators are lazy we cannot check that they are ordered before starting.
  *
  * Example:
  * {{{
  *   val list1 = Seq( (1, "a"), (2, "a") , (3, "a") )
  *   val list2 = Seq( (1, "b"), (2, "b") , (3, "b") )
  *   val iterator = new MergeJoinIterator(
  *     list1.iterator,
  *     list2.iterator,
  *     (x: (Int, String)) => x._1,
  *     (y: (Int, String)) => y._1
  *   )
  *   val result = iterator.toSeq
  *   // (1, Seq((1, "a")), Seq((1, "b"))),
  *   // (2, Seq((2, "a")), Seq((2, "b"))),
  *   // (3, Seq((3, "a")), Seq((3, "b")))
  * }}}
  */
class MergeJoinIterator[L, R, K](
  iteratorLeft: Iterator[L],
  iteratorRight: Iterator[R],
  keyExtractLeft: L => K,
  keyExtractRight: R => K )(
implicit
  order : Ordering[K])
extends Iterator[(K, Seq[L], Seq[R])] {

  private[this] val itemsLeft = new BufferedIterator2(iteratorLeft)
  private[this] val itemsRight = new BufferedIterator2(iteratorRight)

  override def hasNext = itemsLeft.hasNext || itemsRight.hasNext

  /**
    * We need to determine which iterator is behind since we are assuming
    * sorted order. We then pull all the elements from that iterator as long
    * as the key matches, then pull all the elements from the right iterator.
    */
  override def next(): (K, Seq[L], Seq[R]) = {

    def nextValidKey: K = {
      if (itemsLeft.hasNext && itemsRight.hasNext) {
        Ordering[K].min(keyExtractLeft(itemsLeft.head), keyExtractRight(itemsRight.head))
      } else if (itemsLeft.hasNext) {
        keyExtractLeft(itemsLeft.head)
      } else {
        keyExtractRight(itemsRight.head)
      }
    }

    val key =  nextValidKey
    val bufferLeft = new ArrayBuffer[L]
    val bufferRight = new ArrayBuffer[R]
    itemsLeft.appendWhile(l => keyExtractLeft(l) == key, bufferLeft)
    itemsRight.appendWhile(r => keyExtractRight(r) == key, bufferRight)
    (key, bufferLeft.toSeq, bufferRight.toSeq)
  }
}
