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

/** Serves the same purpose as `BufferedIterator` in Scala, but its `takeWhile` method 
  * properly doesn't consume the next element. */
class BufferedIterator2[T](iterator: Iterator[T]) extends Iterator[T] {

  // Instead of a pair of T and Boolean we could use Option, but
  // this would allocate a new Option object per each item, which might be
  // too big overhead in tight loops using this iterator.
  private[this] var headDefined: Boolean = false
  private[this] var headElement: T = advance()

  def head =
    if (headDefined) headElement
    else throw new NoSuchElementException("Head of empty iterator")

  def headOption = headElement

  private def advance(): T = {
    if (iterator.hasNext) {
      headDefined = true
      iterator.next()
    }
    else {
      headDefined = false
      null.asInstanceOf[T]
    }
  }

  override def hasNext = headDefined

  override def next() = {
    val result = head
    headElement = advance()
    result
  }


  override def takeWhile(p: T => Boolean): Iterator[T] = {
    new Iterator[T]() {
      override def hasNext = headDefined && p(headElement)
      override def next() =
        if (hasNext) BufferedIterator2.this.next()
        else throw new NoSuchElementException
    }
  }

  def appendWhile(p: (T) => Boolean, target: ArrayBuffer[T]): Unit = {
    while (headDefined && p(headElement)) {
      target += headElement
      headElement = advance()
    }
  }
}
