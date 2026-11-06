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

package io.teknek.arizona.spark.connector.cql

import scala.collection.concurrent.TrieMap
import scala.annotation.tailrec

/** Atomically counts references to objects of any type */
class RefCountMap[T] {

  private val refCounts = new TrieMap[T, Int]

  /** Returns current reference count for the given key.
    * This value may be constantly changing, so do not use it for synchronization purposes. */
  final def get(key: T): Int =
    refCounts.getOrElse(key, 0)

  /** Atomically increases reference count only if the reference counter is already greater than 0.
    * @return true if reference counter was greater than zero and has been increased */
  @tailrec
  final def acquireIfNonZero(key: T): Int = {
    refCounts.get(key) match {
      case Some(count) if count > 0 =>
        if (refCounts.replace(key, count, count + 1))
          count + 1
        else
          acquireIfNonZero(key)
      case _ =>
        0
    }
  }

  /** Atomically increases reference count by one.
    * @return reference count after increase */
  @tailrec
  final def acquire(key: T): Int = {
    refCounts.get(key) match {
      case Some(count) =>
        if (refCounts.replace(key, count, count + 1))
          count + 1
        else
          acquire(key)
      case None =>
        if (refCounts.putIfAbsent(key, 1).isEmpty)
          1
        else
          acquire(key)
    }
  }

  /** Atomically decreases reference count by `n`.
    * @return reference count after decrease
    * @throws IllegalStateException if the reference count before decrease is less than `n` */
  @tailrec
  final def release(key: T, n: Int = 1): Int = {
    refCounts.get(key) match {
      case Some(count) if count > n =>
        if (refCounts.replace(key, count, count - n))
          count - n
        else
          release(key, n)
      case Some(count) if count == n =>
        if (refCounts.remove(key, n))
          0
        else
          release(key, n)
      case _ =>
        throw new IllegalStateException("Release without acquire for key: " + key)
    }
  }

  /** Resets state of all counters to 0 */
  def clear(): Unit = refCounts.clear()

}
