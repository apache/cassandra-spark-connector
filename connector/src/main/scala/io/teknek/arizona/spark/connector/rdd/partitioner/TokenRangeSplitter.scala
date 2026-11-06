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

package io.teknek.arizona.spark.connector.rdd.partitioner

import scala.collection.parallel.{ForkJoinTaskSupport, ParIterable}
import java.util.concurrent.ForkJoinPool
import _root_.io.teknek.arizona.spark.connector.rdd.partitioner.TokenRangeSplitter.WholeRing
import _root_.io.teknek.arizona.spark.connector.rdd.partitioner.dht.{Token, TokenRange}
import _root_.io.teknek.arizona.spark.connector.util.RuntimeUtil


/** Splits a token ranges into smaller sub-ranges,
  * each with the desired approximate number of rows. */
private[partitioner] trait TokenRangeSplitter[V, T <: Token[V]] {

  def split(tokenRanges: Iterable[TokenRange[V, T]], splitCount: Int): Iterable[TokenRange[V, T]] = {

    val ringFractionPerSplit = WholeRing / splitCount.toDouble
    val parTokenRanges: ParIterable[TokenRange[V, T]] = RuntimeUtil.toParallelIterable(tokenRanges)

    parTokenRanges.tasksupport = new ForkJoinTaskSupport(TokenRangeSplitter.pool)
    parTokenRanges.flatMap(tokenRange => {
      val splitCount = Math.rint(tokenRange.ringFraction / ringFractionPerSplit).toInt
      split(tokenRange, math.max(1, splitCount))
    }).toList
  }

  /** Splits the token range uniformly into splitCount sub-ranges. */
   def split(tokenRange: TokenRange[V, T], splitCount: Int): Seq[TokenRange[V, T]]
}

object TokenRangeSplitter {
  private val MaxParallelism = 16

  private val WholeRing = 1.0

  private val pool = new ForkJoinPool(MaxParallelism)
}
