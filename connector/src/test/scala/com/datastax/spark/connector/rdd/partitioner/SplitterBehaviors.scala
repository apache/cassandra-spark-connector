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

package com.datastax.spark.connector.rdd.partitioner

import org.scalatest.Matchers

import com.datastax.spark.connector.rdd.partitioner.dht.{Token, TokenRange}

private[partitioner] trait SplitterBehaviors[V, T <: Token[V]] {
  this: Matchers =>

  case class SplitResult(splitCount: Int, minSize: BigInt, maxSize: BigInt)

  def splitWholeRingIn(count: Int): Seq[TokenRange[V, T]]

  def range(start: BigInt, end: BigInt): TokenRange[V, T]

  def splittedIn(splitCount: Int): Int = splitCount

  def outputs(splits: Int, withSize: BigInt, sizeTolerance: BigInt = BigInt(0)): SplitResult =
    SplitResult(splits, withSize - sizeTolerance, withSize + sizeTolerance)

  def testSplittingTokens(splitter: => TokenRangeSplitter[V, T]): Unit = {
    val hugeRanges = splitWholeRingIn(10)

    val splitCases = Seq[(TokenRange[V, T], Int, SplitResult)](
      (range(start = 0, end = 1), splittedIn(1), outputs(splits = 1, withSize = 1)),
      (range(start = 0, end = 10), splittedIn(1), outputs(splits = 1, withSize = 10)),
      (range(start = 0, end = 1), splittedIn(10), outputs(splits = 1, withSize = 1)),

      (range(start = 0, end = 9), splittedIn(10), outputs(splits = 9, withSize = 1)),
      (range(start = 0, end = 10), splittedIn(10), outputs(splits = 10, withSize = 1)),
      (range(start = 0, end = 11), splittedIn(10), outputs(splits = 10, withSize = 1, sizeTolerance = 1)),

      (range(start = 10, end = 50), splittedIn(10), outputs(splits = 10, withSize = 4)),
      (range(start = 0, end = 1000), splittedIn(3), outputs(splits = 3, withSize = 333, sizeTolerance = 1)),

      (hugeRanges.head, splittedIn(100), outputs(splits = 100, withSize = hugeRanges.head.rangeSize / 100, sizeTolerance = 4)),
      (hugeRanges.last, splittedIn(100), outputs(splits = 100, withSize = hugeRanges.last.rangeSize / 100, sizeTolerance = 4))
    )

    for ((range, splittedIn, expected) <- splitCases) {

      val splits = splitter.split(range, splittedIn)

      withClue(s"Splitting range (${range.start}, ${range.end}) in $splittedIn splits failed.") {
        splits.size should be(expected.splitCount)
        splits.head.start should be(range.start)
        splits.last.end should be(range.end)
        splits.foreach(_.replicas should be(range.replicas))
        splits.foreach(s => s.rangeSize should (be >= expected.minSize and be <= expected.maxSize))
        splits.map(_.rangeSize).sum should be(range.rangeSize)
        splits.map(_.ringFraction).sum should be(range.ringFraction +- .000000001)
        for (Seq(range1, range2) <- splits.sliding(2)) range1.end should be(range2.start)
      }
    }
  }

  def testSplittingTokenSequences(splitter: TokenRangeSplitter[V, T]): Unit = {
    val mediumRanges = splitWholeRingIn(100)
    val wholeRingSize = mediumRanges.map(_.rangeSize).sum

    val splitCases = Seq[(Seq[TokenRange[V, T]], Int, SplitResult)](
      // we have 100 ranges, so 100 splits is minimum
      (mediumRanges, splittedIn(3), outputs(splits = 100, withSize = wholeRingSize / 100)),

      (mediumRanges, splittedIn(100), outputs(splits = 100, withSize = wholeRingSize / 100)),

      (mediumRanges, splittedIn(101), outputs(splits = 100, withSize = wholeRingSize / 100)),

      (mediumRanges, splittedIn(149), outputs(splits = 100, withSize = wholeRingSize / 100)),

      (mediumRanges, splittedIn(150), outputs(splits = 200, withSize = wholeRingSize / 200, sizeTolerance = 1)),

      (mediumRanges, splittedIn(151), outputs(splits = 200, withSize = wholeRingSize / 200, sizeTolerance = 1)),

      (mediumRanges, splittedIn(199), outputs(splits = 200, withSize = wholeRingSize / 200, sizeTolerance = 1)),

      (mediumRanges, splittedIn(200), outputs(splits = 200, withSize = wholeRingSize / 200, sizeTolerance = 1)),

      (mediumRanges, splittedIn(201), outputs(splits = 200, withSize = wholeRingSize / 200, sizeTolerance = 1))
    )

    for ((ranges, splittedIn, expected) <- splitCases) {

      val splits = splitter.split(ranges, splittedIn)

      withClue(s"Splitting ${ranges.size} ranges in $splittedIn splits failed.") {
        splits.size should be(expected.splitCount)
        splits.head.start should be(ranges.head.start)
        splits.last.end should be(ranges.last.end)
        splits.foreach(s => s.rangeSize should (be >= expected.minSize and be <= expected.maxSize))
        splits.map(_.rangeSize).sum should be(ranges.map(_.rangeSize).sum)
        splits.map(_.ringFraction).sum should be(1.0 +- .000000001)
        for (Seq(range1, range2) <- splits.sliding(2)) range1.end should be(range2.start)
      }
    }
  }
}
