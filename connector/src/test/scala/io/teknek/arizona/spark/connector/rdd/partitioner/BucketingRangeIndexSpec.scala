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

import scala.util.Random
import org.scalacheck.Gen
import org.scalatest.{FlatSpec, Matchers}
import _root_.io.teknek.arizona.spark.connector.rdd.partitioner.dht.TokenFactory.{Murmur3TokenFactory, RandomPartitionerTokenFactory}
import _root_.io.teknek.arizona.spark.connector.rdd.partitioner.dht.{BigIntToken, LongToken, TokenRange}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class BucketingRangeIndexSpec extends FlatSpec with ScalaCheckPropertyChecks with Matchers {

  private val clusterSize = 1000 * 16

  private implicit object TupleBounds extends RangeBounds[(Int, Int), Int] {
    override def start(range: (Int, Int)): Int =
      range._1
    override def end(range: (Int, Int)): Int =
      range._2
    override def isFull(range: (Int, Int)): Boolean =
      range._1 == range._2
    override def contains(range: (Int, Int), point: Int): Boolean =
      if (range._2 <= range._1)
        point >= range._1 || point < range._2
      else
        point >= range._1 && point < range._2
  }

  /** Creates non overlapping random rangesContaining */
  private def randomRanges(n: Int): Seq[(Int, Int)] = {
    val randomInts =
      (Int.MinValue +: Iterator.continually(Random.nextInt()).take(n).toSeq :+ Int.MaxValue)
        .distinct
        .sortBy(identity)
    val randomRanges = for (Seq(a, b) <- randomInts.sliding(2).toSeq) yield (a, b)
    randomRanges
  }

  "BucketingRangeIndex" should "find ranges containing given point when ranges do not overlap" in {
    val index = new BucketingRangeIndex(randomRanges(clusterSize))
    for (x <- Iterator.continually(Random.nextInt()).take(100000)) {
      val containingRanges = index.rangesContaining(x)
      assert(containingRanges.size == 1)
      assert(x >= containingRanges.head._1)
      assert(x < containingRanges.head._2)
    }
  }

  it should "find ranges containing given point when ranges do overlap" in {
    val index = new BucketingRangeIndex(
      randomRanges(clusterSize) ++ randomRanges(clusterSize) ++ randomRanges(clusterSize))
    for (x <- Iterator.continually(Random.nextInt()).take(100000)) {
      val containingRanges = index.rangesContaining(x)
      assert(containingRanges.size == 3)
      assert(containingRanges forall (x >= _._1))
      assert(containingRanges forall (x < _._2))
    }
  }

  it should "find proper ranges when they wrap around" in {
    val range1 = (0, 0)                         // full range
    val range2 = (Int.MaxValue, Int.MinValue)   // only MaxValue included
    val range3 = (Int.MaxValue, 0)              // lower half
    val index = new BucketingRangeIndex(Seq(range1, range2, range3))
    assert(index.rangesContaining(0) == Seq(range1))
    assert(index.rangesContaining(Int.MaxValue) == Seq(range1, range2, range3))
    assert(index.rangesContaining(Int.MaxValue / 2) == Seq(range1))
    assert(index.rangesContaining(Int.MinValue) == Seq(range1, range3))
    assert(index.rangesContaining(Int.MinValue / 2) == Seq(range1, range3))
  }

  def randomBigIntToken(): BigIntToken =
    BigIntToken(BigInt(127, Random).abs)

  val bigIntTokens = Gen.const(1).map(_ => randomBigIntToken())
  val longTokens = Gen.choose(Long.MinValue, Long.MaxValue).map(LongToken.apply)
  val positiveIntegers = Gen.choose(1, Int.MaxValue)

  "LongTokenBucketing" should "respect the required bucket range" in {
    val bucketing = implicitly[MonotonicBucketing[LongToken]]
    forAll(longTokens, positiveIntegers) { (t: LongToken, n: Int) =>
      val b = bucketing.bucket(n)(t)
      assert(b >= 0)
      assert(b < n)
    }
  }

  it should "be weakly monotonic" in {
    val bucketing = implicitly[MonotonicBucketing[LongToken]]
    forAll(longTokens, longTokens, positiveIntegers) { (t1: LongToken, t2: LongToken, n: Int) =>
      val b1 = bucketing.bucket(n)(t1)
      val b2 = bucketing.bucket(n)(t2)
      assert(t1 == t2 && b1 == b2 || t1 < t2 && b1 <= b2 || t1 > t2 && b1 >= b2)
    }
  }

  "BigIntTokenBucketing" should "respect the required bucket range" in {
    val bucketing = implicitly[MonotonicBucketing[BigIntToken]]
    forAll(bigIntTokens, positiveIntegers) { (t: BigIntToken, n: Int) =>
      val b = bucketing.bucket(n)(t)
      assert(b >= 0)
      assert(b < n)
    }
  }

  it should "be weakly monotonic" in {
    val bucketing = implicitly[MonotonicBucketing[BigIntToken]]
    forAll(bigIntTokens, bigIntTokens, positiveIntegers) { (t1: BigIntToken, t2: BigIntToken, n: Int) =>
      val b1 = bucketing.bucket(n)(t1)
      val b2 = bucketing.bucket(n)(t2)
      assert(t1 == t2 && b1 == b2 || t1 < t2 && b1 <= b2 || t1 > t2 && b1 >= b2)
    }
  }

  private type LTR = TokenRangeWithPartitionIndex[Long, LongToken]
  private type LT = LongToken
  private type LV = Long

  private val longTokenFactory = Murmur3TokenFactory
  private val LongFullRange = TokenRange[LV, LT](
      longTokenFactory.minToken,
      longTokenFactory.minToken,
      Set.empty,
      Murmur3TokenFactory)

  "Murmur3Bucketing" should "map all tokens to a single wrapping range" in {
    implicit val tokenOrdering: Ordering[LT] = longTokenFactory.tokenOrdering
    implicit val tokenBucketing: MonotonicBucketing[LT] = longTokenFactory.tokenBucketing

    val singleRangeBucketing =
      new BucketingRangeIndex[LTR, LT](Seq(TokenRangeWithPartitionIndex(LongFullRange, 0)))

    for (tokenValue <- Iterator.continually(Random.nextLong()).take(100000)) {
      val token = LongToken(tokenValue)
      singleRangeBucketing.rangesContaining(token).headOption shouldBe defined
      singleRangeBucketing.rangesContaining(token).head.partitionIndex should be(0)
    }
  }

  private type BITR = TokenRangeWithPartitionIndex[BigInt, BigIntToken]
  private type BT = BigIntToken
  private type BV = BigInt

  private val bigTokenFactory = RandomPartitionerTokenFactory
  private val bigFullRange = TokenRange[BV, BT](
      bigTokenFactory.minToken,
      bigTokenFactory.minToken,
      Set.empty,
      RandomPartitionerTokenFactory)


  "RandomBucketing" should " map all tokens to a single wrapping range" in {
    implicit val tokenOrdering: Ordering[BT] = bigTokenFactory.tokenOrdering
    implicit val tokenBucketing: MonotonicBucketing[BT] = bigTokenFactory.tokenBucketing

     val singleRangeBucketing =
      new BucketingRangeIndex[BITR, BT](Seq(TokenRangeWithPartitionIndex(bigFullRange, 0)))

    for (tokenValue <- Iterator.continually(Random.nextLong().abs).take(100000)) {
      val token = BigIntToken(tokenValue)
      singleRangeBucketing.rangesContaining(token).headOption shouldBe defined
      singleRangeBucketing.rangesContaining(token).head.partitionIndex should be(0)
    }


  }
}
