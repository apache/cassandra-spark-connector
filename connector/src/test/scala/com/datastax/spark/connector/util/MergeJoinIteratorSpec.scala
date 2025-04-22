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

import org.scalatest.{FlatSpec, Matchers}

class MergeJoinIteratorSpec  extends FlatSpec with Matchers {

  "MergeJoinIterator" should "group an empty collection" in {
    new MergeJoinIterator[Int, Int, Int](Iterator.empty, Iterator.empty, identity, identity)
      .isEmpty shouldBe true
  }

  it should "group two sequences with the same key into a single item and preserve order" in {
    val collectionL = Seq(1, 2, 3, 4, 5)
    val collectionR = Seq(1, 2, 3, 4, 5)
    val grouped = new MergeJoinIterator(
      collectionL.iterator,
      collectionR.iterator,
      (_: Int) => 0,
      (_: Int) => 0).toSeq
    grouped should have length 1
    grouped.head._2 should contain inOrder(1, 2, 3, 4, 5)
    grouped.head._3 should contain inOrder(1, 2, 3, 4, 5)
  }

  it should "group a sequence of elements with distinct keys the same number of groups" in {
    val collectionL = Seq(1, 2, 3, 4, 5)
    val collectionR = Seq(1, 2, 3, 4, 5)
    val grouped = new MergeJoinIterator(
      collectionL.iterator,
      collectionR.iterator,
      identity[Int],
      identity[Int]).toSeq
    grouped should have length 5
    grouped.distinct should have length 5 // to check if something wasn't included more than once
  }

  it should "group a sequence of elements with two keys into two groups" in {
    val collectionL = Seq(1 -> 10, 1 -> 11, 1 -> 12, 2 -> 20, 2 -> 21)
    val collectionR = Seq(1 -> 10, 1 -> 11, 1 -> 12, 2 -> 20, 2 -> 21)
    val grouped = new MergeJoinIterator(
      collectionL.iterator,
      collectionR.iterator,
      (x: (Int, Int)) => x._1,
      (y: (Int, Int)) => y._1).toIndexedSeq
    grouped should have length 2
    grouped(0)._1 should be(1)
    grouped(0)._2 should contain inOrder(1 -> 10, 1 -> 11, 1 -> 12)
    grouped(0)._3 should contain inOrder(1 -> 10, 1 -> 11, 1 -> 12)
    grouped(1)._1 should be(2)
    grouped(1)._2 should contain inOrder(2 -> 20, 2 -> 21)
    grouped(1)._3 should contain inOrder(2 -> 20, 2 -> 21)
  }

  it should "be lazy and work with infinite streams" in {
    val streamL = Stream.from(0)
    val streamR = Stream.from(0)
    val grouped = new MergeJoinIterator(streamL.iterator, streamR.iterator, identity[Int],
      identity[Int])
    grouped.take(5).toSeq.map(_._1) should contain inOrder(0, 1, 2, 3, 4)
  }

  it should "take from two unabalanced but ordered streams" in {
    val collectionL = Seq(1 -> "1a", 1 -> "1b", 1 -> "1c", 2 -> "2a", 3 -> "3a")
    val collectionR = Seq(1 -> "1a", 2 -> "2a", 2 -> "2b", 2 -> "2c", 4 -> "4a")
    val grouped = new MergeJoinIterator(
      collectionL.iterator,
      collectionR.iterator,
      (x: (Int, String)) => x._1,
      (y: (Int, String)) => y._1).toIndexedSeq
    grouped(0)._1 should be(1)
    grouped(0)._2 should contain inOrder(1 -> "1a", 1 -> "1b", 1 -> "1c")
    grouped(0)._3 should contain(1 -> "1a")
    grouped(1)._1 should be(2)
    grouped(1)._2 should contain(2 -> "2a")
    grouped(1)._3 should contain inOrder(2 -> "2a", 2 -> "2b", 2 -> "2c")
    grouped(2)._1 should be(3)
    grouped(2)._2 should contain(3 -> "3a")
    grouped(2)._3 should be('empty)
    grouped(3)._1 should be(4)
    grouped(3)._2 should be('empty)
    grouped(3)._3 should contain(4 -> "4a")
  }

  it should "work with more complicated keys" in {
    val collectionL = Seq((0, 0, 0, 0), (0, 0, 0, 1), (0, 0, 1, 0), (1, 0, 0, 1), (1, 1, 0, 0))
    val collectionR = Seq((0, 0, "a", "a"), (0, 0, "b", "b"), (1, 1, "c", "c"))
    val grouped = new MergeJoinIterator(
      collectionL.iterator,
      collectionR.iterator,
      (x: (Int, Int, Int, Int)) => (x._1, x._2),
      (y: (Int, Int, String, String)) => (y._1, y._2)).toSeq
    grouped should have length (3)
    grouped(0)._1 should be ((0,0))
    grouped(0)._2 should have length (3)
    grouped(0)._3 should have length (2)
    grouped(1)._1 should be ((1,0))
    grouped(1)._2 should have length (1)
    grouped(1)._3 should have length (0)
    grouped(2)._1 should be ((1,1))
    grouped(2)._2 should have length (1)
    grouped(2)._3 should have length (1)
  }

}
