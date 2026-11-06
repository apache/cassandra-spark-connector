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

import org.scalatest.{Matchers, FlatSpec}

import scala.collection.mutable.ArrayBuffer

class BufferedIterator2Spec extends FlatSpec with Matchers {

  "BufferedIterator" should "return the same items as the standard Iterator" in {
    val iterator = new BufferedIterator2(Seq(1, 2, 3, 4, 5).iterator)
    iterator.hasNext shouldBe true
    iterator.next() shouldBe 1
    iterator.hasNext shouldBe true
    iterator.next() shouldBe 2
    iterator.hasNext shouldBe true
    iterator.next() shouldBe 3
    iterator.hasNext shouldBe true
    iterator.next() shouldBe 4
    iterator.hasNext shouldBe true
    iterator.next() shouldBe 5
    iterator.hasNext shouldBe false
  }

  it should "be convertible to a Seq" in {
    val iterator = new BufferedIterator2(Seq(1, 2, 3, 4, 5).iterator)
    iterator.toSeq should contain inOrder(1, 2, 3, 4, 5)
  }

  it should "wrap an empty iterator" in {
    val iterator = new BufferedIterator2(Iterator.empty)
    iterator.isEmpty shouldBe true
    iterator.hasNext shouldBe false
  }

  it should "offer the head element without consuming the underlying iterator" in {
    val iterator = new BufferedIterator2(Seq(1, 2, 3, 4, 5).iterator)
    iterator.head shouldBe 1
    iterator.next() shouldBe 1
  }

  it should "offer takeWhile that consumes only the elements matching the predicate" in {
    val iterator = new BufferedIterator2(Seq(1, 2, 3, 4, 5).iterator)
    val firstThree = iterator.takeWhile(_ <= 3).toList

    firstThree should contain inOrder (1, 2, 3)
    iterator.head shouldBe 4
    iterator.next() shouldBe 4
  }

  it should "offer appendWhile that copies elements to ArrayBuffer and consumes only the elements matching the predicate" in {
    val iterator = new BufferedIterator2(Seq(1, 2, 3, 4, 5).iterator)
    val buffer = new ArrayBuffer[Int]
    iterator.appendWhile(_ <= 3, buffer)

    buffer should contain inOrder (1, 2, 3)
    iterator.head shouldBe 4
    iterator.next() shouldBe 4
  }

  it should "throw NoSuchElementException if trying to get next() element that doesn't exist" in {
    val iterator = new BufferedIterator2(Seq(1, 2).iterator)
    iterator.next()
    iterator.next()
    a [NoSuchElementException] should be thrownBy iterator.next()
  }
}
