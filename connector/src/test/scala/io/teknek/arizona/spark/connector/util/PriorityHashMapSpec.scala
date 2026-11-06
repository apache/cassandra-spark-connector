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

import org.scalatest.{Matchers, WordSpec}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class PriorityHashMapSpec extends WordSpec with Matchers {

  private def putItems(m: PriorityHashMap[Int, Int], keys: Seq[Int], values: Seq[Int]): Unit = {
    for ((k, v) <- keys zip values) {
      m.put(k, v)
      m.contains(k) shouldBe true
      m(k) shouldBe v
      m.get(k) shouldBe Some(v)
    }
    m.size shouldBe keys.distinct.size
    m.headValue shouldBe values.max
  }

  private def removeKeys(m: PriorityHashMap[Int, Int], keys: Seq[Int]): Unit = {
    var size = m.size
    for (k <- keys) {
      if (m.remove(k))
        size -= 1
      m.contains(k) shouldBe false
      m.get(k) shouldBe None
    }
    m.size shouldBe size
  }

  private val capacity = 1024
  private val maxKey = capacity

  private def createSortedMap: PriorityHashMap[Int, Int] = {
    val m = new PriorityHashMap[Int, Int](capacity)
    val keys = 1 to maxKey
    val values = 1 to maxKey
    putItems(m, keys, values)
    m
  }

  private def createRandomValueMap: PriorityHashMap[Int, Int] = {
    val m = new PriorityHashMap[Int, Int](capacity)
    val keys = 1 to maxKey
    val values = random.shuffle(1 to maxKey)
    putItems(m, keys, values)
    m
  }

  private def createTotallyRandomMap: PriorityHashMap[Int, Int] = {
    val m = new PriorityHashMap[Int, Int](capacity)
    val keys = Iterator.continually(random.nextInt()).take(capacity).toSeq
    val values = Iterator.continually(random.nextInt()).take(capacity).toSeq
    putItems(m, keys, values)
    m
  }

  private val random = new Random

  "A PriorityHashMap" should {
    "support adding elements (simple)" in {
      val m = new PriorityHashMap[Int, Int](8)
      m.put(1, 3)
      m.put(3, 1)
      m.put(2, 2)
      m.remove(2)
      m.remove(1)
      m.remove(3)
      m.isEmpty shouldBe true
    }

    "support adding elements ascending by value" in {
      val m = new PriorityHashMap[Int, Int](capacity)
      val keys = 1 to maxKey
      val values = 1 to maxKey
      putItems(m, keys, values)
    }

    "support adding elements descending by value" in {
      val m = new PriorityHashMap[Int, Int](capacity)
      val keys = 1 to maxKey
      val values = (1 to maxKey).reverse
      putItems(m, keys, values)
    }

    "support adding elements in random order of values" in {
      val m = new PriorityHashMap[Int, Int](capacity)
      val keys = 1 to maxKey
      val values = random.shuffle(1 to maxKey)
      putItems(m, keys, values)
    }

    "support adding elements in random order of values and keys" in {
      val m = new PriorityHashMap[Int, Int](capacity)
      val keys = Iterator.continually(random.nextInt()).take(capacity).toSeq
      val values = Iterator.continually(random.nextInt()).take(capacity).toSeq
      putItems(m, keys, values)
    }

    "support removing elements in ascending order" in {
      val m = createSortedMap
      removeKeys(m, 1 to maxKey)
      m.isEmpty shouldBe true
    }

    "support removing elements in descending order" in {
      val m = createSortedMap
      removeKeys(m, (1 to maxKey).reverse)
      m.isEmpty shouldBe true
    }

    "support removing elements in random order from a sorted map" in {
      val m = createSortedMap
      removeKeys(m, random.shuffle(1 to maxKey))
      m.isEmpty shouldBe true
    }

    "support removing elements from a randomly created map in random order" in {
      val m = createTotallyRandomMap
      removeKeys(m, random.shuffle(m.keys))
      m.isEmpty shouldBe true
    }

    "allow to heapsort an array of integers" in {
      val m = createRandomValueMap
      val out = new ArrayBuffer[Int]
      while (m.nonEmpty) {
        out.append(m.headValue)
        m.remove(m.headKey)
      }
      for (i <- 0 until out.size - 1) {
        out(i) should be >= out(i + 1)
      }
    }

    "allow to update item priority" in {
      val m = createSortedMap

      m.headKey shouldBe maxKey
      m.headValue shouldBe maxKey

      for (k <- random.shuffle(1 to maxKey))
        m.put(k, -k)   // reverse priority

      // now (1, -1) will be the pair with the largest value:
      m.headKey shouldBe 1
      m.headValue shouldBe -1
    }

    "be able to store multiple items with the same priority" in {
      val m = new PriorityHashMap[Int, Int](capacity)
      m.put(0, 0)
      m.put(1, 0)
      m.put(2, 0)
      m.put(3, 0)
      m.put(4, 0)
      m.headValue shouldBe 0
      m.size shouldBe 5

      m.contains(0) shouldBe true
      m.contains(1) shouldBe true
      m.contains(2) shouldBe true
      m.contains(3) shouldBe true
      m.contains(4) shouldBe true

      m.remove(3) shouldBe true
      m.contains(3) shouldBe false
      m.contains(4) shouldBe true
      m.size shouldBe 4
    }


    "return false when removing a non-existing key" in {
      val m = new PriorityHashMap[Int, Int](capacity)
      m.put(1, 1)
      m.put(2, 2)
      m.put(2, 4)
      m.remove(2) shouldBe true
      m.remove(10) shouldBe false
    }

    "have capacity rounded up to the nearest power of two" in {
      new PriorityHashMap[Int, Int](7).capacity shouldBe 8
      new PriorityHashMap[Int, Int](8).capacity shouldBe 8
      new PriorityHashMap[Int, Int](9).capacity shouldBe 16
      new PriorityHashMap[Int, Int](15).capacity shouldBe 16
      new PriorityHashMap[Int, Int](16).capacity shouldBe 16
      new PriorityHashMap[Int, Int](17).capacity shouldBe 32
    }

    "throw NoSuchElement exception if requested a head of empty map" in {
      val emptyMap = new PriorityHashMap[Int, String](capacity)
      an [NoSuchElementException] should be thrownBy emptyMap.headKey
      an [NoSuchElementException] should be thrownBy emptyMap.headValue
    }

    "throw NoSuchElement exception if requested a non-existing key" in {
      val emptyMap = new PriorityHashMap[Int, String](capacity)
      an [NoSuchElementException] should be thrownBy emptyMap(1)
    }

    "throw IllegalStateException exception if trying to exceed allowed capacity" in {
      val m = new PriorityHashMap[Int, Int](capacity)
      for (i <- 0 until m.capacity)
        m.put(i, i)
      an [IllegalStateException] should be thrownBy m.put(m.capacity, m.capacity)

      m.remove(0)
      m.put(m.capacity, m.capacity)  // now should be ok
    }
  }

}
