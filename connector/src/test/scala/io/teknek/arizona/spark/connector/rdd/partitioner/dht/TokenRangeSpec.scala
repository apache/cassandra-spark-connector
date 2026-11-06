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

package io.teknek.arizona.spark.connector.rdd.partitioner.dht

import org.scalatest.{FlatSpec, Matchers}
import _root_.io.teknek.arizona.spark.connector.rdd.partitioner.dht.TokenFactory.{Murmur3TokenFactory, RandomPartitionerTokenFactory}

class TokenRangeSpec extends FlatSpec with Matchers  {

  type LongRange = TokenRange[Long, LongToken]
  type BigRange = TokenRange[BigInt, BigIntToken]


  "LongRanges " should " contain tokens with easy no wrapping bounds" in {
    val lr = new LongRange(LongToken(-100), LongToken(10000), Set.empty, Murmur3TokenFactory)
    //Tokens Inside
    for (l <- 1 to 1000) {
      lr.contains(LongToken(l)) should be (true)
    }

    //Tokens outside
    for (l <- -500 to -300) {
      lr.contains((LongToken(l))) should be (false)
    }
  }

  it should " contain tokens with wrapping bounds in" in {
    val lr = new LongRange(LongToken(1000), LongToken(-1000), Set.empty, Murmur3TokenFactory)

    //Tokens Inside
    for (l <- 30000 to 30500) {
      lr.contains((LongToken(l))) should be(true)
    }

    //Tokens outside
    for (l <- -500 to 500) {
      lr.contains(LongToken(l)) should be(false)
    }

  }

  "BigRanges " should " contain tokens with easy no wrapping bounds" in {
    val lr = new BigRange(BigIntToken(-100), BigIntToken(10000), Set.empty, RandomPartitionerTokenFactory)
    //Tokens Inside
    for (l <- 1 to 1000) {
      lr.contains(BigIntToken(l)) should be (true)
    }

    //Tokens outside
    for (l <- -500 to -300) {
      lr.contains((BigIntToken(l))) should be (false)
    }
  }

  it should " contain tokens with wrapping bounds in" in {
    val lr = new BigRange(BigIntToken(1000), BigIntToken(100), Set.empty, RandomPartitionerTokenFactory)

    //Tokens Inside
    for (l <- 0 to 50) {
      lr.contains(BigIntToken(l)) should be (true)
    }

    //Tokens outside
    for (l <- 200 to 500) {
      lr.contains((BigIntToken(l))) should be (false)
    }
  }

}
