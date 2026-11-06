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

class Murmur3TokenFactorySpec extends FlatSpec with Matchers {

  val factory = TokenFactory.Murmur3TokenFactory
  
  "Murmur3TokenFactory" should "create a token from String" in {
    factory.tokenFromString("0") shouldBe LongToken(0L)
    factory.tokenFromString("-1") shouldBe LongToken(-1L)
    factory.tokenFromString(Long.MaxValue.toString) shouldBe factory.maxToken
    factory.tokenFromString(Long.MinValue.toString) shouldBe factory.minToken
  }

  it should "create a String representation of a token" in {
    factory.tokenToString(LongToken(0L)) shouldBe "0"
    factory.tokenToString(LongToken(-1L)) shouldBe "-1"
    factory.tokenToString(factory.maxToken) shouldBe Long.MaxValue.toString
    factory.tokenToString(factory.minToken) shouldBe Long.MinValue.toString
  }

  it should "calculate the distance between tokens if right > left" in {
    factory.distance(LongToken(0L), LongToken(1L)) shouldBe BigInt(1)
  }

  it should "calculate the distance between tokens if right <= left" in {
    factory.distance(LongToken(0L), LongToken(0L)) shouldBe factory.totalTokenCount
    factory.distance(factory.maxToken, factory.minToken) shouldBe BigInt(0)
  }

  it should "calculate ring fraction" in {
    factory.ringFraction(LongToken(0L), LongToken(0L)) shouldBe 1.0
    factory.ringFraction(LongToken(0L), factory.maxToken) shouldBe 0.5
    factory.ringFraction(factory.maxToken, LongToken(0L)) shouldBe 0.5
  }
}
