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

import java.net.InetAddress

import org.scalatest.{Matchers, _}

import _root_.io.teknek.arizona.spark.connector.rdd.partitioner.dht.TokenFactory.RandomPartitionerTokenFactory
import _root_.io.teknek.arizona.spark.connector.rdd.partitioner.dht.TokenFactory.RandomPartitionerTokenFactory.{minToken, totalTokenCount}
import _root_.io.teknek.arizona.spark.connector.rdd.partitioner.dht.{BigIntToken, TokenRange}

class RandomPartitionerTokenRangeSplitterSpec
  extends FlatSpec
    with SplitterBehaviors[BigInt, BigIntToken]
    with Matchers {

  private val splitter = new RandomPartitionerTokenRangeSplitter

  "RandomPartitionerSplitter" should "split tokens" in testSplittingTokens(splitter)

  it should "split token sequences" in testSplittingTokenSequences(splitter)

  override def splitWholeRingIn(count: Int): Seq[TokenRange[BigInt, BigIntToken]] = {
    val hugeTokensIncrement = totalTokenCount / count
    (0 until count).map(i =>
      range(minToken.value + i * hugeTokensIncrement, minToken.value + (i + 1) * hugeTokensIncrement)
    )
  }

  override def range(start: BigInt, end: BigInt): TokenRange[BigInt, BigIntToken] =
    new TokenRange[BigInt, BigIntToken](
      BigIntToken(start),
      BigIntToken(end),
      Set(InetAddress.getLocalHost),
      RandomPartitionerTokenFactory)
}
