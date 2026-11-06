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

import org.scalatest.{FlatSpec, Matchers}

import _root_.io.teknek.arizona.spark.connector.rdd.partitioner.dht.TokenFactory.Murmur3TokenFactory
import _root_.io.teknek.arizona.spark.connector.rdd.partitioner.dht.TokenFactory.Murmur3TokenFactory._
import _root_.io.teknek.arizona.spark.connector.rdd.partitioner.dht.{LongToken, TokenRange}

class Murmur3PartitionerTokenRangeSplitterSpec
  extends FlatSpec
  with SplitterBehaviors[Long, LongToken]
  with Matchers {

  private val splitter = new Murmur3PartitionerTokenRangeSplitter

  "Murmur3PartitionerSplitter" should "split tokens" in testSplittingTokens(splitter)

  it should "split token sequences" in testSplittingTokenSequences(splitter)

  override def splitWholeRingIn(count: Int): Seq[TokenRange[Long, LongToken]] = {
    val hugeTokensIncrement = totalTokenCount / count
    (0 until count).map(i =>
      range(minToken.value + i * hugeTokensIncrement, minToken.value + (i + 1) * hugeTokensIncrement)
    )
  }

  override def range(start: BigInt, end: BigInt): TokenRange[Long, LongToken] =
    new TokenRange[Long, LongToken](
      LongToken(start.toLong),
      LongToken(end.toLong),
      Set(InetAddress.getLocalHost),
      Murmur3TokenFactory)
}