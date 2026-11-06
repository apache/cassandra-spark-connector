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

import _root_.io.teknek.arizona.spark.connector.rdd.partitioner.dht.LongToken

/** Fast token range splitter assuming that data are spread out evenly in the whole range. */
private[partitioner] class Murmur3PartitionerTokenRangeSplitter
  extends TokenRangeSplitter[Long, LongToken] {

  private type TokenRange = io.teknek.arizona.spark.connector.rdd.partitioner.dht.TokenRange[Long, LongToken]

  override def split(tokenRange: TokenRange, splitSize: Int): Seq[TokenRange] = {
    val rangeSize = tokenRange.rangeSize
    val splitPointsCount = if (rangeSize < splitSize) rangeSize.toInt else splitSize
    val splitPoints = (0 until splitPointsCount).map({ i =>
      new LongToken(tokenRange.start.value + (rangeSize * i / splitPointsCount).toLong)
    }) :+ tokenRange.end

    for (Seq(left, right) <- splitPoints.sliding(2).toSeq) yield
      new TokenRange(left, right, tokenRange.replicas, tokenRange.tokenFactory)
  }
}
