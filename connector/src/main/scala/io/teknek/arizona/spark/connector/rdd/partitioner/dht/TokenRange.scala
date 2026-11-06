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

import java.net.InetAddress

import com.datastax.oss.driver.api.core.metadata.token.{Token => NativeToken}

case class TokenRange[V, T <: Token[V]] (
    start: T, end: T, replicas: Set[InetAddress], tokenFactory: TokenFactory[V, T]) {

  private[partitioner] lazy val rangeSize = tokenFactory.distance(start, end)

  private[partitioner] lazy val ringFraction = tokenFactory.ringFraction(start, end)

  def isWrappedAround(implicit tf: TokenFactory[V, T]): Boolean =
    start >= end && end != tf.minToken

  def isFull(implicit tf: TokenFactory[V, T]): Boolean =
    start == end && end == tf.minToken

  def isEmpty(implicit tf: TokenFactory[V, T]): Boolean =
    start == end && end != tf.minToken

  def unwrap(implicit tf: TokenFactory[V, T]): Seq[TokenRange[V, T]] = {
    val minToken = tf.minToken
    if (isWrappedAround)
      Seq(
        TokenRange(start, minToken, replicas, tokenFactory),
        TokenRange(minToken, end, replicas, tokenFactory))
    else
      Seq(this)
  }

  def contains(token: T)(implicit tf: TokenFactory[V, T]): Boolean = {
    (end == tf.minToken && token > start
      || start == tf.minToken && token <= end
      || !isWrappedAround && token > start && token <= end
      || isWrappedAround && (token > start || token <= end))
  }

  def startNativeToken(): NativeToken = start.nativeToken

  def endNativeToken(): NativeToken = end.nativeToken
}

