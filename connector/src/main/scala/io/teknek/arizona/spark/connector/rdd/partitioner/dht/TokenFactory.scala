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

import _root_.io.teknek.arizona.spark.connector.cql.CassandraConnector

import scala.language.existentials

import _root_.io.teknek.arizona.spark.connector.rdd.partitioner.MonotonicBucketing

trait TokenFactory[V, T <: Token[V]] extends Serializable {
  def minToken: T
  def maxToken: T

  /** Total token count in a ring. */
  def totalTokenCount: BigInt

  /** Number of tokens in a range from `token1` to `token2`.
    * If `token2 < token1`, then range wraps around. */
  def distance(token1: T, token2: T): BigInt

  /** Fraction of the ring in a range from `token1` to `token2`.
    * If `token2 < token1`, then range wraps around.
    * Returns 1.0 for a full ring range, 0.0 for an empty range. */
   def ringFraction(token1: T, token2: T): Double =
    distance(token1, token2).toDouble / totalTokenCount.toDouble

  /** Creates a token from its string representation */
  def tokenFromString(string: String): T

  /** Converts a token to its string representation */
  def tokenToString(token: T): String

  /** Defines how to compare tokens */
  implicit def tokenOrdering: Ordering[T]

  /** Defines how to group tokens into buckets, useful for partitioning  */
  implicit def tokenBucketing: MonotonicBucketing[T]
}

object TokenFactory {

  type V = t forSome { type t }
  type T = t forSome { type t <: Token[V] }

  implicit object Murmur3TokenFactory extends TokenFactory[Long, LongToken] {
    override val minToken = LongToken(Long.MinValue)
    override val maxToken = LongToken(Long.MaxValue)
    override val totalTokenCount = BigInt(maxToken.value) - BigInt(minToken.value)
    override def tokenFromString(string: String) = LongToken(string.toLong)
    override def tokenToString(token: LongToken) = token.value.toString

    override def distance(token1: LongToken, token2: LongToken): BigInt = {
      val left = token1.value
      val right = token2.value
      if (right > left) BigInt(right) - BigInt(left)
      else BigInt(right) - BigInt(left) + totalTokenCount
    }

    override def tokenBucketing: MonotonicBucketing[LongToken] =
      implicitly[MonotonicBucketing[LongToken]]

    override def tokenOrdering: Ordering[LongToken] =
      Ordering.by(_.value)
  }

  implicit object RandomPartitionerTokenFactory extends TokenFactory[BigInt, BigIntToken] {
    override val minToken = BigIntToken(-1)
    override val maxToken = BigIntToken(BigInt(2).pow(127))
    override val totalTokenCount = maxToken.value - minToken.value
    override def tokenFromString(string: String) = BigIntToken(BigInt(string))
    override def tokenToString(token: BigIntToken) = token.value.toString()

    override def distance(token1: BigIntToken, token2: BigIntToken) = {
      val left = token1.value
      val right = token2.value
      if (right > left) right - left
      else right - left + totalTokenCount
    }

    override def tokenBucketing: MonotonicBucketing[BigIntToken] =
      implicitly[MonotonicBucketing[BigIntToken]]

    override def tokenOrdering: Ordering[BigIntToken] =
      Ordering.by(_.value)
  }


  def forCassandraPartitioner(partitionerClassName: String): TokenFactory[V, T] = {
    val partitioner =
      partitionerClassName match {
        case "org.apache.cassandra.dht.Murmur3Partitioner" => Murmur3TokenFactory
        case "org.apache.cassandra.dht.RandomPartitioner" => RandomPartitionerTokenFactory
        case _ => throw new IllegalArgumentException(s"Unsupported partitioner: $partitionerClassName")
      }
    partitioner.asInstanceOf[TokenFactory[V, T]]
  }

  def forSystemLocalPartitioner(connector: CassandraConnector): TokenFactory[V, T] = {
    val partitionerClassName = connector.withSessionDo { session =>
      session.execute("SELECT partitioner FROM system.local").one().getString(0)
    }
    forCassandraPartitioner(partitionerClassName)
  }
}




