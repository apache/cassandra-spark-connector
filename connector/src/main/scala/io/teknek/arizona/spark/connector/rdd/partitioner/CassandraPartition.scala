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

import org.apache.spark.Partition
import _root_.io.teknek.arizona.spark.connector.rdd.partitioner.dht.{Token, TokenFactory, TokenRange}
import org.apache.spark.sql.connector.read.InputPartition

/** Stores a CQL `WHERE` predicate matching a range of tokens. */
case class CqlTokenRange[V, T <: Token[V]](range: TokenRange[V, T])(implicit tf: TokenFactory[V, T]) {

  require(!range.isWrappedAround)

  def cql(pk: String): (String, Seq[Any]) =
    if (range.start == tf.minToken && range.end == tf.minToken)
      (s"token($pk) >= ?", Seq(range.start.value))
    else if (range.start == tf.minToken)
      (s"token($pk) <= ?", Seq(range.end.value))
    else if (range.end == tf.minToken)
      (s"token($pk) > ?", Seq(range.start.value))
    else
      (s"token($pk) > ? AND token($pk) <= ?", Seq(range.start.value, range.end.value))
}

trait EndpointPartition extends Partition {
  def endpoints: Array[String]
}

/** Metadata describing Cassandra table partition processed by a single Spark task.
  * Beware the term "partition" is overloaded. Here, in the context of Spark,
  * it means an arbitrary collection of rows that can be processed locally on a single Cassandra cluster node.
  * A `CassandraPartition` typically contains multiple CQL partitions, i.e. rows identified by different values of
  * the CQL partitioning key.
  *
  * @param index identifier of the partition, used internally by Spark
  * @param endpoints which nodes the data partition is located on
  * @param tokenRanges token ranges determining the row set to be fetched
  * @param dataSize estimated amount of data in the partition
  */
case class CassandraPartition[V, T <: Token[V]] (
  index: Int,
  endpoints: Array[String],
  tokenRanges: Iterable[CqlTokenRange[V, T]],
  dataSize: Long) extends EndpointPartition with InputPartition {

  override def preferredLocations(): Array[String] = endpoints
}

