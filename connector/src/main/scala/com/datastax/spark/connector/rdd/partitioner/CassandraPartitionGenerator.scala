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

package com.datastax.spark.connector.rdd.partitioner

import com.datastax.oss.driver.api.core.CqlIdentifier
import com.datastax.oss.driver.api.core.metadata.TokenMap
import com.datastax.oss.driver.api.core.metadata.token.{TokenRange => DriverTokenRange}

import scala.language.existentials
import scala.reflect.ClassTag
import scala.util.Try
import com.datastax.spark.connector.util.DriverUtil._
import com.datastax.spark.connector.util.{DriverUtil, Logging}
import com.datastax.spark.connector.ColumnSelector
import com.datastax.spark.connector.cql.{CassandraConnector, TableDef}
import com.datastax.spark.connector.rdd.partitioner.dht.{Token, TokenFactory}
import com.datastax.spark.connector.writer.RowWriterFactory
import org.apache.spark.sql.connector.read.InputPartition
import scala.jdk.CollectionConverters._


/** Creates CassandraPartitions for given Cassandra table */
private[connector] class CassandraPartitionGenerator[V, T <: Token[V]](
    connector: CassandraConnector,
    tableDef: TableDef,
    splitCount: Int)(
  implicit
    tokenFactory: TokenFactory[V, T]) extends Logging{

  type Token = com.datastax.spark.connector.rdd.partitioner.dht.Token[T]
  type TokenRange = com.datastax.spark.connector.rdd.partitioner.dht.TokenRange[V, T]

  private val keyspaceName = CqlIdentifier.fromInternal(tableDef.keyspaceName) // TODO Lets fix all this later


  private def tokenRange(range: DriverTokenRange, metadata: TokenMap): TokenRange = {

    val startToken = tokenFactory.tokenFromString(metadata.format(range.getStart))
    val endToken = tokenFactory.tokenFromString(metadata.format(range.getEnd))
    val replicas = metadata
      .getReplicas(keyspaceName, range).asScala
      .map(node =>
        DriverUtil.toAddress(node)
          .getOrElse(throw new IllegalStateException(s"Unable to determine Node Broadcast Address of $node")))
      .map(_.getAddress)
      .toSet
    new TokenRange(startToken, endToken, replicas, tokenFactory)
  }

  private[partitioner] def describeRing: Seq[TokenRange] = {
    val ranges = connector.withSessionDo { session =>
      val tokenMap = Option(session.getMetadata.getTokenMap.get)
        .getOrElse(throw new IllegalStateException("Unable to determine Token Range Metadata"))
      for (tr <- tokenMap.getTokenRanges().asScala) yield tokenRange(tr, tokenMap)
    }

    /**
      * When we have a single Spark Partition use a single global range. This
      * will let us more easily deal with Partition Key equals and In clauses
      */
    if (splitCount == 1) {
      Seq(ranges.head.copy[V, T](tokenFactory.minToken, tokenFactory.minToken))
    } else {
      ranges.toSeq
    }
  }

  private def createTokenRangeSplitter: TokenRangeSplitter[V, T] = {
    tokenFactory.asInstanceOf[TokenFactory[_, _]] match {
      case TokenFactory.RandomPartitionerTokenFactory =>
        new RandomPartitionerTokenRangeSplitter().asInstanceOf[TokenRangeSplitter[V, T]]
      case TokenFactory.Murmur3TokenFactory =>
        new Murmur3PartitionerTokenRangeSplitter().asInstanceOf[TokenRangeSplitter[V, T]]
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported TokenFactory $tokenFactory")
    }
  }

  private def rangeToCql(range: TokenRange): Seq[CqlTokenRange[V, T]] =
    range.unwrap.map(CqlTokenRange(_))

  def partitions: Seq[CassandraPartition[V, T]] = {
    val hostAddresses = new NodeAddresses(connector)
    val tokenRanges = describeRing
    val endpointCount = tokenRanges.map(_.replicas).reduce(_ ++ _).size
    if (endpointCount == 0)
      throw new IllegalArgumentException(s"Could not retrieve endpoints for the given table " +
        s"(${keyspaceName}.${tableDef.name}), are you trying to read a table view? Table views are not supported, " +
        s"see SPARKC-612.")
    val maxGroupSize = tokenRanges.size / endpointCount

    val splitter = createTokenRangeSplitter
    val splits = splitter.split(tokenRanges, splitCount).toSeq

    val clusterer = new TokenRangeClusterer[V, T](splitCount, maxGroupSize)
    val tokenRangeGroups = clusterer.group(splits).toArray

    val partitions = for (group <- tokenRangeGroups) yield {
      val replicas = group.map(_.replicas).reduce(_ intersect _)
      val rowCount = group.map(_.rangeSize).sum
      val cqlRanges = group.flatMap(rangeToCql)
      // partition index will be set later
      CassandraPartition(0, replicas.flatMap(hostAddresses.hostNames).toArray, cqlRanges, rowCount.toLong)
    }

    // sort partitions and assign sequential numbers so that
    // partition index matches the order of partitions in the sequence

    // group partitions with the same endpoints.
    // Less endpoints partition has -- harder to find local executor for it
    // sort by endpoints size to  distribute partition with less endpoints first.
    // sort partition by size inside each group to start proccessing with big partitions
    val partitionsGroupedByEndpoints = partitions.groupBy(_.endpoints).toSeq.sortBy(_._1.size).map(_._2.sortBy(-_.dataSize))
    // merge all groups to distribute load eventually for all endpoints.
    // Loop over groups and get next element of each
    // emmit them in order. So we get one partition for different endpoints.
    val groupIterators = partitionsGroupedByEndpoints.map(_.iterator)
    val roundRobinSelectedPartitions = Iterator.continually { groupIterators.filter(_.hasNext).map(_.next()) }
      .takeWhile(_.nonEmpty).flatten.toSeq

    val indexedPartitions = roundRobinSelectedPartitions
      .zipWithIndex
      .map { case (p, index) => p.copy(index = index) }
    indexedPartitions
  }

  def getInputPartitions(): Array[InputPartition] = {
    partitions.toArray
  }

  /**
    * Attempts to build a partitioner for this C* RDD if it was keyed with Type Key. If possible
    * returns a partitioner of type Key. The type is required so we know what kind of objects we
    * will need to bind to prepared statements when determining the token on new objects.
    */
  def partitioner[Key: ClassTag : RowWriterFactory](
      keyMapper: ColumnSelector): Option[CassandraPartitioner[Key, V, T]] = {

    val part = Try {
      val newPartitioner = new CassandraPartitioner(connector, tableDef, partitions, keyMapper)
      // This is guaranteed to succeed so we don't want to send out an ERROR message if it breaks
      newPartitioner.verify(log = false)
      newPartitioner
    }

    if (part.isFailure) {
      logDebug(s"Not able to automatically create a partitioner: ${part.failed.get.getMessage}")
    }

    part.toOption
  }
}

object CassandraPartitionGenerator {

  type V = t forSome { type t }
  type T = t forSome { type t <: Token[V] }

  /** Creates a `CassandraPartitionGenerator` for the given cluster and table.
    * Unlike the class constructor, this method does not take the generic `V` and `T` parameters,
    * and therefore you don't need to specify the ones proper for the partitioner used in the
    * Cassandra cluster. */
  def apply(
    conn: CassandraConnector,
    tableDef: TableDef,
    splitCount: Int)(
    implicit tokenFactory: TokenFactory[V, T]): CassandraPartitionGenerator[V, T] = {

    new CassandraPartitionGenerator(conn, tableDef, splitCount)(tokenFactory)
  }
}
