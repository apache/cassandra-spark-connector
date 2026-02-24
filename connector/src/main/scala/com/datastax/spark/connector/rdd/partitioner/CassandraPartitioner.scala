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

import scala.reflect.ClassTag
import scala.util.Try

import org.apache.spark.Partitioner

import com.datastax.spark.connector.util.Logging
import com.datastax.spark.connector.cql.{CassandraConnector, TableDef}
import com.datastax.spark.connector.rdd.partitioner.dht.{Token, TokenFactory, TokenRange}
import com.datastax.spark.connector.writer.RowWriterFactory
import com.datastax.spark.connector.{ColumnSelector, PartitionKeyColumns}
import com.datastax.spark.connector.util.Logging

/** Holds a token range together with the index of a partition this token range belongs to */
case class TokenRangeWithPartitionIndex[V, T <: Token[V]](range: TokenRange[V, T], partitionIndex: Int)

object TokenRangeWithPartitionIndex {

  implicit def rangeBounds[V, T <: Token[V]](implicit tf: TokenFactory[V, T])
      : RangeBounds[TokenRangeWithPartitionIndex[V, T], T] = {
    type ITR = TokenRangeWithPartitionIndex[V, T]
    new RangeBounds[ITR, T] {
      override def start(range: ITR): T = range.range.start
      override def end(range: ITR): T = range.range.end
      override def isFull(range: ITR): Boolean = range.range.isFull
      override def contains(range: ITR, token: T): Boolean = range.range.contains(token)
    }
  }

}

/**
  * A [[org.apache.spark.Partitioner]] implementation which performs the inverse
  * operation of a traditional C* hashing. Requires the Key type and the token
  * value type V.
  *
  * Will take objects of type Key and determine given the token ranges in `indexedRanges`
  * which range the Key would belong in given the C* schema in `TableDef`
  *
  * Under the hood uses a bound statement to generate routing keys which are then
  * used the driver's internal token factories to determine the token for the
  * routing key.
  */
private[connector] class CassandraPartitioner[Key : ClassTag, V, T <: Token[V]](
  private val connector: CassandraConnector,
  private val tableDef: TableDef,
  val partitions: Seq[CassandraPartition[V, T]],
  val keyMapping: ColumnSelector = PartitionKeyColumns)(
implicit
  @transient
  private val rwf: RowWriterFactory[Key],
  tokenFactory: TokenFactory[V, T]) extends Partitioner with Logging {

  /** Changes the tableDef target of this partitioner. Can only be done within a keyspace
    * verification of key mapping will occur with the call to [[verify()]] */
  def withTableDef(tableDef: TableDef): CassandraPartitioner[Key, V, T] = {
    if (tableDef.keyspaceName != this.tableDef.keyspaceName) {
      throw new IllegalArgumentException(
        s"""Cannot apply partitioner from keyspace
           |${this.tableDef.keyspaceName} to table
           |${tableDef.keyspaceName}.${tableDef.tableName} because the keyspaces do
           |not match""".stripMargin)
    }

    new CassandraPartitioner(connector, tableDef, partitions, keyMapping)
  }

  /** Changes the current key mapping for this partitioner. Verification of the mapping
    * occurs on call to [[verify()]] */
  def withKeyMapping(keyMapping: ColumnSelector): CassandraPartitioner[Key, V, T] =
    new CassandraPartitioner(connector, tableDef, partitions, keyMapping)

  private lazy val partitionKeyNames =
    PartitionKeyColumns.selectFrom(tableDef).map(_.columnName).toSet

  private lazy val partitionKeyMapping = keyMapping
    .selectFrom(tableDef)
    .filter( colRef => partitionKeyNames.contains(colRef.columnName))

  private lazy val partitionKeyWriter = {
    logDebug(
      s"""Building Partitioner with mapping
         |${partitionKeyMapping.map(x => (x.columnName, x.selectedAs))}
         |for table $tableDef""".stripMargin)
    implicitly[RowWriterFactory[Key]]
      .rowWriter(tableDef, partitionKeyMapping)
  }

  /** Builds and makes sure we can make a rowWriter with the current TableDef and keyMapper */
  def verify(log: Boolean = true): Unit = {
    val attempt = Try(partitionKeyWriter)
    if (attempt.isFailure) {
      if (log)
        logError("Unable to build partition key writer CassandraPartitioner.", attempt.failed.get)
      throw attempt.failed.get
    }
  }

  /** Since the Token Generator relies on a (non-serializable) prepared statement we need to
    * make sure it is not serialized to executors and is made fresh on each executor */
  @transient
  private lazy val tokenGenerator =
    new TokenGenerator(connector, tableDef, partitionKeyWriter)

  private type ITR = TokenRangeWithPartitionIndex[V, T]

  @transient
  private lazy val indexedTokenRanges: Seq[ITR] =
    for (p <- partitions; tr <- p.tokenRanges) yield
      TokenRangeWithPartitionIndex(tr.range, p.index)

  @transient
  private lazy val tokenRangeLookupTable: BucketingRangeIndex[ITR, T] = {
    implicit val tokenOrdering = tokenFactory.tokenOrdering
    implicit val tokenBucketing = tokenFactory.tokenBucketing
    new BucketingRangeIndex(indexedTokenRanges)
  }

  override def getPartition(key: Any): Int = {
    key match {
      case x: Key =>
        val driverToken = tokenGenerator.getStringTokenFor(x)
        val connectorToken = tokenFactory.tokenFromString(driverToken)
        tokenRangeLookupTable.rangesContaining(connectorToken).head.partitionIndex
      case other =>
        throw new IllegalArgumentException(s"Couldn't determine the key from object $other")
    }
  }

  override def numPartitions: Int =
    partitions.length

  override def equals(that: Any): Boolean = that match {
    case that: CassandraPartitioner[Key, V, T] @unchecked =>
      (this.indexedTokenRanges == that.indexedTokenRanges
        && this.tableDef.keyspaceName == that.tableDef.keyspaceName
        && this.connector == that.connector)
    case _ =>
      false
  }

  override def hashCode: Int = {
    indexedTokenRanges.hashCode() + tableDef.keyspaceName.hashCode * 31
  }

}

