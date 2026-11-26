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

package com.datastax.spark.connector.datasource

import com.datastax.oss.driver.api.core.CqlIdentifier
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.datastax.spark.connector.cql.{CassandraConnector, TableDef}
import com.datastax.spark.connector.datasource.CassandraSourceUtil.consolidateConfs
import com.datastax.spark.connector.RowsInBatch
import com.datastax.spark.connector.writer.{BatchGroupingKey, TTLOption, TimestampOption, WriteConf}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra.ColumnReferences
import org.apache.spark.sql.cassandra.CassandraSourceRelation.{TTLParam, WriteClusteringParam, WriteTimeParam}
import org.apache.spark.sql.connector.distributions.{Distribution, Distributions}
import org.apache.spark.sql.connector.expressions.{Expression, Expressions, SortDirection, SortOrder}
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.connector.write.streaming.{StreamingDataWriterFactory, StreamingWrite}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.jdk.CollectionConverters._
import scala.util.Try

case class CassandraWriteBuilder(
  session: SparkSession,
  tableDef: TableDef,
  catalogName: String,
  options: CaseInsensitiveStringMap,
  inputSchema: StructType)
  extends WriteBuilder with SupportsTruncate {

  private val consolidatedConf = consolidateConfs(
    session.sparkContext.getConf,
    session.conf.getAll,
    catalogName,
    tableDef.keyspaceName,
    options.asScala.toMap)

  private val initialWriteConf = WriteConf.fromSparkConf(consolidatedConf)

  private val primaryKeyNames = tableDef.primaryKey.map(_.columnName).toSet
  private val inputColumnNames = inputSchema.map(_.name).toSet

  private val missingPrimaryKeyColumns = primaryKeyNames -- inputColumnNames

  if (missingPrimaryKeyColumns.nonEmpty) {
    throw new CassandraCatalogException(
      s"""Attempting to write to C* Table but missing
         |primary key columns: ${missingPrimaryKeyColumns.mkString("[", ",", "]")}""".stripMargin)
  }

  private val ttlWriteOption =
    consolidatedConf.getOption(TTLParam.name)
      .map(value =>
        Try(value.toInt)
          .map(TTLOption.constant)
          .getOrElse(TTLOption.perRow(value)))
      .getOrElse(initialWriteConf.ttl)

  private val timestampWriteOption =
    consolidatedConf.getOption(WriteTimeParam.name)
      .map(value =>
        Try(value.toLong)
          .map(TimestampOption.constant)
          .getOrElse(TimestampOption.perRow(value)))
      .getOrElse(initialWriteConf.timestamp)

  private val connector = CassandraConnector(consolidatedConf)
  private val writeConf = initialWriteConf.copy(ttl = ttlWriteOption, timestamp = timestampWriteOption)

  override def build(): Write = {
    val bulkWrite = getWrite()
    CassandraWriteClustering.clustering(tableDef, writeConf, consolidatedConf) match {
      case Some(clustering) => ClusteredCassandraWrite(bulkWrite, clustering)
      case None => CassandraWrite(bulkWrite)
    }
  }

  override def buildForBatch(): BatchWrite = getWrite()

  private def getWrite(): CassandraBulkWrite = {
    CassandraBulkWrite(session, connector, tableDef, writeConf, inputSchema, consolidatedConf)
  }

  override def buildForStreaming(): StreamingWrite = getWrite()

  /**
    * With Cassandra we cannot actually do this before commit since we are writing constantly,
    * our best option is to truncate now. Since we have no notion of rollbacks this is probably
    * the best we can do.
    **/
  override def truncate(): WriteBuilder = {
    if (consolidatedConf.getOption("confirm.truncate").getOrElse("false").toBoolean) {
      connector.withSessionDo(session =>
        session.execute(
          QueryBuilder.truncate(CqlIdentifier.fromInternal(tableDef.keyspaceName), CqlIdentifier.fromInternal(tableDef.tableName)).asCql()))
      this
    } else {
      throw new UnsupportedOperationException(
        """You are attempting to use overwrite mode which will truncate
          |this table prior to inserting data. If you would merely like
          |to change data already in the table use the "Append" mode.
          |To actually truncate please pass in true value to the option
          |"confirm.truncate" or set that value to true in the session.conf
          | when saving. """.stripMargin)
    }
  }
}

/** A write Spark plans as it comes. */
case class CassandraWrite(bulkWrite: CassandraBulkWrite) extends Write {
  override def toBatch: BatchWrite = bulkWrite
  override def toStreaming: StreamingWrite = bulkWrite
}

/** A write for which Spark clusters the rows by partition key and sorts them by primary key (per Spark task), so
  * the rows of each Cassandra partition reach the writer together and are sent in full single-partition batches. */
case class ClusteredCassandraWrite(bulkWrite: CassandraBulkWrite, clustering: CassandraWriteClustering)
  extends Write with RequiresDistributionAndOrdering {

  override def toBatch: BatchWrite = bulkWrite
  override def toStreaming: StreamingWrite = bulkWrite

  override def requiredDistribution(): Distribution =
    Distributions.clustered(clustering.distributionColumns.map(ColumnReferences.column).toArray[Expression])

  // Best effort: Spark (AQE) may split a hot Cassandra partition over several tasks, or coalesce small tasks.
  // Batching only needs the rows of a partition to be contiguous within a task.
  override def distributionStrictlyRequired(): Boolean = false

  override def requiredOrdering(): Array[SortOrder] =
    clustering.orderingColumns.map(c => Expressions.sort(ColumnReferences.column(c), SortDirection.ASCENDING)).toArray

  override def description(): String = s"${super.description()} clustered by " +
    s"${clustering.distributionColumns.mkString("[", ", ", "]")}, sorted by ${clustering.orderingColumns.mkString("[", ", ", "]")}"
}

/** Columns to cluster a write by (the partition key) and to sort it by (the primary key). */
case class CassandraWriteClustering(distributionColumns: Seq[String], orderingColumns: Seq[String])

object CassandraWriteClustering {

  sealed trait Setting
  case object Auto extends Setting
  case object On extends Setting
  case object Off extends Setting

  def setting(conf: SparkConf): Setting =
    conf.get(WriteClusteringParam.name, WriteClusteringParam.default).trim.toLowerCase match {
      case "auto" => Auto
      case "on" | "true" => On
      case "off" | "false" => Off
      case invalid => throw new IllegalArgumentException(
        s"""$invalid is not a valid ${WriteClusteringParam.name} value.
           |${WriteClusteringParam.description}""".stripMargin)
    }

  /** Whether clustering the rows can fill batches: only if a partition can hold several rows and rows are
    * batched by partition. Not for counters, whose updates are not idempotent: no reason to add a shuffle to them. */
  def batchingProfits(tableDef: TableDef, writeConf: WriteConf): Boolean =
    tableDef.clusteringColumns.nonEmpty &&
      writeConf.batchGroupingKey == BatchGroupingKey.Partition &&
      writeConf.batchSize != RowsInBatch(1) &&
      !tableDef.columns.exists(_.isCounterColumn)

  def clustering(tableDef: TableDef, writeConf: WriteConf, conf: SparkConf): Option[CassandraWriteClustering] = {
    val enabled = setting(conf) match {
      case On => true
      case Off => false
      case Auto => batchingProfits(tableDef, writeConf)
    }
    if (enabled) {
      Some(CassandraWriteClustering(
        tableDef.partitionKey.map(_.columnName),
        tableDef.primaryKey.map(_.columnName)))
    } else {
      None
    }
  }
}

case class CassandraBulkWrite(
  session: SparkSession,
  connector: CassandraConnector,
  tableDef: TableDef,
  writeConf: WriteConf,
  inputSchema: StructType,
  consolidatedConf: SparkConf)
  extends BatchWrite
    with StreamingWrite {


  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = getWriterFactory()

  override def commit(messages: Array[WriterCommitMessage]): Unit = {} //No Commit in Cassandra Driver Writes

  override def abort(messages: Array[WriterCommitMessage]): Unit = {} //No Abort possible in Cassandra Driver Writes

  override def createStreamingWriterFactory(info: PhysicalWriteInfo): StreamingDataWriterFactory = getWriterFactory()

  override def useCommitCoordinator(): Boolean = super.useCommitCoordinator()

  private def getWriterFactory(): CassandraDriverDataWriterFactory = {
    CassandraDriverDataWriterFactory(
      connector,
      tableDef,
      inputSchema,
      writeConf
    )
  }

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}

  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}
}
