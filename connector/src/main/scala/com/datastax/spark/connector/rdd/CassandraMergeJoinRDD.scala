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

package com.datastax.spark.connector.rdd

import java.io.IOException

import com.datastax.bdp.util.ScalaJavaUtil.asScalaFuture

import scala.language.existentials
import scala.reflect.ClassTag
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.metrics.InputMetricsUpdater
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{BoundStatement, Row}
import com.datastax.oss.driver.api.core.metadata.Metadata
import com.datastax.oss.driver.api.core.metadata.token.Token
import com.datastax.spark.connector.CassandraRowMetadata
import com.datastax.spark.connector.cql.{CassandraConnector, ColumnDef}
import com.datastax.spark.connector.rdd.partitioner.{CassandraPartition, CqlTokenRange, NodeAddresses}
import com.datastax.spark.connector.rdd.reader.{PrefetchingResultSetIterator, RowReader}
import com.datastax.spark.connector.types.ColumnType
import com.datastax.spark.connector.util.Quote._
import com.datastax.spark.connector.util.{CountingIterator, MergeJoinIterator, NameTools, schemaFromCassandra}

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._

/**
  * A RDD which pulls from two separate CassandraTableScanRDDs which share partition keys and
  * keyspaces. These tables will be joined on READ using a merge iterator. As long as we join
  * on the token of the partition key the two iterators should be read in order.
  */
class CassandraMergeJoinRDD[L,R](
    sc: SparkContext,
    leftScanRDD: CassandraTableScanRDD[L],
    rightScanRDD: CassandraTableScanRDD[R])(
  implicit
    leftClassTag: ClassTag[L],
    rightClassTag: ClassTag[R])
  extends RDD[(Seq[L], Seq[R])](sc, Seq.empty){

  val connector = leftScanRDD.connector

  def getPartitionKey(connector: CassandraConnector, keyspaceName: String, tableName: String): Seq[ColumnDef] = {
    schemaFromCassandra(connector, Some(keyspaceName), Some(tableName)).tables.headOption match {
      case Some(table) => table.partitionKey
      case None => {
        val metadata: Metadata = connector.withSessionDo(_.getMetadata)
        val suggestions = NameTools.getSuggestions(metadata, keyspaceName, tableName)
        val errorMessage = NameTools.getErrorString(keyspaceName, Some(tableName), suggestions)
        throw new IOException(errorMessage)
      }
    }
  }

  def checkValidMergeJoin() {
    val leftPartitionKeyTypes = getPartitionKey(
      leftScanRDD.connector,
      leftScanRDD.keyspaceName,
      leftScanRDD.tableName)
      .map(_.columnType)

    val rightPartitionKeyTypes = getPartitionKey(
      rightScanRDD.connector,
      rightScanRDD.keyspaceName,
      rightScanRDD.tableName)
      .map(_.columnType)

    assert( leftPartitionKeyTypes == rightPartitionKeyTypes,
      "Partition key types do not match between Right and Left RDDs in MergeJoin")
  }

  val TokenColumn = "mj_tok_col"
  def tokenExtractor (row: Row): Token = {
    row.getToken(TokenColumn)
  }

  private lazy val leftPartitionKeyStr =
    getPartitionKey(connector, leftScanRDD.keyspaceName, leftScanRDD.tableName)
      .map(_.columnName)
      .map(quote)
      .mkString(", ")

  private def tokenRangeToCqlQuery[T](
    fromRDD: CassandraTableScanRDD[T],
    range: CqlTokenRange[_, _]): (String, Seq[Any]) = {

    val columns = fromRDD.selectedColumnRefs.map(_.cql).mkString(", ")
    val pk = fromRDD.tableDef.partitionKey.map(colDef => quote(colDef.columnName)).mkString(",")
    val (cql, values) = range.cql(leftPartitionKeyStr)
    val filter = (cql +: fromRDD.where.predicates).filter(_.nonEmpty).mkString(" AND ")
    val limitClause = fromRDD.limit.map(limit => s"LIMIT $limit").getOrElse("")
    val orderBy = fromRDD.clusteringOrder.map(_.toCql(fromRDD.tableDef)).getOrElse("")
    val quotedKeyspaceName = quote(fromRDD.keyspaceName)
    val quotedTableName = quote(fromRDD.tableName)
    val queryTemplate =
      s"SELECT $columns, TOKEN($pk) as $TokenColumn " +
        s"FROM $quotedKeyspaceName.$quotedTableName " +
        s"WHERE $filter $orderBy $limitClause ALLOW FILTERING"
    val queryParamValues = values ++ fromRDD.where.values
    (queryTemplate, queryParamValues)
  }

  private def createStatement(
    session: CqlSession,
    readConf: ReadConf,
    cql: String,
    values: Any*): BoundStatement = {

    try {
      val stmt = session.prepare(cql)
      val converters = stmt.getVariableDefinitions.asScala
        .map(v => ColumnType.converterToCassandra(v.getType))
        .toArray
      val convertedValues =
        for ((value, converter) <- values zip converters)
          yield converter.convert(value)
      stmt.bind(convertedValues: _*)
        .setIdempotent(true)
        .setPageSize(readConf.fetchSizeInRows)
        .setConsistencyLevel(readConf.consistencyLevel)
    }
    catch {
      case t: Throwable =>
        throw new IOException(s"Exception during preparation of $cql: ${t.getMessage}", t)
    }
  }

  private def convertRowIterator[T](
      iterator: Iterator[Row],
      rowReader: RowReader[T],
      columnMetaData: CassandraRowMetadata): Iterator[T] = {

    iterator.map{ row =>
      rowReader.read(row, columnMetaData)
    }
  }

  private def fetchTokenRange[T](
    session: CqlSession,
    fromRDD: CassandraTableScanRDD[T],
    range: CqlTokenRange[_, _],
    inputMetricsUpdater: InputMetricsUpdater): (CassandraRowMetadata, Iterator[Row]) = {

    val (cql, values) = tokenRangeToCqlQuery[T](fromRDD, range)
    logDebug(
      s"Fetching data for range ${range} " +
        s"with $cql " +
        s"with params ${values.mkString("[", ",", "]")}")
    val stmt = createStatement(session, fromRDD.readConf, cql, values: _*)

    import com.datastax.spark.connector.util.Threads.BlockingIOExecutionContext

    val fetchResult = asScalaFuture(session.executeAsync(stmt)).map { rs =>
      val columnNames = fromRDD.selectedColumnRefs.map(_.selectedAs).toIndexedSeq ++ Seq(TokenColumn)
      val columnMetaData = CassandraRowMetadata.fromResultSet(columnNames, rs, session)
      val iterator = new PrefetchingResultSetIterator(rs)
      val iteratorWithMetrics = iterator.map(inputMetricsUpdater.updateMetrics)
      logDebug(s"Row iterator for range $range obtained successfully.")
      (columnMetaData, iteratorWithMetrics)
    }.recover {
      case t: Throwable => throw new IOException(s"Exception during execution of $cql: ${t.getMessage}", t)
    }
    Await.result(fetchResult, Duration.Inf)
  }

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[(Seq[L], Seq[R])] = {

    /** Open two sessions if Cluster Configurations are different **/
    def openSessions(): (CqlSession, CqlSession) = {
      if (leftScanRDD.connector == rightScanRDD.connector) {
        val session = leftScanRDD.connector.openSession()
        (session, session)
      } else {
        (leftScanRDD.connector.openSession, rightScanRDD.connector.openSession)
      }
    }

    def closeSessions(leftSession: CqlSession, rightSession : CqlSession): Unit = {
      if (leftSession != rightSession) rightSession.close()
      leftSession.close()
    }

    val (leftSession, rightSession) = openSessions()

    type V = t forSome { type t }
    type T = t forSome { type t <: com.datastax.spark.connector.rdd.partitioner.dht.Token[V] }
    val partition = split.asInstanceOf[CassandraPartition[V, T]]
    val tokenRanges = partition.tokenRanges

    val metricsReadConf = new ReadConf(taskMetricsEnabled =
      leftScanRDD.readConf.taskMetricsEnabled || rightScanRDD.readConf.taskMetricsEnabled)

    val metricsUpdater = InputMetricsUpdater(context, metricsReadConf)

    val mergingIterator = tokenRanges.iterator.flatMap { tokenRange =>
      val (leftMetadata, leftRowIterator) = fetchTokenRange(leftSession, leftScanRDD, tokenRange, metricsUpdater)
      val (rightMetadata, rightRowIterator) = fetchTokenRange(rightSession, rightScanRDD, tokenRange, metricsUpdater)

      val rowMerger = new MergeJoinIterator[Row, Row, Token](
        leftRowIterator,
        rightRowIterator,
        tokenExtractor,
        tokenExtractor
      )
      rowMerger.map { case (t: Token, lRows : Seq[Row], rRows: Seq[Row]) => (
        t,
        convertRowIterator(lRows.iterator, leftScanRDD.rowReader, leftMetadata).toList,
        convertRowIterator(rRows.iterator, rightScanRDD.rowReader, rightMetadata).toList)
      }
    }

    val countingIterator = new CountingIterator(mergingIterator)

    context.addTaskCompletionListener { (context) =>
      val duration = metricsUpdater.finish() / 1000000000d
      logDebug(
        f"""Fetched ${countingIterator.count} rows from
            |${leftScanRDD.keyspaceName} ${leftScanRDD.tableName} and ${rightScanRDD.tableName}
            |for partition ${partition.index} in $duration%.3f s.""".stripMargin)
      closeSessions(leftSession, rightSession)
      context
    }
    val iteratorWithoutToken = countingIterator.map(tuple => (tuple._2, tuple._3))
    iteratorWithoutToken
  }

  override protected def getPartitions: Array[Partition] = {
    checkValidMergeJoin()
    if (leftScanRDD.partitions.length >= rightScanRDD.partitions.length)
      leftScanRDD.partitions else rightScanRDD.partitions
  }

  override def getPreferredLocations(split: Partition): Seq[String] =
    split.asInstanceOf[CassandraPartition[_, _]].endpoints
}
