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

import com.datastax.bdp.util.ScalaJavaUtil._
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{BoundStatement, Row}
import com.datastax.spark.connector.util._

import scala.language.existentials
import scala.reflect.ClassTag
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.metrics.InputMetricsUpdater
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}
import com.datastax.oss.driver.api.core.metadata.Metadata
import com.datastax.oss.driver.api.core.metadata.token.Token
import com.datastax.spark.connector.CassandraRowMetadata
import com.datastax.spark.connector.cql.{CassandraConnector, ColumnDef, Schema}
import com.datastax.spark.connector.rdd.CassandraCoGroupedRDD._
import com.datastax.spark.connector.rdd.partitioner.{CassandraPartition, CqlTokenRange, NodeAddresses}
import com.datastax.spark.connector.rdd.reader.{PrefetchingResultSetIterator, RowReader}
import com.datastax.spark.connector.types.ColumnType
import com.datastax.spark.connector.util.Quote._
import com.datastax.spark.connector.util.{CountingIterator, MultiMergeJoinIterator, NameTools}

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._

/**
  * A RDD which pulls from provided separate CassandraTableScanRDDs which share partition keys type and
  * keyspaces. These tables will be joined on READ using a merge iterator. As long as we join
  * on the token of the partition key the two iterators should be read in order.
  * Note: this implementation do not restrict partition keys has the same names, but they should have the same types
  */
class CassandraCoGroupedRDD[T](
    sc: SparkContext,
    scanRDDs: Seq[CassandraTableScanRDD[T]])(
  implicit
    classTag: ClassTag[T])
  extends RDD[Seq[Seq[T]]] (sc, Seq.empty){

  lazy val connector = scanRDDs.head.connector

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

  protected def checkValidMergeJoin(): Unit = {
    assert(scanRDDs.size > 1, "Can not merge less then 2 tables")
    // equals is transitive so check head with each other is enough to verify all can be merged
    for (rdd <- scanRDDs.tail) {
      assert(hasEqualPartitionKeysTypes(scanRDDs.head, rdd),
        s"Partition key types do not match between the ${scanRDDs.head.tableName} and ${rdd.tableName} RDDs in CoGroup")
    }
  }

  def isValidMergeJoin(): Boolean = {
    scanRDDs.size > 1 && scanRDDs.tail.forall(
      hasEqualPartitionKeysTypes(scanRDDs.head, _))
  }

  private def hasEqualPartitionKeysTypes(leftScanRDD: CassandraTableScanRDD[T], rightScanRDD: CassandraTableScanRDD[T]): Boolean = {
    val leftPartitionKeyTypes: Seq[ColumnType[_]] = getPartitionKey(
      leftScanRDD.connector,
      leftScanRDD.keyspaceName,
      leftScanRDD.tableName)
      .map(_.columnType)

    val rightPartitionKeyTypes = getPartitionKey(
      rightScanRDD.connector,
      rightScanRDD.keyspaceName,
      rightScanRDD.tableName)
      .map(_.columnType)

    leftPartitionKeyTypes == rightPartitionKeyTypes
  }

  private def partitionKeyStr(rdd: CassandraTableScanRDD[_]) =
    getPartitionKey(connector, rdd.keyspaceName, rdd.tableName)
      .map(_.columnName)
      .map(quote)
      .mkString(", ")

  def tokenExtractor (row: Row): Token = {
    row.getToken(TokenColumn)
  }

  private def tokenRangeToCqlQuery[T](
    fromRDD: CassandraTableScanRDD[T],
    range: CqlTokenRange[_, _]): (String, Seq[Any]) = {
    val columns = fromRDD.selectedColumnRefs.map(_.cql).mkString(", ")
    val pk = fromRDD.tableDef.partitionKey.map(colDef => quote(colDef.columnName)).mkString(",")
    val (cql, values) = range.cql(partitionKeyStr(fromRDD))
    val filter = (cql +: fromRDD.where.predicates).filter(_.nonEmpty).mkString(" AND ")
    val limitClause = fromRDD.limit.map(limit => s"LIMIT $limit").getOrElse("")
    val orderBy = fromRDD.clusteringOrder.map(_.toCql(fromRDD.tableDef)).getOrElse("")
    val quotedKeyspaceName = quote(fromRDD.keyspaceName)
    val quotedTableName = quote(fromRDD.tableName)
    val queryTemplate =
      s"SELECT $columns, TOKEN($pk) as ${TokenColumn} " +
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
        .setConsistencyLevel(readConf.consistencyLevel)
        .setPageSize(readConf.fetchSizeInRows)
    }
    catch {
      case t: Throwable =>
        throw new IOException(s"Exception during preparation of $cql: ${t.getMessage}", t)
    }
  }

  private def convertRowSeq[T](
      seq: Seq[Row],
      rowReader: RowReader[T],
      columnMetaData: CassandraRowMetadata): Seq[T] = {
    seq.map(rowReader.read(_, columnMetaData))
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
      val columnMetaData = CassandraRowMetadata.fromResultSet(columnNames, rs, session.getContext.getCodecRegistry)
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
  override def compute(split: Partition, context: TaskContext): Iterator[Seq[Seq[T]]] = {
    /** Open two sessions if Cluster Configurations are different **/
    def openSession(rdd: CassandraTableScanRDD[T]): CqlSession = {
         if (connector == rdd.connector) {
           connector.openSession()
        } else {
          rdd.connector.openSession
        }
    }

    def closeSessions(sessions: Seq[CqlSession]): Unit = {
      for (s<-sessions) {
        if (!s.isClosed) s.close()
      }
    }

    val rddWithSessions: Seq[(CassandraTableScanRDD[T], CqlSession)] = scanRDDs.map (rdd => (rdd, openSession(rdd)))

    type V = t forSome { type t }
    type K = t forSome { type t <: com.datastax.spark.connector.rdd.partitioner.dht.Token[V] }
    val partition = split.asInstanceOf[CassandraPartition[V, K]]
    val tokenRanges = partition.tokenRanges

    val metricsReadConf = new ReadConf(taskMetricsEnabled = scanRDDs.exists(_.readConf.taskMetricsEnabled))

    val metricsUpdater = InputMetricsUpdater(context, metricsReadConf)

    val mergingIterator: Iterator[Seq[Seq[T]]] = tokenRanges.iterator.flatMap { tokenRange =>
      val rowsWithMeta =
        rddWithSessions.map { case (rdd, session) => fetchTokenRange(session, rdd, tokenRange, metricsUpdater) }

      val metaData = rowsWithMeta.map(_._1)
      val rows = rowsWithMeta.map(_._2)
      val rowMerger = new MultiMergeJoinIterator[Row, Token](
        rows,
        tokenExtractor
     )

     rowMerger.map ((allGroups: Seq[Seq[Row]]) => {
         allGroups.zip(metaData).zip(scanRDDs).map { case ((rows, meta), rdd) =>
           convertRowSeq(rows, rdd.rowReader, meta)
         }
       })
    }

    val countingIterator = new CountingIterator(mergingIterator)

    context.addTaskCompletionListener { (context) =>
      val duration = metricsUpdater.finish() / 1000000000d
      logDebug(
        f"""Fetched ${countingIterator.count} rows from
            |${scanRDDs.head.keyspaceName}
            |for partition ${partition.index} in $duration%.3f s.""".stripMargin)
      closeSessions(rddWithSessions.map(_._2))
      context
    }

    countingIterator
  }

  override protected def getPartitions: Array[Partition] = {
    checkValidMergeJoin()
    scanRDDs.maxBy(_.partitions.length).partitions
  }

  override def getPreferredLocations(split: Partition): Seq[String] =
    split.asInstanceOf[CassandraPartition[_, _]].endpoints

}

object CassandraCoGroupedRDD {
  val TokenColumn = "mj_tok_col"
}
