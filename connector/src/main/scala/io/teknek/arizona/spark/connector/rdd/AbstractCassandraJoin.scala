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

package io.teknek.arizona.spark.connector.rdd

import java.util.concurrent.Future

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{PreparedStatement, Row, SimpleStatement}
import _root_.io.teknek.arizona.spark.connector._
import _root_.io.teknek.arizona.spark.connector.datasource.JoinHelper
import _root_.io.teknek.arizona.spark.connector.datasource.ScanHelper.CqlQueryParts
import _root_.io.teknek.arizona.spark.connector.util.{CountingIterator}
import _root_.io.teknek.arizona.spark.connector.writer._
import _root_.org.apache.spark.metrics.InputMetricsUpdater
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}

import scala.jdk.CollectionConverters._
/**
 * This trait contains shared methods from [[io.teknek.arizona.spark.connector.rdd.CassandraJoinRDD]] and
 * [[io.teknek.arizona.spark.connector.rdd.CassandraLeftJoinRDD]] to avoid code duplication.
 *
 * @tparam L item type on the left side of the join (any RDD)
 * @tparam R item type on the right side of the join (fetched from Cassandra)
 */
private[rdd] trait AbstractCassandraJoin[L, R] {

  self: CassandraRDD[(L, R)] with CassandraTableRowReaderProvider[_] =>

  val left: RDD[L]
  val joinColumns: ColumnSelector
  val manualRowWriter: Option[RowWriter[L]]
  implicit val rowWriterFactory: RowWriterFactory[L]

  private[rdd] def fetchIterator(
    session: CqlSession,
    bsb: BoundStatementBuilder[L],
    rowMetadata: CassandraRowMetadata,
    lastIt: Iterator[L],
    metricsUpdater: InputMetricsUpdater
  ): Iterator[(L, R)]

  val requestsPerSecondRateLimiter = JoinHelper.requestsPerSecondRateLimiter(readConf)
  val maybeRateLimit: (Row => Row) = JoinHelper.maybeRateLimit(readConf)

  lazy val joinColumnNames = JoinHelper.joinColumnNames(joinColumns, tableDef)

  lazy val rowWriter = manualRowWriter match {
    case Some(_rowWriter) => _rowWriter
    case None => implicitly[RowWriterFactory[L]].rowWriter(tableDef, joinColumnNames.toIndexedSeq)
  }

  /**
   * This method will create the RowWriter required before the RDD is serialized.
   * This is called during getPartitions
   */
  protected def checkValidJoin(): Seq[ColumnRef] = {
    val partitionKeyColumnNames = tableDef.partitionKey.map(_.columnName).toSet
    val primaryKeyColumnNames = tableDef.primaryKey.map(_.columnName).toSet
    val colNames = joinColumnNames.map(_.columnName).toSet

    // Initialize RowWriter and Query to be used for accessing Cassandra
    rowWriter.columnNames

    def checkSingleColumn(column: ColumnRef): Unit = {
      require(
        primaryKeyColumnNames.contains(column.columnName),
        s"Can't pushdown join on column $column because it is not part of the PRIMARY KEY"
      )
    }

    // Make sure we have all of the clustering indexes between the 0th position and the max requested
    // in the join:
    val chosenClusteringColumns = tableDef.clusteringColumns
      .filter(cc => colNames.contains(cc.columnName))
    if (!tableDef.clusteringColumns.startsWith(chosenClusteringColumns)) {
      val maxCol = chosenClusteringColumns.last
      val maxIndex = maxCol.componentIndex.get
      val requiredColumns = tableDef.clusteringColumns.takeWhile(_.componentIndex.get <= maxIndex)
      val missingColumns = requiredColumns.toSet -- chosenClusteringColumns.toSet
      throw new IllegalArgumentException(
        s"Can't pushdown join on column $maxCol without also specifying [ $missingColumns ]"
      )
    }
    val missingPartitionKeys = partitionKeyColumnNames -- colNames
    require(
      missingPartitionKeys.isEmpty,
      s"Can't join without the full partition key. Missing: [ $missingPartitionKeys ]"
    )

    //Run To check for conflicting where clauses
    JoinHelper.getJoinQueryString(tableDef, joinColumnNames, CqlQueryParts(selectedColumnRefs, where, limit, clusteringOrder))

    joinColumnNames.foreach(checkSingleColumn)
    joinColumnNames
  }

  /**
   * When computing a CassandraPartitionKeyRDD the data is selected via single CQL statements
   * from the specified C* Keyspace and Table. This will be preformed on whatever data is
   * available in the previous RDD in the chain.
   */
  override def compute(split: Partition, context: TaskContext): Iterator[(L, R)] = {
    val session = connector.openSession()
    val stmt = JoinHelper.getJoinQueryString(tableDef, joinColumnNames, CqlQueryParts(selectedColumnRefs, where, limit, clusteringOrder))
    val preparedStatement = JoinHelper.getJoinPreparedStatement(session, stmt, consistencyLevel)
    val bsb = JoinHelper.getKeyBuilderStatementBuilder(session, rowWriter, preparedStatement, where)
    val rowMetadata = JoinHelper.getCassandraRowMetadata(session,preparedStatement, selectedColumnRefs)
    val metricsUpdater = InputMetricsUpdater(context, readConf)
    val rowIterator = fetchIterator(session, bsb, rowMetadata, left.iterator(split, context), metricsUpdater)
    val countingIterator = new CountingIterator(rowIterator, None)

    context.addTaskCompletionListener { (context) =>
      val duration = metricsUpdater.finish() / 1000000000d
      logDebug(
        f"Fetched ${countingIterator.count} rows " +
          f"from $keyspaceName.$tableName " +
          f"for partition ${split.index} in $duration%.3f s."
      )
      session.close()
      context
    }
    countingIterator
  }

  override protected def getPartitions: Array[Partition] = {
    verify()
    checkValidJoin()
    left.partitions
  }

  override def getPreferredLocations(split: Partition): Seq[String] = left.preferredLocations(split)

  override def toEmptyCassandraRDD: EmptyCassandraRDD[(L, R)] =
    new EmptyCassandraRDD[(L, R)](
      sc = left.sparkContext,
      keyspaceName = keyspaceName,
      tableName = tableName,
      columnNames = columnNames,
      where = where,
      limit = limit,
      clusteringOrder = clusteringOrder,
      readConf = readConf
    )


}

