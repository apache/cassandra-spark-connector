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

import com.datastax.spark.connector.cql.{CassandraConnector, TableDef}
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector.rdd.reader.PrefetchingResultSetIterator
import com.datastax.spark.connector.util.Logging
import com.datastax.spark.connector.util.Threads.BlockingIOExecutionContext
import com.datastax.spark.connector.writer.{CassandraRowWriter, QueryExecutor}
import com.datastax.spark.connector.{CassandraRow, CassandraRowMetadata, ColumnName, RowCountRef}
import com.google.common.util.concurrent.SettableFuture
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.sources.In
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.metrics.InputMetricsUpdater
import org.apache.spark.TaskContext

import scala.util.{Failure, Success}


case class CassandraInJoinReaderFactory(
  connector: CassandraConnector,
  tableDef: TableDef,
  inClauses: Seq[In],
  readConf: ReadConf,
  schema: StructType,
  cqlQueryParts: ScanHelper.CqlQueryParts)
  extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    if (cqlQueryParts.selectedColumnRefs.contains(RowCountRef)) {
      CassandraInJoinCountReader(connector, tableDef, inClauses, readConf, schema, cqlQueryParts, partition)
    } else {
      CassandraInJoinReader(connector, tableDef, inClauses, readConf, schema, cqlQueryParts, partition)
    }
}


abstract class CassandraBaseInJoinReader(
  connector: CassandraConnector,
  tableDef: TableDef,
  inClauses: Seq[In],
  readConf: ReadConf,
  schema: StructType,
  cqlQueryParts: ScanHelper.CqlQueryParts,
  partition: InputPartition)
  extends PartitionReader[InternalRow]
    with Logging {

  protected val numberedInputPartition = partition.asInstanceOf[NumberedInputPartition]
  protected val joinColumnNames = inClauses.map(in => ColumnName(in.attribute)).toIndexedSeq
  protected val session = connector.openSession()
  protected val rowWriter = CassandraRowWriter.Factory.rowWriter(tableDef, joinColumnNames)
  protected val rowReader = new UnsafeRowReaderFactory(schema).rowReader(tableDef, cqlQueryParts.selectedColumnRefs)

  protected val keyIterator: Iterator[CassandraRow] = InClauseKeyGenerator.getIterator(numberedInputPartition.index, numberedInputPartition.total, inClauses) //Generate Iterators for this partition here

  protected val stmt = JoinHelper.getJoinQueryString(tableDef, joinColumnNames, cqlQueryParts)
  protected val preparedStatement = JoinHelper.getJoinPreparedStatement(session, stmt, readConf.consistencyLevel)
  protected val bsb = JoinHelper.getKeyBuilderStatementBuilder(session, rowWriter, preparedStatement, cqlQueryParts.whereClause)
  protected val rowMetadata = JoinHelper.getCassandraRowMetadata(session, preparedStatement, cqlQueryParts.selectedColumnRefs)

  protected val queryExecutor = QueryExecutor(session, readConf.parallelismLevel, None, None)
  protected val maybeRateLimit = JoinHelper.maybeRateLimit(readConf)
  protected val requestsPerSecondRateLimiter = JoinHelper.requestsPerSecondRateLimiter(readConf)

  protected val metricsUpdater = InputMetricsUpdater(TaskContext.get(), readConf)
  protected def pairWithRight(left: CassandraRow): SettableFuture[Iterator[(CassandraRow, InternalRow)]] = {
    val resultFuture = SettableFuture.create[Iterator[(CassandraRow, InternalRow)]]
    val leftSide = Iterator.continually(left)

    queryExecutor.executeAsync(bsb.bind(left).executeAs(readConf.executeAs)).onComplete {
      case Success(rs) =>
        val resultSet = new PrefetchingResultSetIterator(rs)
        val iteratorWithMetrics = resultSet.map(metricsUpdater.updateMetrics)
        /* This is a much less than ideal place to actually rate limit, we are buffering
        these futures this means we will most likely exceed our threshold*/
        val throttledIterator = iteratorWithMetrics.map(maybeRateLimit)
        val rightSide = throttledIterator.map(rowReader.read(_, rowMetadata))
        resultFuture.set(leftSide.zip(rightSide))
      case Failure(throwable) =>
        resultFuture.setException(throwable)
    }
    resultFuture
  }

  protected val queryFutures = keyIterator.map(left => {
    requestsPerSecondRateLimiter.maybeSleep(1)
    pairWithRight(left)
  })

  protected def getIterator() = JoinHelper.slidingPrefetchIterator(queryFutures, readConf.parallelismLevel).flatten.map(_._2)

  protected val rowIterator = getIterator()

  protected var currentRow: InternalRow = _

  override def next(): Boolean = {
    if (rowIterator.hasNext) {
      currentRow = rowIterator.next()
      true
    } else {
      false
    }
  }

  override def get(): InternalRow = currentRow

  override def close(): Unit = {
    metricsUpdater.finish()
    session.close()
  }
}

case class CassandraInJoinReader(
  connector: CassandraConnector,
  tableDef: TableDef,
  inClauses: Seq[In],
  readConf: ReadConf,
  schema: StructType,
  cqlQueryParts: ScanHelper.CqlQueryParts,
  partition: InputPartition)
  extends CassandraBaseInJoinReader(connector, tableDef, inClauses, readConf, schema, cqlQueryParts, partition)

case class CassandraInJoinCountReader(
  connector: CassandraConnector,
  tableDef: TableDef,
  inClauses: Seq[In],
  readConf: ReadConf,
  schema: StructType,
  cqlQueryParts: ScanHelper.CqlQueryParts,
  partition: InputPartition)
  extends CassandraBaseInJoinReader(connector, tableDef, inClauses, readConf, schema, cqlQueryParts, partition) {

  //Our read is not based on the structure of the table we are reading from
  override val rowReader =
    new UnsafeRowReaderFactory(StructType(Seq(StructField("count", LongType, false))))
      .rowReader(tableDef, cqlQueryParts.selectedColumnRefs)

  /*
  Casting issue here for extremely large C* partitions,
  but it's unlikely that a Count Request will succeed if the
  partition has more than Int.Max Entries anyway.
  */
  override val rowIterator: Iterator[InternalRow] = {
    getIterator().flatMap(row => Iterator.fill(row.getLong(0).toInt)(InternalRow.empty))
  }
}

object InClauseKeyGenerator {
  def getIterator(index: Int, totalPartitions: Int, inClauses: Seq[In]): Iterator[CassandraRow] = {
    val values = cross(inClauses.map(_.values.toStream)) //We need to enumerate our cross product lazily
    val columns = inClauses.map(_.attribute)
    val rowMetadata = CassandraRowMetadata.fromColumnNames(columns.toIndexedSeq)
    values
      .map(_.toIndexedSeq.asInstanceOf[IndexedSeq[AnyRef]])
      .zipWithIndex
      .filter { case (_, dataIndex) => dataIndex % totalPartitions == index }
      .map { case (data, _) => new CassandraRow(rowMetadata, data) }
      .toIterator
  }

  def cross(iter: Iterable[Iterable[_]]): Iterable[List[_]] = {
    iter.headOption match {
      case Some(left) => for {
        leftElement <- left
        rightElement <- cross(iter.tail)
      } yield leftElement :: rightElement
      case None => Iterable(Nil)
    }
  }
}
