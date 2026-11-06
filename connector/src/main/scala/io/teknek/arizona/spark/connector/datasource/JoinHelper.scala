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

package io.teknek.arizona.spark.connector.datasource

import java.util.concurrent.Future

import com.datastax.oss.driver.api.core.cql.{PreparedStatement, Row, SimpleStatement}
import com.datastax.oss.driver.api.core.{ConsistencyLevel, CqlIdentifier, CqlSession}
import _root_.io.teknek.arizona.spark.connector.cql.{TableDef, getRowBinarySize}
import _root_.io.teknek.arizona.spark.connector.datasource.ScanHelper.CqlQueryParts
import _root_.io.teknek.arizona.spark.connector.rdd.{CassandraLimit, CqlWhereClause, ReadConf}
import _root_.io.teknek.arizona.spark.connector.util.CqlWhereParser.{EqPredicate, InListPredicate, InPredicate, RangePredicate}
import _root_.io.teknek.arizona.spark.connector.util.{CqlWhereParser, Logging}
import _root_.io.teknek.arizona.spark.connector.writer.{BoundStatementBuilder, RateLimiter, RowWriter}
import _root_.io.teknek.arizona.spark.connector.{AllColumns, CassandraRowMetadata, ColumnRef, ColumnSelector, PartitionKeyColumns, PrimaryKeyColumns, SomeColumns}

object JoinHelper extends Logging {

  def joinColumnNames(joinColumns: ColumnSelector, tableDef: TableDef): Seq[ColumnRef] = joinColumns match {
    case AllColumns => throw new IllegalArgumentException(
      "Unable to join against all columns in a Cassandra Table. Only primary key columns allowed."
    )
    case PrimaryKeyColumns =>
      tableDef.primaryKey.map(col => col.columnName: ColumnRef)
    case PartitionKeyColumns =>
      tableDef.partitionKey.map(col => col.columnName: ColumnRef)
    case SomeColumns(cs@_*) =>
      ScanHelper.checkColumnsExistence(cs, tableDef)
      cs.map {
        case c: ColumnRef => c
        case _ => throw new IllegalArgumentException(
          "Unable to join against unnamed columns. No CQL Functions allowed."
        )
      }
  }

  def getJoinQueryString(
    tableDef: TableDef,
    joinColumns: Seq[ColumnRef],
    queryParts: CqlQueryParts) = {

    val whereClauses = queryParts.whereClause.predicates.flatMap(CqlWhereParser.parse)
    val joinColumnNames = joinColumns.map(_.columnName)

    val joinColumnPredicates = whereClauses.collect {
      case EqPredicate(c, _) if joinColumnNames.contains(c) => c
      case InPredicate(c) if joinColumnNames.contains(c) => c
      case InListPredicate(c, _) if joinColumnNames.contains(c) => c
      case RangePredicate(c, _, _) if joinColumnNames.contains(c) => c
    }.toSet

    require(
      joinColumnPredicates.isEmpty,
      s"""Columns specified in both the join on clause and the where clause.
         |Partition key columns are always part of the join clause.
         |Columns in both: ${joinColumnPredicates.mkString(", ")}""".stripMargin
    )


    logDebug("Generating Single Key Query Prepared Statement String")
    logDebug(s"SelectedColumns : ${queryParts.selectedColumnRefs} -- JoinColumnNames : $joinColumnNames")
    val columns = queryParts.selectedColumnRefs.map(_.cql).mkString(", ")
    val joinWhere = joinColumnNames.map(name => s"${CqlIdentifier.fromInternal(name).asCql(true)} = :$name")
    val limitClause = CassandraLimit.limitToClause(queryParts.limitClause)
    val orderBy = queryParts.clusteringOrder.map(_.toCql(tableDef)).getOrElse("")
    val filter = (queryParts.whereClause.predicates ++ joinWhere).mkString(" AND ")
    val quotedKeyspaceName = CqlIdentifier.fromInternal(tableDef.keyspaceName).asCql(true)
    val quotedTableName = CqlIdentifier.fromInternal(tableDef.tableName).asCql(true)
    val query =
      s"SELECT $columns " +
        s"FROM $quotedKeyspaceName.$quotedTableName " +
        s"WHERE $filter $orderBy $limitClause"
    logDebug(s"Query : $query")
    query
  }

  def getJoinPreparedStatement(
    session: CqlSession,
    queryString: String,
    consistencyLevel: ConsistencyLevel): PreparedStatement = {

    val stmt = SimpleStatement.newInstance(queryString).setConsistencyLevel(consistencyLevel).setIdempotent(true)
    session.prepare(stmt)
  }

  def getCassandraRowMetadata(
    session: CqlSession,
    statement: PreparedStatement,
    selectedColumnRefs: IndexedSeq[ColumnRef]): CassandraRowMetadata = {

    val codecRegistry = session.getContext.getCodecRegistry
    val columnNames = selectedColumnRefs.map(_.selectedAs).toIndexedSeq
    CassandraRowMetadata.fromPreparedStatement(columnNames, statement, codecRegistry)
  }

  def getKeyBuilderStatementBuilder[L](
    session: CqlSession,
    rowWriter: RowWriter[L],
    preparedStatement: PreparedStatement,
    whereClause: CqlWhereClause): BoundStatementBuilder[L] = {

    val protocolVersion = session.getContext.getProtocolVersion
    new BoundStatementBuilder[L](rowWriter, preparedStatement, whereClause.values, protocolVersion = protocolVersion)
  }

  /** Prefetches a batchSize of elements at a time **/
  def slidingPrefetchIterator[T](it: Iterator[Future[T]], batchSize: Int): Iterator[T] = {
    val (firstElements, lastElement) = it
      .grouped(batchSize)
      .sliding(2)
      .span(_ => it.hasNext)

    (firstElements.map(_.head) ++ lastElement.flatten).flatten.map(_.get)
  }

  def requestsPerSecondRateLimiter(readConf: ReadConf) = new RateLimiter(
    readConf.readsPerSec.getOrElse(Integer.MAX_VALUE).toLong,
    readConf.readsPerSec.getOrElse(Integer.MAX_VALUE).toLong
  )

  def maybeRateLimit(readConf: ReadConf): (Row => Row) = readConf.throughputMiBPS match {
    case Some(throughput) =>
      val bytesPerSecond: Long = (throughput * 1024 * 1024).toLong
      val rateLimiter = new RateLimiter(bytesPerSecond, bytesPerSecond)
      logDebug(s"Throttling join at $bytesPerSecond bytes per second")
      (row: Row) => {
        rateLimiter.maybeSleep(getRowBinarySize(row))
        row
      }
    case None => identity[Row]
  }


}
