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

import java.util.OptionalLong

import com.datastax.spark.connector.RowCountRef
import com.datastax.spark.connector.cql.{CassandraConnector, TableDef}
import com.datastax.spark.connector.datasource.ScanHelper.CqlQueryParts
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector.rdd.partitioner.dht.{Token, TokenFactory}
import com.datastax.spark.connector.rdd.partitioner.{CassandraPartition, DataSizeEstimates}
import com.datastax.spark.connector.util.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.metrics.InputMetricsUpdater
import org.apache.spark.TaskContext

case class CassandraScanPartitionReaderFactory(
  connector: CassandraConnector,
  tableDef: TableDef,
  schema: StructType,
  readConf: ReadConf,
  queryParts: CqlQueryParts) extends PartitionReaderFactory {

  def isCountQuery: Boolean = queryParts.selectedColumnRefs.contains(RowCountRef)

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {

    val cassandraPartition = partition.asInstanceOf[CassandraPartition[Any, _ <: Token[Any]]]
    if (isCountQuery) {
      //Count Pushdown
      CassandraCountPartitionReader(
        connector,
        tableDef,
        schema,
        readConf,
        queryParts,
        cassandraPartition)
    } else {
      CassandraScanPartitionReader(
        connector,
        tableDef,
        schema,
        readConf,
        queryParts,
        cassandraPartition)
    }
  }
}


abstract class CassandraPartitionReaderBase
  extends PartitionReader[InternalRow]
    with SupportsReportStatistics
    with Logging {

  protected val connector: CassandraConnector
  protected val tableDef: TableDef
  protected val schema: StructType
  protected val readConf: ReadConf
  protected val queryParts: CqlQueryParts
  protected val partition: CassandraPartition[Any, _ <: Token[Any]]

  protected val tokenRanges = partition.tokenRanges
  protected val rowIterator = getIterator()
  protected var lastRow: InternalRow = InternalRow()

  protected val metricsUpdater = InputMetricsUpdater(TaskContext.get(), readConf)

  override def next(): Boolean = {
    if (rowIterator.hasNext) {
      lastRow = rowIterator.next()
      true
    } else {
      false
    }
  }

  override def get(): InternalRow = lastRow

  override def close(): Unit = {
    metricsUpdater.finish()
    scanner.close()
  }

  protected val scanner = connector.connectionFactory.getScanner(readConf, connector.conf, columnNames)

  protected def columnNames = queryParts.selectedColumnRefs.map(_.selectedAs).toIndexedSeq

  //Currently Does not take into account TODO filters or pruning
  override def estimateStatistics(): Statistics = new Statistics {
    //We call the types from the void
    type V = t forSome {type t}
    type T = t forSome {type t <: Token[V]}

    override def sizeInBytes(): OptionalLong = {
      implicit val tokenFactory = TokenFactory.forSystemLocalPartitioner(connector)
      val sizeInBytes = new DataSizeEstimates[V, T](connector, tableDef.keyspaceName, tableDef.keyspaceName).dataSizeInBytes
      OptionalLong.of(sizeInBytes)
    }

    //We would need to do something much cleverer here to actually get something accurate, todo for later
    override def numRows(): OptionalLong = OptionalLong.empty()
  }

  override def readSchema(): StructType = schema

  /*
  Iterator flatMap trick flattens the iterator-of-iterator structure into a single iterator.
  flatMap on iterator is lazy, therefore a query for the next token range is executed not earlier
  than all of the rows returned by the previous query have been consumed.
  */
  protected def getIterator(): Iterator[InternalRow] = {
    tokenRanges.iterator.flatMap { range =>
      val scanResult = ScanHelper.fetchTokenRange(scanner, tableDef, queryParts, range, readConf.consistencyLevel, readConf.fetchSizeInRows)
      val meta = scanResult.metadata
      val iteratorWithMetrics = scanResult.rows.map(metricsUpdater.updateMetrics)
      iteratorWithMetrics.map(rowReader.read(_, meta))
    }
  }

  protected val rowReader = new UnsafeRowReaderFactory(schema).rowReader(tableDef, queryParts.selectedColumnRefs)
}

/**
  * Physical Scan Reader of Cassandra
  *
  * @param connector  Connection to Cassandra to use for Reading
  * @param tableDef   Table Definition Information for the table being scanned
  * @param schema     Output Schema to be produced from this read
  * @param readConf   Options relating to how the read should be performed
  * @param queryParts Additional query elements to add to the TokenRange Scan query
  * @param partition  The Token Range to Query with Localization Info
  */
case class CassandraScanPartitionReader(
  connector: CassandraConnector,
  tableDef: TableDef,
  schema: StructType,
  readConf: ReadConf,
  queryParts: CqlQueryParts,
  partition: CassandraPartition[Any, _ <: Token[Any]]) extends CassandraPartitionReaderBase

/**
  * Runs a COUNT(*) query instead of a request for actual rows
  * Takes the results and returns that many empty internal rows
  */
case class CassandraCountPartitionReader(
  connector: CassandraConnector,
  tableDef: TableDef,
  schema: StructType,
  readConf: ReadConf,
  queryParts: CqlQueryParts,
  partition: CassandraPartition[Any, _ <: Token[Any]]) extends CassandraPartitionReaderBase {

  //Our read is not based on the structure of the table we are reading from
  override val rowReader = new UnsafeRowReaderFactory(StructType(Seq(StructField("count", LongType, false))))
    .rowReader(tableDef, queryParts.selectedColumnRefs)

  /*
  Casting issue here for extremely large C* partitions,
  but it's unlikely that a Count Request will succeed if the
  Split has more than Int.Max Entries anyway.
  */
  override val rowIterator: Iterator[InternalRow] = {
    getIterator().flatMap(row => Iterator.fill(row.getLong(0).toInt)(InternalRow.empty))
  }
}

