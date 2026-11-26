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

import com.datastax.oss.driver.api.core.ConsistencyLevel
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.{CassandraConnector, TableDef}
import com.datastax.spark.connector.datasource.ScanHelper
import com.datastax.spark.connector.rdd.reader._
import com.datastax.spark.connector.util.tableFromCassandra

import scala.reflect.ClassTag

/**
 * Used to get a RowReader of type [R] for transforming the rows of a particular Cassandra table into
 * scala objects. Performs necessary checking of the schema and output class to make sure they are
 * compatible.
 * @see [[CassandraTableScanRDD]]
 * @see [[CassandraJoinRDD]]
 */
trait CassandraTableRowReaderProvider[R] {

  protected def connector: CassandraConnector

  protected def keyspaceName: String

  protected def tableName: String

  protected def columnNames: ColumnSelector

  protected def readConf: ReadConf

  protected def splitCount: Option[Int] = readConf.splitCount

  protected[connector] def splitSize: Long = readConf.splitSizeInMB * 1024L * 1024L

  protected def fetchSize: Int = readConf.fetchSizeInRows

  protected def consistencyLevel: ConsistencyLevel = readConf.consistencyLevel

  /** RowReaderFactory and ClassTag should be provided from implicit parameters in the constructor
    * of the class implementing this trait
    * @see CassandraTableScanRDD */
  protected val rowReaderFactory: RowReaderFactory[R]

  protected val classTag: ClassTag[R]

  lazy val rowReader: RowReader[R] =
    rowReaderFactory.rowReader(tableDef, columnNames.selectFrom(tableDef))

  lazy val tableDef: TableDef = tableFromCassandra(connector, keyspaceName, tableName)

  /** Returns the columns to be selected from the table.*/
  lazy val selectedColumnRefs: IndexedSeq[ColumnRef] = {
    val providedColumns =
      columnNames match {
        case AllColumns => tableDef.columns.map(col => col.columnName: ColumnRef)
        case PrimaryKeyColumns => tableDef.primaryKey.map(col => col.columnName: ColumnRef)
        case PartitionKeyColumns => tableDef.partitionKey.map(col => col.columnName: ColumnRef)
        case SomeColumns(cs@_*) => ScanHelper.checkColumnsExistence(cs, tableDef)
      }

    // Let's leave only the columns needed by the rowReader.
    // E.g. even if the user selects AllColumns,
    // this will make sure only the columns needed by the RowReader are actually fetched.
    rowReader.neededColumns match {
      case Some(neededColumns) => providedColumns.filter(neededColumns.toSet).toIndexedSeq
      case None => providedColumns.toIndexedSeq
    }
  }

  /** Filters currently selected set of columns with a new set of columns */
  def narrowColumnSelection(columns: Seq[ColumnRef]): Seq[ColumnRef] = {
    columnNames match {
      case SomeColumns(cs@_*) =>
        checkColumnsAvailable(columns, cs)
      case AllColumns =>
      case PartitionKeyColumns | PrimaryKeyColumns =>
      // we do not check for column existence yet as it would require fetching schema and a call to C*
      // columns existence will be checked by C* once the RDD gets computed.
    }
    columns
  }

  /** Throws IllegalArgumentException if columns sequence contains unavailable columns */
  private def checkColumnsAvailable(
      columns: Seq[ColumnRef],
      availableColumns: Seq[ColumnRef]): Unit = {

    val availableColumnsSet = availableColumns.collect {
      case ColumnName(columnName, _) => columnName
    }.toSet

    val notFound = columns.collectFirst {
      case ColumnName(columnName, _) if !availableColumnsSet.contains(columnName) => columnName
    }

    if (notFound.isDefined)
      throw new IllegalArgumentException(
        s"Column not found in selection: ${notFound.get}. " +
          s"Available columns: [${availableColumns.mkString(",")}].")
  }


  protected lazy val cassandraPartitionerClassName =
    connector.withSessionDo {
      session =>
        session.execute("SELECT partitioner FROM system.local").one().getString(0)
    }

  /** Checks for existence of keyspace and table.*/
  def verify() = {
    tableDef.columns // will throw IOException if table does not exist
    rowReader // will instantiate rowReader which must validate existence of columns
  }

}
