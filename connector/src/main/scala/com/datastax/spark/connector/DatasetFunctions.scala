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

package com.datastax.spark.connector

import com.datastax.oss.driver.api.core.ProtocolVersion
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.mapper.DataFrameColumnMapper
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, Encoder}
import org.apache.spark.sql.cassandra.{AlwaysOn, CassandraSourceRelation, DirectJoinSetting}

/** Provides Cassandra-specific methods on [[org.apache.spark.sql.DataFrame]] */
class DatasetFunctions[K: Encoder](dataset: Dataset[K]) extends Serializable {

  val sparkContext: SparkContext = dataset.sparkSession.sparkContext

  def directJoin(directJoinSetting: DirectJoinSetting= AlwaysOn): Dataset[K] = {
    CassandraSourceRelation.setDirectJoin(dataset, directJoinSetting)
  }

  /**
   *  Creates a C* table based on the Dataset Struct provided. Optionally
   *  takes in a list of partition columns or clustering columns names. When absent
   *  the first column will be used as the partition key and there will be no clustering
   *  keys.
   */
  @deprecated("Use DatasourceV2 Catalog Api", "3.0.0")
  def createCassandraTable(
    keyspaceName: String,
    tableName: String,
    partitionKeyColumns: Option[Seq[String]] = None,
    clusteringKeyColumns: Option[Seq[String]] = None)(
  implicit
    connector: CassandraConnector = CassandraConnector(sparkContext)): Unit = {

    val protocolVersion = connector.withSessionDo(_.getContext.getProtocolVersion)
    val rawTable = new DataFrameColumnMapper(dataset.schema).newTable(keyspaceName, tableName, protocolVersion)
    val partitionKeyNames = partitionKeyColumns.getOrElse(rawTable.partitionKey.map(_.columnName))
    val clusteringKeyNames = clusteringKeyColumns.getOrElse(Seq.empty)

    createCassandraTableEx(keyspaceName, tableName, partitionKeyNames,
      clusteringKeyNames.map((_, ClusteringColumn.Ascending)))(connector)
  }

    /**
    *  Creates a C* table based on the Dataset Struct provided.
    *  Takes in a list of partition columns, clustering columns names, and optionally, the table options.
    */
  @deprecated("Use DatasourceV2 Catalog Api", "3.0.0")
  def createCassandraTableEx(
    keyspaceName: String,
    tableName: String,
    partitionKeyColumns: Seq[String],
    clusteringKeyColumns: Seq[(String, ClusteringColumn.SortingOrder)],
    ifNotExists: Boolean = false,
    tableOptions: Map[String, String] = Map())(
  implicit
    connector: CassandraConnector = CassandraConnector(sparkContext)): Unit = {

    val protocolVersion = connector.withSessionDo(_.getContext.getProtocolVersion)

    val rawTable = new DataFrameColumnMapper(dataset.schema).newTable(keyspaceName, tableName, protocolVersion)
    val columnMapping = rawTable.columnByName

    val columnNames = columnMapping.keys.toSet
    val partitionKeyNames = partitionKeyColumns
    val clusteringKeyNames = clusteringKeyColumns.map(_._1)
    val regularColumnNames = (columnNames -- (partitionKeyNames ++ clusteringKeyNames)).toSeq

    def missingColumnException(columnName: String, columnType: String) = {
      new IllegalArgumentException(
        s""""$columnName" not Found. Unable to make specified column $columnName a $columnType.
          |Available Columns: $columnNames""".stripMargin)
    }

    val table = rawTable.copy (
      partitionKey = partitionKeyNames
          .map(partitionKeyName =>
            columnMapping.getOrElse(partitionKeyName,
              throw missingColumnException(partitionKeyName, "Partition Key Column")))
          .map(_.copy(columnRole = PartitionKeyColumn))
      ,
      clusteringColumns = clusteringKeyColumns
          .map(clusteringKey =>
            (columnMapping.getOrElse(clusteringKey._1,
              throw missingColumnException(clusteringKey._1, "Clustering Column")),
              clusteringKey._2))
          .zipWithIndex
          .map { case (col, index) => col._1.copy(columnRole = ClusteringColumn(index, col._2))}
      ,
      regularColumns = regularColumnNames
          .map(regularColumnName =>
             columnMapping.getOrElse(regularColumnName,
                  throw missingColumnException(regularColumnName, "Regular Column")))
          .map(_.copy(columnRole = RegularColumn))
      ,
      ifNotExists = ifNotExists
      ,
      tableOptions = tableOptions
    )

    connector.withSessionDo(session => session.execute(table.cql))
  }

}
