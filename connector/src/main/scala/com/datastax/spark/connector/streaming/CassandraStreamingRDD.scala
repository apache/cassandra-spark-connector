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

package com.datastax.spark.connector.streaming

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.rdd.reader._
import com.datastax.spark.connector.rdd._
import com.datastax.spark.connector.{AllColumns, ColumnSelector}
import org.apache.spark.streaming.StreamingContext

import scala.annotation.nowarn
import scala.reflect.ClassTag

/** RDD representing a Cassandra table for Spark Streaming.
  * @see [[com.datastax.spark.connector.rdd.CassandraTableScanRDD]]*/
@nowarn("cat=deprecation")
class CassandraStreamingRDD[R] private[connector] (
    sctx: StreamingContext,
    connector: CassandraConnector,
    keyspace: String,
    table: String,
    columns: ColumnSelector = AllColumns,
    where: CqlWhereClause = CqlWhereClause.empty,
    empty: Boolean = false,
    limit: Option[CassandraLimit] = None,
    clusteringOrder: Option[ClusteringOrder] = None,
    readConf: ReadConf = ReadConf())(
  implicit
    ct : ClassTag[R],
    @transient val rrf: RowReaderFactory[R])
  extends CassandraTableScanRDD[R](
    sc = sctx.sparkContext,
    connector = connector,
    keyspaceName = keyspace,
    tableName = table,
    columnNames = columns,
    where = where,
    limit = limit,
    clusteringOrder = clusteringOrder,
    readConf = readConf)
