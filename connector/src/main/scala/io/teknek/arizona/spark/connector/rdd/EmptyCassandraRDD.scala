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

import _root_.io.teknek.arizona.spark.connector._
import _root_.io.teknek.arizona.spark.connector.cql.CassandraConnector
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag

/** Represents a CassandraRDD with no rows.
  * This RDD does not load any data from Cassandra and doesn't require for the table to exist. */
class EmptyCassandraRDD[R : ClassTag](
    @transient val sc: SparkContext,
    val keyspaceName: String,
    val tableName: String,
    val columnNames: ColumnSelector = AllColumns,
    val where: CqlWhereClause = CqlWhereClause.empty,
    val limit: Option[CassandraLimit] = None,
    val clusteringOrder: Option[ClusteringOrder] = None,
    val readConf: ReadConf = ReadConf())
  extends CassandraRDD[R](sc, Seq.empty) {

  override type Self = EmptyCassandraRDD[R]

  override protected def copy(
      columnNames: ColumnSelector = columnNames,
      where: CqlWhereClause = where,
      limit: Option[CassandraLimit] = limit,
      clusteringOrder: Option[ClusteringOrder] = None,
      readConf: ReadConf = readConf,
      connector: CassandraConnector = connector) = {

    new EmptyCassandraRDD[R](
      sc = sc,
      keyspaceName = keyspaceName,
      tableName = tableName,
      columnNames = columnNames,
      where = where,
      limit = limit,
      clusteringOrder = clusteringOrder,
      readConf = readConf)
  }

  override val cassandraCount: Long = 0L

  override protected def getPartitions: Array[Partition] = Array.empty

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[R] =
    throw new UnsupportedOperationException("Cannot call compute on an EmptyRDD")

  lazy val selectedColumnRefs: Seq[ColumnRef] = {
    val providedColumnNames =
      columnNames match {
        case AllColumns => Seq()
        case PrimaryKeyColumns => Seq()
        case PartitionKeyColumns => Seq()
        case SomeColumns(cs@_*) => cs
      }
    providedColumnNames
  }

  override protected def connector: CassandraConnector =
    throw new UnsupportedOperationException("Empty Cassandra RDD don't have connections to cassandra")

  override def toEmptyCassandraRDD: EmptyCassandraRDD[R] = copy()

  override protected def narrowColumnSelection(
      columns: Seq[ColumnRef]): Seq[ColumnRef] = columns
}
