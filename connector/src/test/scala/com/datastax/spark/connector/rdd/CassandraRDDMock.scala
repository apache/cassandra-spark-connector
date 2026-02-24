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

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.{ColumnRef, ColumnSelector, SomeColumns}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}

import scala.reflect.ClassTag

/**
  * A Fake CassandraRDD for mocking CassandraRDDs. Instead of reading data from cassandra this
  * call uses an RDD prev as a source for data. In essenece this is a passthrough RDD which
  * takes the contents of the previous RDD and pretends it is a CassandraRDD.
  */
class CassandraRDDMock[R : ClassTag](prev: RDD[R], keyspace: String = "fake", table: String = "fake") extends
  CassandraRDD[R](prev.sparkContext, prev.dependencies){

  /** This is slightly different than Scala this.type.
    * this.type is the unique singleton type of an object which is not compatible with other
    * instances of the same type, so returning anything other than `this` is not really possible
    * without lying to the compiler by explicit casts.
    * Here SelfType is used to return a copy of the object - a different instance of the same type */
  override type Self = this.type

  override protected[connector] def keyspaceName: String = keyspace
  override protected[connector] def tableName: String = table
  override protected def columnNames: ColumnSelector = SomeColumns()
  override protected def where: CqlWhereClause = CqlWhereClause(Seq.empty, Seq.empty)
  override protected def readConf: ReadConf = ReadConf()
  override protected def limit: Option[CassandraLimit] = None
  override protected def clusteringOrder: Option[ClusteringOrder] = None
  override protected def connector: CassandraConnector = ???
  override val selectedColumnRefs: Seq[ColumnRef] = Seq.empty
  override protected def narrowColumnSelection(columns: Seq[ColumnRef]): Seq[ColumnRef] = Seq.empty

  override def toEmptyCassandraRDD: EmptyCassandraRDD[R] = new EmptyCassandraRDD[R](prev.sparkContext, keyspace, table)

  /** Counts the number of items in this RDD by selecting count(*) on Cassandra table */
  override def cassandraCount(): Long = prev.count()

  /** Doesn't actually copy since we don't really use any of these parameters **/
  override protected def copy(
     columnNames: ColumnSelector,
     where: CqlWhereClause,
     limit: Option[CassandraLimit],
     clusteringOrder: Option[ClusteringOrder],
     readConf: ReadConf,
     connector: CassandraConnector) : CassandraRDDMock.this.type = this

  /*** Pass through the parent RDD's Compute and partitions ***/
  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[R] = prev.compute(split, context)
  override protected def getPartitions: Array[Partition] = prev.partitions
}
