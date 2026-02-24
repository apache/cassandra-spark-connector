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

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.{CassandraConnector, CassandraConnectorConf}
import com.datastax.spark.connector.rdd.partitioner.{CassandraPartitionedRDD, ReplicaPartitioner}
import com.datastax.spark.connector.rdd.ValidRDDType
import com.datastax.spark.connector.rdd.reader.RowReaderFactory
import com.datastax.spark.connector.util.Logging
import com.datastax.spark.connector.writer._
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

class DStreamFunctions[T](dstream: DStream[T])
  extends WritableToCassandra[T]
  with Serializable
  with Logging {

  def warnIfKeepAliveIsShort(): Unit = {
    val keepAliveDuration: Duration = Duration(conf.getInt(
      CassandraConnectorConf.KeepAliveMillisParam.name,
      CassandraConnectorConf.KeepAliveMillisParam.default))

    val streamDuration: Duration = dstream.slideDuration

    if (keepAliveDuration < streamDuration)
      logWarning( s"""Currently ${CassandraConnectorConf.KeepAliveMillisParam.name}  is set to
        |${keepAliveDuration}, which is less than ${streamDuration}. This will cause connections to
        | be closed and recreated between batches. If this is not what you intended, increase the value
        | of ${CassandraConnectorConf.KeepAliveMillisParam.name} to a larger value.""".stripMargin)
  }

  override def sparkContext: SparkContext = dstream.context.sparkContext

  def conf = sparkContext.getConf

  /**
   * Performs [[com.datastax.spark.connector.writer.WritableToCassandra]] for each produced RDD.
   * Uses specific column names with an additional batch size.
   */
  def saveToCassandra(
    keyspaceName: String,
    tableName: String,
    columnNames: ColumnSelector = AllColumns,
    writeConf: WriteConf = WriteConf.fromSparkConf(conf),
    tokenRangeAccumulator: Option[TokenRangeAccumulator] = None)(
  implicit
    connector: CassandraConnector = CassandraConnector(conf),
    rwf: RowWriterFactory[T]): Unit = {
    warnIfKeepAliveIsShort()

    val writer = TableWriter(connector, keyspaceName, tableName, columnNames, writeConf, tokenRangeAcc = tokenRangeAccumulator)
    dstream.foreachRDD(rdd => rdd.sparkContext.runJob(rdd, writer.write _))
  }

  /**
    * Delete data from Cassandra table, using data from the stream as a list of primary keys.
    * Uses the specified column names.
    * @see [[com.datastax.spark.connector.writer.WritableToCassandra]]
    */
  def deleteFromCassandra(
    keyspaceName: String,
    tableName: String,
    deleteColumns: ColumnSelector = SomeColumns(),
    keyColumns: ColumnSelector = PrimaryKeyColumns,
    writeConf: WriteConf = WriteConf.fromSparkConf(sparkContext.getConf))(
  implicit
    connector: CassandraConnector = CassandraConnector(sparkContext),
    rwf: RowWriterFactory[T]): Unit = {

    // column delete require full primary key, partition key is enough otherwise
    val columnDelete = deleteColumns match {
      case c :SomeColumns => c.columns.nonEmpty
      case _  => false
    }
    val writer = TableWriter(connector, keyspaceName, tableName, keyColumns, writeConf, !columnDelete)
    dstream.foreachRDD(rdd => rdd.sparkContext.runJob(rdd, writer.delete(deleteColumns) _))
  }

  /**
   * Transforms RDDs with [[com.datastax.spark.connector.RDDFunctions.repartitionByCassandraReplica]]
   * for each produced RDD.
   */
  def repartitionByCassandraReplica(
    keyspaceName: String,
    tableName: String,
    partitionsPerHost: Int = 10,
    partitionKeyMapper: ColumnSelector = PartitionKeyColumns)(
  implicit
    connector: CassandraConnector = CassandraConnector(conf),
    currentType: ClassTag[T],
    rwf: RowWriterFactory[T]): DStream[T] = {

    val partitioner = new ReplicaPartitioner[T](
      tableName,
      keyspaceName,
      partitionsPerHost,
      partitionKeyMapper,
      connector)

    dstream.transform(rdd => {
      val repart = rdd
        .map((_, None))
        .partitionBy(partitioner)
        .mapPartitions(_.map(_._1), preservesPartitioning = true)

      new CassandraPartitionedRDD[T](repart, keyspaceName, tableName)
    })
  }

  /**
   * Transforms RDDs with [[com.datastax.spark.connector.RDDFunctions.joinWithCassandraTable]]
   * for each produced RDD
   */
  def joinWithCassandraTable[R](
    keyspaceName: String, tableName: String,
    selectedColumns: ColumnSelector = AllColumns,
    joinColumns: ColumnSelector = PartitionKeyColumns)(
  implicit
    connector: CassandraConnector = CassandraConnector(sparkContext),
    newType: ClassTag[R],
    @transient rrf: RowReaderFactory[R],
    ev: ValidRDDType[R],
    currentType: ClassTag[T],
    @transient rwf: RowWriterFactory[T]): DStream[(T,R)] = {

    warnIfKeepAliveIsShort()

    /**
     * To make this function serializable for Checkpointing we will make a rowWriter and the use
     * that to create our future joinRDDs.
     */
    val rdd = dstream.context.sparkContext.emptyRDD[T]
    val cjRdd = rdd.joinWithCassandraTable[R](keyspaceName, tableName, selectedColumns, joinColumns)

    dstream.transform(rdd =>
      cjRdd.applyToRDD(rdd))
  }
}
