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

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.datasource.ScanHelper
import com.datastax.spark.connector.datasource.ScanHelper.CqlQueryParts
import com.datastax.spark.connector.rdd.CassandraLimit._
import com.datastax.spark.connector.rdd.partitioner.dht.{TokenFactory, Token => ConnectorToken}
import com.datastax.spark.connector.rdd.partitioner.{CassandraPartition, CassandraPartitionGenerator, CqlTokenRange, NodeAddresses, _}
import com.datastax.spark.connector.rdd.reader._
import com.datastax.spark.connector.util.CountingIterator
import com.datastax.spark.connector.writer.RowWriterFactory
import org.apache.spark.metrics.InputMetricsUpdater
import org.apache.spark.rdd.{PartitionCoalescer, RDD}
import org.apache.spark.{Partition, Partitioner, SparkContext, TaskContext}

import java.io.IOException
import scala.language.existentials
import scala.reflect.ClassTag
import scala.jdk.CollectionConverters._


/** RDD representing a Table Scan of A Cassandra table.
  *
  * This class is the main entry point for analyzing data in Cassandra database with Spark.
  * Obtain objects of this class by calling
  * [[com.datastax.spark.connector.SparkContextFunctions.cassandraTable]].
  *
  * Configuration properties should be passed in the [[org.apache.spark.SparkConf SparkConf]]
  * configuration of [[org.apache.spark.SparkContext SparkContext]].
  * `CassandraRDD` needs to open connection to Cassandra, therefore it requires appropriate
  * connection property values to be present in [[org.apache.spark.SparkConf SparkConf]].
  * For the list of required and available properties, see
  * [[com.datastax.spark.connector.cql.CassandraConnector CassandraConnector]].
  *
  * `CassandraRDD` divides the data set into smaller partitions, processed locally on every
  * cluster node. A data partition consists of one or more contiguous token ranges.
  * To reduce the number of roundtrips to Cassandra, every partition is fetched in batches.
  *
  * The following properties control the number of partitions and the fetch size:
  * - spark.cassandra.input.split.sizeInMB: approx amount of data to be fetched into a single Spark
  *   partition, default 512 MB
  * - spark.cassandra.input.fetch.sizeInRows:  number of CQL rows fetched per roundtrip,
  *   default 1000
  *
  * A `CassandraRDD` object gets serialized and sent to every Spark Executor, which then
  * calls the `compute` method to fetch the data on every node. The `getPreferredLocations`
  * method tells Spark the preferred nodes to fetch a partition from, so that the data for
  * the partition are at the same node the task was sent to. If Cassandra nodes are collocated
  * with Spark nodes, the queries are always sent to the Cassandra process running on the same
  * node as the Spark Executor process, hence data are not transferred between nodes.
  * If a Cassandra node fails or gets overloaded during read, the queries are retried
  * to a different node.
  *
  * By default, reads are performed at ConsistencyLevel.LOCAL_ONE in order to leverage data-locality
  * and minimize network traffic. This read consistency level is controlled by the
  * spark.cassandra.input.consistency.level property.
  */
class CassandraTableScanRDD[R] private[connector](
    @transient val sc: SparkContext,
    val connector: CassandraConnector,
    val keyspaceName: String,
    val tableName: String,
    val columnNames: ColumnSelector = AllColumns,
    val where: CqlWhereClause = CqlWhereClause.empty,
    val limit: Option[CassandraLimit] = None,
    val clusteringOrder: Option[ClusteringOrder] = None,
    val readConf: ReadConf = ReadConf(),
    overridePartitioner: Option[Partitioner] = None,
    val tokenRangeFilter: (ConnectorToken[_], ConnectorToken[_]) => Boolean = (_, _) => true)(
  implicit
    val classTag: ClassTag[R],
    @transient val rowReaderFactory: RowReaderFactory[R])
  extends CassandraRDD[R](sc, Seq.empty)
  with CassandraTableRowReaderProvider[R] {

  override type Self = CassandraTableScanRDD[R]

  override protected def copy(
    columnNames: ColumnSelector = columnNames,
    where: CqlWhereClause = where,
    limit: Option[CassandraLimit] = limit,
    clusteringOrder: Option[ClusteringOrder] = None,
    readConf: ReadConf = readConf,
    connector: CassandraConnector = connector): Self = {

    require(sc != null,
      "RDD transformation requires a non-null SparkContext. " +
        "Unfortunately SparkContext in this CassandraRDD is null. " +
        "This can happen after CassandraRDD has been deserialized. " +
        "SparkContext is not Serializable, therefore it deserializes to null." +
        "RDD transformations are not allowed inside lambdas used in other RDD transformations.")

    new CassandraTableScanRDD[R](
      sc = sc,
      connector = connector,
      keyspaceName = keyspaceName,
      tableName = tableName,
      columnNames = columnNames,
      where = where,
      limit = limit,
      clusteringOrder = clusteringOrder,
      readConf = readConf,
      overridePartitioner = overridePartitioner,
      tokenRangeFilter = tokenRangeFilter)
  }



  override protected def convertTo[B : ClassTag : RowReaderFactory]: CassandraTableScanRDD[B] = {
    new CassandraTableScanRDD[B](
      sc = sc,
      connector = connector,
      keyspaceName = keyspaceName,
      tableName = tableName,
      columnNames = columnNames,
      where = where,
      limit = limit,
      clusteringOrder = clusteringOrder,
      readConf = readConf,
      overridePartitioner = overridePartitioner,
      tokenRangeFilter = tokenRangeFilter)
  }

  /**
    * Internal method for assigning a partitioner to this RDD, this lacks type safety checks for
    * the Partitioner of type [K, V, T]. End users will use the implicit provided in
    * [[CassandraTableScanPairRDDFunctions]]
    */
  private[connector] def withPartitioner[K, V, T <: ConnectorToken[V]](
    partitioner: Option[Partitioner]): CassandraTableScanRDD[R] = {

    val cassPart = partitioner match {
      case Some(newPartitioner: CassandraPartitioner[_, _, _]) => {
        this.partitioner match {
          case Some(currentPartitioner: CassandraPartitioner[_, _, _]) =>
            /** Preserve the mapping set by the current partitioner **/
            logDebug(
              s"""Preserving Partitioner: $currentPartitioner with mapping
                 |${currentPartitioner.keyMapping}""".stripMargin)
            Some(
              newPartitioner
                .withTableDef(tableDef)
                .withKeyMapping(currentPartitioner.keyMapping))
          case _ =>
            logDebug(s"Assigning new Partitioner $newPartitioner")
            Some(newPartitioner.withTableDef(tableDef))
        }
      }
      case Some(other: Partitioner) => throw new IllegalArgumentException(
        s"""Unable to assign
          |non-CassandraPartitioner $other to CassandraTableScanRDD """.stripMargin)
      case None => None
    }

    new CassandraTableScanRDD[R](
      sc = sc,
      connector = connector,
      keyspaceName = keyspaceName,
      tableName = tableName,
      columnNames = columnNames,
      where = where,
      limit = limit,
      clusteringOrder = clusteringOrder,
      readConf = readConf,
      overridePartitioner = cassPart,
      tokenRangeFilter = tokenRangeFilter)
  }

  /** Selects a subset of columns mapped to the key and returns an RDD of pairs.
    * Similar to the builtin Spark keyBy method, but this one uses implicit
    * RowReaderFactory to construct the key objects.
    * The selected columns must be available in the CassandraRDD.
    *
    * If the selected columns contain the complete partition key a
    * `CassandraPartitioner` will also be created.
    *
    * @param columns column selector passed to the rrf to create the row reader,
    *                useful when the key is mapped to a tuple or a single value
    */
  def keyBy[K](columns: ColumnSelector)(implicit
    classtag: ClassTag[K],
    rrf: RowReaderFactory[K],
    rwf: RowWriterFactory[K]): CassandraTableScanRDD[(K, R)] = {

    val kRRF = implicitly[RowReaderFactory[K]]
    val vRRF = rowReaderFactory
    implicit val kvRRF = new KeyValueRowReaderFactory[K, R](columns, kRRF, vRRF)

    val selectedColumnNames = columns.selectFrom(tableDef).map(_.columnName).toSet
    val partitionKeyColumnNames = PartitionKeyColumns.selectFrom(tableDef).map(_.columnName).toSet

    if (selectedColumnNames.asJava.containsAll(partitionKeyColumnNames.asJava)) {
      val partitioner = partitionGenerator.partitioner[K](columns)
      logDebug(
        s"""Made partitioner ${partitioner} for $this""".stripMargin)
      convertTo[(K, R)].withPartitioner(partitioner)

    } else {
      convertTo[(K, R)]
    }
  }

  /** Extracts a key of the given class from the given columns.
    *
    *  @see `keyBy(ColumnSelector)` */
  def keyBy[K](columns: ColumnRef*)(implicit
    classtag: ClassTag[K],
    rrf: RowReaderFactory[K],
    rwf: RowWriterFactory[K]): CassandraTableScanRDD[(K, R)] =
    keyBy(SomeColumns(columns: _*))

  /** Extracts a key of the given class from all the available columns.
    *
    * @see `keyBy(ColumnSelector)` */
  def keyBy[K]()(implicit
    classtag: ClassTag[K],
    rrf: RowReaderFactory[K],
    rwf: RowWriterFactory[K]): CassandraTableScanRDD[(K, R)] =
    keyBy(AllColumns)

  def minimalSplitCount: Int = {
    context.defaultParallelism * 2 + 1
  }

  @transient lazy val partitionGenerator = ScanHelper.getPartitionGenerator(
    connector,
    tableDef,
    where,
    minimalSplitCount,
    readConf.splitCount,
    splitSize)

  /**
    * This method overrides the default spark behavior and will not create a CoalesceRDD. Instead it will reduce
    * the number of partitions by adjusting the partitioning of C* data on read. Using this method will override
    * spark.cassandra.input.split.size.
    * The method is useful with where() method call, when actual size of data is smaller then the table size.
    * It has no effect if a partition key is used in where clause.
    *
    * @param numPartitions      number of partitions
    * @param shuffle            whether to call shuffle after
    * @param partitionCoalescer is ignored if no shuffle, or just passed to shuffled CoalesceRDD
    * @param ord
    * @return new CassandraTableScanRDD with predefined number of partitions
    */

  override def coalesce(numPartitions: Int, shuffle: Boolean = false, partitionCoalescer: Option[PartitionCoalescer])(implicit ord: Ordering[R] = null): RDD[R]
  = {
    val rdd = copy(readConf = readConf.copy(splitCount = Some(numPartitions)))
    if (shuffle) {
      rdd.superCoalesce(numPartitions, shuffle, partitionCoalescer)
    } else {
      rdd
    }
  }

  private def superCoalesce(numPartitions: Int, shuffle: Boolean = false, partitionCoalescer: Option[PartitionCoalescer])(implicit ord: Ordering[R] = null) =
    super.coalesce(numPartitions, shuffle, partitionCoalescer);


  @transient override val partitioner = overridePartitioner

  override def getPartitions: Array[Partition] = {
    verify() // let's fail fast
    val partitions: Array[Partition] = partitioner match {
      case Some(cassPartitioner: CassandraPartitioner[_, _, _]) => {
        cassPartitioner.verify()
        cassPartitioner.partitions.toArray[Partition]
      }
      case Some(other: Partitioner) =>
        throw new IllegalArgumentException(s"Invalid partitioner $other")

      case None => partitionGenerator.partitions.toArray[Partition]
    }

    logDebug(s"Created total ${partitions.length} partitions for $keyspaceName.$tableName.")
    logTrace("Partitions: \n" + partitions.mkString("\n"))
    partitions
  }

  override def getPreferredLocations(split: Partition): Seq[String] =
    split.asInstanceOf[CassandraPartition[_, _]].endpoints.toIndexedSeq

  override def compute(split: Partition, context: TaskContext): Iterator[R] = {
    val partition = split.asInstanceOf[CassandraPartition[TokenFactory.V, TokenFactory.T]]
    val tokenRanges = // Only let token ranges that shouldn't be skipped through
      partition.tokenRanges.filter { cqlRange =>
        val (start, end) = (cqlRange.range.start.asInstanceOf[ConnectorToken[_]],
          cqlRange.range.end.asInstanceOf[ConnectorToken[_]])
        val result = tokenRangeFilter(start, end)

        logInfo(s"tokenRangeFilter(${start}, ${end}) = ${result}")

        result
      }
    val metricsUpdater = InputMetricsUpdater(context, readConf)

    val columnNames = selectedColumnRefs.map(_.selectedAs).toIndexedSeq

    val scanner = connector.connectionFactory.getScanner(readConf, connector.conf, columnNames)
    val queryParts = CqlQueryParts(selectedColumnRefs, where, limit, clusteringOrder)

    // Iterator flatMap trick flattens the iterator-of-iterator structure into a single iterator.
    // flatMap on iterator is lazy, therefore a query for the next token range is executed not earlier
    // than all of the rows returned by the previous query have been consumed
    val rowIterator = tokenRanges.iterator.flatMap { range =>
      try {
        val scanResult = ScanHelper.fetchTokenRange(scanner, tableDef, queryParts, range, consistencyLevel, fetchSize)
        val iteratorWithMetrics = scanResult.rows.map(metricsUpdater.updateMetrics)
        val result = iteratorWithMetrics.map(rowReader.read(_, scanResult.metadata))
        result
      } catch {
        case t: Throwable =>
          throw new IOException(s"Exception during scan execution for $range : ${t.getMessage}", t)
      }
    }
    val countingIterator = new CountingIterator(rowIterator, limitForIterator(limit))

    context.addTaskCompletionListener { context =>
      val duration = metricsUpdater.finish() / 1000000000d
      logDebug(f"Fetched ${countingIterator.count} rows from $keyspaceName.$tableName " +
        f"for partition ${partition.index} in $duration%.3f s.")
      scanner.close()
      context
    }

    countingIterator
  }

  override def toEmptyCassandraRDD: EmptyCassandraRDD[R] = {
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

  override def cassandraCount(): Long = {
    columnNames match {
      case SomeColumns(_) =>
        logWarning("You are about to count rows but an explicit projection has been specified.")
      case _ =>
    }

    val counts = CassandraTableScanRDD.countRDD(this)
    counts.reduce(_ + _)
  }

}

object CassandraTableScanRDD {

  def apply[T : ClassTag : RowReaderFactory](
    sc: SparkContext,
    keyspaceName: String,
    tableName: String): CassandraTableScanRDD[T] = {

    new CassandraTableScanRDD[T](
      sc = sc,
      connector = CassandraConnector(sc),
      keyspaceName = keyspaceName,
      tableName = tableName,
      readConf = ReadConf.fromSparkConf(sc.getConf),
      columnNames = AllColumns,
      where = CqlWhereClause.empty)
  }

  def apply[K, V](
      sc: SparkContext,
      keyspaceName: String,
      tableName: String)(
    implicit
      keyCT: ClassTag[K],
      valueCT: ClassTag[V],
      rrf: RowReaderFactory[(K, V)],
      rwf: RowWriterFactory[K]): CassandraTableScanRDD[(K, V)] = {

    val rdd = new CassandraTableScanRDD[(K, V)](
      sc = sc,
      connector = CassandraConnector(sc),
      keyspaceName = keyspaceName,
      tableName = tableName,
      readConf = ReadConf.fromSparkConf(sc.getConf),
      columnNames = AllColumns,
      where = CqlWhereClause.empty)
    rdd.withPartitioner(rdd.partitionGenerator.partitioner[K](PartitionKeyColumns))
  }

  /**
    * It is used by cassandraCount() and spark sql cassandra source to push down counts to cassandra
    * @param rdd
    * @tparam R
    * @return rdd, each partitions will have only one long value: number of rows in the partition
    */
  def countRDD[R] (rdd: CassandraTableScanRDD[R]): CassandraTableScanRDD[Long] = {
    new CassandraTableScanRDD[Long](
      sc = rdd.sc,
      connector = rdd.connector,
      keyspaceName = rdd.keyspaceName,
      tableName = rdd.tableName,
      columnNames = SomeColumns(RowCountRef),
      where = rdd.where,
      limit = rdd.limit,
      clusteringOrder = rdd.clusteringOrder,
      readConf = rdd.readConf,
      tokenRangeFilter = rdd.tokenRangeFilter)
  }
}
