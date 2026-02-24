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

import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import com.datastax.spark.connector.rdd.partitioner.CassandraPartitioner
import com.datastax.spark.connector.rdd.partitioner.dht.Token
import com.datastax.spark.connector.rdd.reader.RowReaderFactory
import com.datastax.spark.connector.writer.RowWriterFactory
import org.apache.spark.Partitioner

import scala.language.existentials
import scala.reflect.ClassTag

final class CassandraTableScanPairRDDFunctions[K, V](rdd: CassandraTableScanRDD[(K, V)]) extends
  Serializable {

  /**
    * Use the [[CassandraPartitioner]] from another [[CassandraTableScanRDD]] which
    * shares the same key type. All Partition Keys columns must also be present in the keys of
    * the target RDD.
    */
  def applyPartitionerFrom[X](
    thatRdd: CassandraTableScanRDD[(K, X)]): CassandraTableScanRDD[(K, V)] = {

    val partitioner = thatRdd.partitioner match {
      case Some(part: CassandraPartitioner[K, _, _] @unchecked) => part
      case Some(other: Partitioner) =>
        throw new IllegalArgumentException(s"Partitioner $other is not a CassandraPartitioner")
      case None => throw new IllegalArgumentException(s"$thatRdd has no partitioner to apply")
    }

    applyPartitioner(partitioner)
  }

  /**
    * Use a specific [[CassandraPartitioner]] to use with this PairRDD.
    */
  def applyPartitioner[TokenValue, T <: Token[TokenValue]](
    partitioner: CassandraPartitioner[K, TokenValue, T]): CassandraTableScanRDD[(K, V)] = {
    rdd.withPartitioner(Some(partitioner))
  }
}

final class CassandraTableScanRDDFunctions[R](rdd: CassandraTableScanRDD[R]) extends Serializable {
  /**
    * Shortcut for `rdd.keyBy[K].applyPartitionerFrom(thatRDD[K, V])` where K is the key
    * type of the target RDD. This guarentees that the partitioner applied to this rdd
    * will match the key type.
    */
  def keyAndApplyPartitionerFrom[K, X](
    thatRDD: CassandraTableScanRDD[(K, X)],
    columnSelector: ColumnSelector = PartitionKeyColumns)(
  implicit
    classTag: ClassTag[K],
    rrf: RowReaderFactory[K],
    rwf: RowWriterFactory[K]) = {

    rdd.keyBy[K](columnSelector).applyPartitionerFrom(thatRDD)
  }
}
