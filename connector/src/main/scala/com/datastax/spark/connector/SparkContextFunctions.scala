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

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.rdd.reader.RowReaderFactory
import com.datastax.spark.connector.rdd._
import com.datastax.spark.connector.rdd.partitioner.dht.Token
import com.datastax.spark.connector.writer.RowWriterFactory
import org.apache.spark.SparkContext

import scala.reflect.ClassTag

/** Provides Cassandra-specific methods on [[org.apache.spark.SparkContext SparkContext]] */
class SparkContextFunctions(@transient val sc: SparkContext) extends Serializable {

  /** Returns a view of a Cassandra table as `CassandraRDD`.
    * This method is made available on [[org.apache.spark.SparkContext SparkContext]] by importing
    * `com.datastax.spark.connector._`
    *
    * Depending on the type parameter passed to `cassandraTable`, every row is converted to one of the following:
    *   - an [[CassandraRow]] object (default, if no type given)
    *   - a tuple containing column values in the same order as columns selected by [[com.datastax.spark.connector.rdd.CassandraRDD#select CassandraRDD#select]]
    *   - object of a user defined class, populated by appropriate [[com.datastax.spark.connector.mapper.ColumnMapper ColumnMapper]]
    *
    * Example:
    * {{{
    *   CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };
    *   CREATE TABLE test.words (word text PRIMARY KEY, count int);
    *   INSERT INTO test.words (word, count) VALUES ('foo', 20);
    *   INSERT INTO test.words (word, count) VALUES ('bar', 20);
    *   ...
    * }}}
    * {{{
    *   // Obtaining RDD of CassandraRow objects:
    *   val rdd1 = sc.cassandraTable("test", "words")
    *   rdd1.first.getString("word")  // foo
    *   rdd1.first.getInt("count")    // 20
    *
    *   // Obtaining RDD of tuples:
    *   val rdd2 = sc.cassandraTable[(String, Int)]("test", "words").select("word", "count")
    *   rdd2.first._1  // foo
    *   rdd2.first._2  // 20
    *
    *   // Obtaining RDD of user defined objects:
    *   case class WordCount(word: String, count: Int)
    *   val rdd3 = sc.cassandraTable[WordCount]("test", "words")
    *   rdd3.first.word  // foo
    *   rdd3.first.count // 20
    * }}}*/
  def cassandraTable[T](
    keyspace: String,
    table: String,
    tokenRangeFilter: (Token[_], Token[_]) => Boolean = (_, _) => true)(
  implicit
    connector: CassandraConnector = CassandraConnector(sc),
    readConf: ReadConf = ReadConf.fromSparkConf(sc.getConf),
    ct: ClassTag[T], rrf: RowReaderFactory[T],
    ev: ValidRDDType[T]) = new CassandraTableScanRDD[T](sc, connector, keyspace, table,
      readConf = readConf, tokenRangeFilter = tokenRangeFilter)

  /** Produces the empty CassandraRDD which does not perform any validation and it does not even
    * try to return any rows. */
  def emptyCassandraTable[T](keyspace: String, table: String)
                            (implicit connector: CassandraConnector = CassandraConnector(sc),
                             readConf: ReadConf = ReadConf.fromSparkConf(sc.getConf),
                             ct: ClassTag[T], rrf: RowReaderFactory[T],
                             ev: ValidRDDType[T]) =
    new EmptyCassandraRDD[T](sc, keyspace, table, readConf = readConf)
}
