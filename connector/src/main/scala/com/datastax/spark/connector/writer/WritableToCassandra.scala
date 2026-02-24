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

package com.datastax.spark.connector.writer

import com.datastax.spark.connector.ColumnSelector
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkContext

abstract class WritableToCassandra[T] {

  def sparkContext: SparkContext

  private[connector] lazy val connector = CassandraConnector(sparkContext)

  /**
   * Saves the data from [[org.apache.spark.rdd.RDD RDD]] to a Cassandra table.
   * By default, it saves all properties that have corresponding Cassandra columns.
   *
   * Example:
   * {{{
   *   CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };
   *   CREATE TABLE test.words(word VARCHAR PRIMARY KEY, count INT, other VARCHAR);
   * }}}
   *
   * {{{
   *   case class WordCount(word: String, count: Int, other: String)
   *   val rdd = sc.parallelize(Seq(WordCount("foo", 5, "bar")))
   * }}}
   *
   * By default, the underlying RDD class must provide data for all columns:
   * {{{
   *   rdd.saveToCassandra("test", "words")
   * }}}
   *
   * By default, writes are performed at ConsistencyLevel.LOCAL_QUORUM.
   * This write consistency level is controlled by the following property:
   *   - spark.cassandra.output.consistency.level: consistency level for RDD writes, string matching the ConsistencyLevel enum name.
   *
   * @param keyspaceName the name of the Keyspace to use
   * @param tableName the name of the Table to use
   * @param columnNames The list of column names to save data to.
   *                Uses only the unique column names, and you must select at least all primary key
   *                columns. All other fields are discarded. Non-selected property/column names are left unchanged.
   * @param writeConf additional configuration object allowing to set consistency level, batch size, etc.
   */
  def saveToCassandra(keyspaceName: String,
                      tableName: String,
                      columnNames: ColumnSelector,
                      writeConf: WriteConf,
                      tokenRangeAccumulator: Option[TokenRangeAccumulator])
                     (implicit connector: CassandraConnector, rwf: RowWriterFactory[T]): Unit

  /**
    * Delete data from Cassandra table, using data from the [[org.apache.spark.rdd.RDD RDD]] as a list of primary keys.
    * Uses the specified column names.
    * By default, it deletes all columns from corresponding Cassandra rows.
    *
    * Example:
    * {{{
    *   CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };
    *   CREATE TABLE test.words(word VARCHAR PRIMARY KEY, count INT, other VARCHAR);
    *   INSERT INTO test.words((word,count,other) values ('foo', 5, 'foo');
    * }}}
    *
    * {{{
    *   case class WordCount(word: String, count: Int, other: String)
    *   val rdd = sc.cassandraTable("test", "words")
    * }}}
    *
    * The underlying RDD class must provide data for all primary key columns.
    * Delete "other" column values
    * {{{
    *   rdd.deleteFromCassandra("test", "words", Seq("other"))
    * }}}
    *
    * This delete consistency level and other properties are the same as for writes
    *
    * @param keyspaceName the name of the Keyspace to use
    * @param tableName the name of the Table to use
    * @param deleteColumns The list of column names to delete, empty ColumnSelector means full row.
    * @param keyColumns Primary key columns selector, Optional. All RDD primary columns columns will be checked by default
    * @param writeConf additional configuration object allowing to set consistency level, batch size, etc.
    */
  def deleteFromCassandra(keyspaceName: String,
                          tableName: String,
                          deleteColumns: ColumnSelector,
                          keyColumns: ColumnSelector,
                          writeConf: WriteConf)
                         (implicit connector: CassandraConnector, rwf: RowWriterFactory[T]): Unit

}
