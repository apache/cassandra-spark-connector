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
import com.datastax.spark.connector.rdd.{ReadConf, ValidRDDType}
import org.apache.spark.streaming.StreamingContext
import com.datastax.spark.connector.SparkContextFunctions
import com.datastax.spark.connector.rdd.partitioner.dht.Token
import com.datastax.spark.connector.rdd.reader.RowReaderFactory

/** Provides Cassandra-specific methods on `org.apache.spark.streaming.StreamingContext`.
  * @param ssc the Spark Streaming context
  */
@scala.annotation.nowarn("cat=deprecation")
class StreamingContextFunctions (ssc: StreamingContext) extends SparkContextFunctions(ssc.sparkContext) {
  import scala.reflect.ClassTag

  override def cassandraTable[T](keyspace: String, table: String,
                                 tokenRangeFilter: (Token[_], Token[_]) => Boolean = (_, _) => true)(
    implicit
      connector: CassandraConnector = CassandraConnector(ssc.sparkContext),
      readConf: ReadConf = ReadConf.fromSparkConf(sc.getConf),
      ct: ClassTag[T],
      rrf: RowReaderFactory[T],
      ev: ValidRDDType[T]): CassandraStreamingRDD[T] = {

    new CassandraStreamingRDD[T](ssc, connector, keyspace, table, readConf = readConf)
  }
}
