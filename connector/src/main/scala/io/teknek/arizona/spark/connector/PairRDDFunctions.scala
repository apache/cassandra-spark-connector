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

package io.teknek.arizona.spark.connector

import _root_.io.teknek.arizona.spark.connector.rdd.SpannedByKeyRDD
import org.apache.spark.rdd.RDD

class PairRDDFunctions[K, V](rdd: RDD[(K, V)]) extends Serializable {

  /**
   * Groups items with the same key, assuming the items with the same key are next to each other
   * in the collection. It does not perform shuffle, therefore it is much faster than using
   * much more universal Spark RDD `groupByKey`. For this method to be useful with Cassandra tables,
   * the key must represent a prefix of the primary key, containing at least the partition key of the
   * Cassandra table. */
  def spanByKey: RDD[(K, Seq[V])] =
    new SpannedByKeyRDD[K, V](rdd)

}
