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

import com.datastax.spark.connector.cluster.DefaultCluster
import com.datastax.spark.connector.{CassandraRow, SparkCassandraITFlatSpecBase}
import com.datastax.spark.connector.cql.CassandraConnector

class CassandraRDDMockSpec extends SparkCassandraITFlatSpecBase with DefaultCluster {

  override lazy val conn = CassandraConnector(defaultConf)

  "A CassandraRDDMock" should "behave like a CassandraRDD without needing Cassandra" in {
    val columns = Seq("key", "value")
    //Create a fake CassandraRDD[CassandraRow]
    val rdd = sc
      .parallelize(1 to 10)
      .map(num => CassandraRow.fromMap(columns.zip(Seq(num, num)).toMap))

    val fakeCassandraRDD: CassandraRDD[CassandraRow] = new CassandraRDDMock(rdd)

    fakeCassandraRDD.cassandraCount() should be (10)
  }
}
