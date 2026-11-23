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

package com.datastax.spark.connector.rdd.partitioner

import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cluster.DefaultCluster
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.rdd.partitioner.dht.LongToken

import scala.language.postfixOps

class DataSizeEstimatesSpec extends SparkCassandraITFlatSpecBase with DefaultCluster {
  override lazy val conn = CassandraConnector(defaultConf)

  val tableName = "table1"

  override def beforeClass: Unit = {
    conn.withSessionDo { session => createKeyspace(session) }


    conn.withSessionDo { session =>
      val executor = getExecutor(session)
      session.execute(s"CREATE TABLE $ks.$tableName(key int PRIMARY KEY, value VARCHAR)")
      val ps = session.prepare(s"INSERT INTO $ks.$tableName(key, value) VALUES (?, ?)")
      awaitAll(
        for (i <- 1 to 1000) yield
          executor.executeAsync(ps.bind(i.asInstanceOf[AnyRef], "value" + i))
      )
      executor.waitForCurrentlyExecutingTasks()
    }

    cluster.refreshSizeEstimates()
  }

  "DataSizeEstimates" should "fetch data size estimates for a known table" in {
    val estimates = new DataSizeEstimates[Long, LongToken](conn, ks, tableName)
    estimates.partitionCount should be > 500L
    estimates.partitionCount should be < 2000L
    estimates.dataSizeInBytes should be > 0L
  }

  it should "should return zeroes for an empty table" in {
    val tableName = "table2"
    conn.withSessionDo { session =>
      session.execute(s"CREATE TABLE $ks.$tableName(key int PRIMARY KEY, value VARCHAR)")
    }

    val estimates = new DataSizeEstimates[Long, LongToken](conn, ks, tableName)
    estimates.partitionCount shouldBe 0L
    estimates.dataSizeInBytes shouldBe 0L
  }

  it should "return zeroes for a non-existing table" in {
    val tableName = "table3"
    val estimates = new DataSizeEstimates[Long, LongToken](conn, ks, tableName)
    estimates.partitionCount shouldBe 0L
    estimates.dataSizeInBytes shouldBe 0L
  }
}
