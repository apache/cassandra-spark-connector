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

import java.lang.{Long => JLong}

import com.datastax.spark.connector._
import com.datastax.spark.connector.cluster.TwoNodeCluster
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.rdd.partitioner.EndpointPartition

import scala.concurrent.Future

class ReplicaRepartitionedCassandraRDDSpec extends SparkCassandraITFlatSpecBase with TwoNodeCluster {

  override lazy val conn = CassandraConnector(defaultConf)
  val tableName = "key_value"
  val keys = 0 to 200
  val total = 0 to 10000

  override def beforeClass {
    conn.withSessionDo { session =>
      createKeyspace(session)
      val startTime = System.currentTimeMillis()

      val executor = getExecutor(session)

      awaitAll(
        Future {
          session.execute(
            s"""
               |CREATE TABLE $ks.$tableName (
               |  key INT,
               |  group BIGINT,
               |  value TEXT,
               |  PRIMARY KEY (key, group)
               |)""".stripMargin)
          val ps = session
            .prepare(s"""INSERT INTO $ks.$tableName (key, group, value) VALUES (?, ?, ?)""")
          awaitAll((for (value <- total) yield
            executor.executeAsync(ps.bind(value: Integer, (value * 100).toLong: JLong, value.toString))): _*)
        }
      )
      executor.waitForCurrentlyExecutingTasks()
      println(s"Took ${(System.currentTimeMillis() - startTime) / 1000.0} Seconds to setup Suite Data")
    }
  }

  def checkArrayCassandraRow[T](result: Array[(T, CassandraRow)]) = {
    markup("Checking RightSide Join Results")
    result.length should be(keys.length)
    for (key <- keys) {
      val sorted_result = result.map(_._2).sortBy(_.getInt(0))
      sorted_result(key).getInt("key") should be(key)
      sorted_result(key).getLong("group") should be(key * 100)
      sorted_result(key).getString("value") should be(key.toString)
    }
  }

  "A Tuple RDD specifying partition keys" should "be repartitionable" in {
    val source = sc.parallelize(keys).map(Tuple1(_))
    val repart = source.repartitionByCassandraReplica(ks, tableName, 10)
    repart.partitions.length should be(conn.hosts.size * 10)
    conn.hosts.size should be(2)
    conn.hosts should be(cluster.addresses.toSet)
    val someCass = repart.joinWithCassandraTable(ks, tableName)
    someCass.partitions.foreach {
      case e: EndpointPartition =>
        conn.hostAddresses.map(_.getHostAddress) should contain(e.endpoints.head)
      case _ =>
        fail("Unable to get endpoints on repartitioned RDD, This means preferred locations will be broken")
    }
    val result = someCass.collect
    checkArrayCassandraRow(result)
  }

  it should "be deterministically repartitionable" in {
    val source = sc.parallelize(keys).map(Tuple1(_))
    val repartRDDs = (1 to 10).map(_ =>
      source
        .repartitionByCassandraReplica(ks, tableName, 10)
        .mapPartitionsWithIndex((index, it) => it.map((_, index))))
    val first = repartRDDs(1).collect
    repartRDDs.foreach(rdd => rdd.collect should be(first))
  }

}
