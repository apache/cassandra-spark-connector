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

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.spark.connector.cluster.{DefaultCluster, SeparateJVM}
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.embedded.SparkTemplate._
import com.datastax.spark.connector.rdd.partitioner.dht.TokenFactory
import com.datastax.spark.connector.util.RuntimeUtil
import com.datastax.spark.connector.{SparkCassandraITFlatSpecBase, _}
import org.apache.spark.{SparkContext, SparkException}
import org.scalatest.Inspectors

class CustomTableScanMethodSpec extends SparkCassandraITFlatSpecBase with DefaultCluster with SeparateJVM with Inspectors {

  val ourSc = {
    new SparkContext(
      defaultConf
        .clone
        .set(CassandraConnectionFactory.FactoryParam.name,
          "com.datastax.spark.connector.rdd.DummyFactory"))
  }

  override lazy val conn = CassandraConnector(defaultConf)
  val tokenFactory = TokenFactory.forSystemLocalPartitioner(conn)
  val tableName = "data"
  val noMinimalThreshold = Int.MinValue

  override def beforeClass {
    conn.withSessionDo { session =>

      session.execute(s"CREATE KEYSPACE IF NOT EXISTS $ks " +
        s"WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }")

      session.execute(s"CREATE TABLE $ks.$tableName(key int primary key, value text)")
      val st = session.prepare(s"INSERT INTO $ks.$tableName(key, value) VALUES(?, ?)")
      // 1M rows x 64 bytes of payload = 64 MB of data + overhead
      for (i <- RuntimeUtil.toParallelIterable(1 to 100).par) {
        val key = i.asInstanceOf[AnyRef]
        val value = "123456789.123456789.123456789.123456789.123456789.123456789."
        session.execute(st.bind(key, value))
      }
    }
  }

  "CassandraTableScanRDD" should "be able to use a custom scan method" in withoutLogging {
    //The dummy method set in the SparkConf only throws a NIE
    val se = intercept[SparkException] {
      ourSc.cassandraTable[CassandraRow](ks, tableName).collect
    }
    se.getCause.getMessage should be(DummyFactory.nie.getMessage)
  }
}

object DummyFactory extends CassandraConnectionFactory {

  val nie = new NotImplementedError("TestingOnly")
  override def getScanner(
    readConf: ReadConf,
    connConf: CassandraConnectorConf,
    columnNames: IndexedSeq[String]): Scanner = throw nie

  /** Creates and configures native Cassandra connection */
  override def createSession(conf: CassandraConnectorConf): CqlSession =
    DefaultConnectionFactory.createSession(conf)
}
