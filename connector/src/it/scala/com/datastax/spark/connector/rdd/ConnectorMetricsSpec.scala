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

import java.util.concurrent.LinkedTransferQueue

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.spark.connector._
import com.datastax.spark.connector.cluster.{DefaultCluster, SeparateJVM}
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.rdd.ConnectorMetricsListener.stagesMetrics
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}

class ConnectorMetricsSpec extends SparkCassandraITFlatSpecBase with DefaultCluster with SeparateJVM {

  val ourSc = new SparkContext(
    super.defaultConf
      .clone()
      .set("spark.extraListeners", classOf[ConnectorMetricsListener].getName)
      .setMaster("local")
      .setAppName(getClass.getSimpleName)
  )

  override def afterClass: Unit = {
    super.afterClass
    ourSc.stop()
  }


  override lazy val conn = CassandraConnector(defaultConf)

  override def beforeClass {
    conn.withSessionDo { session =>
      session.execute(
        s"""
           |CREATE KEYSPACE IF NOT EXISTS $ks
           |WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }
           |""".stripMargin)
      makeTables(session)
    }
  }

  def makeTables(session: CqlSession): Unit = {
    session.execute(
      s"""
         |CREATE TABLE IF NOT EXISTS $ks.leftjoin (
         |  key INT, 
         |  x INT, 
         |  PRIMARY KEY (key))
         |""".stripMargin)

    session.execute(
      s"""
         |CREATE TABLE IF NOT EXISTS $ks.rightjoin (
         |  key INT, 
         |  y INT, 
         |  z INT, 
         |  PRIMARY KEY (key, y))
         |""".stripMargin)

    for (i <- 1 to 200) {
      session.execute(s"INSERT INTO $ks.leftjoin (key, x) values ($i, $i)")
      if ((i & 1) == 0) {
        for (j <- 1 to 200) {
          session.execute(s"INSERT INTO $ks.rightjoin (key, y, z) values ($i, $j, $i)")
        }
      }
    }
  }

  "InputMetricsUpdater" should "properly measure amount of data retrieved with CassandraTableScanRDD" in {
    stagesMetrics.clear()
    val rdd = ourSc.cassandraTable(ks, "leftjoin")
    rdd.withReadConf(rdd.readConf.copy(splitCount = Some(16))).collect()
    Eventually.eventually {
      stagesMetrics.size() should be(1)
    }
    val metrics = stagesMetrics.poll()
    metrics.inputMetrics.recordsRead should be(200)
    metrics.inputMetrics.bytesRead should be(200 * 8)
  }

  it should "properly measure amount of data retrieved with CassandraJoinRDD" in {
    stagesMetrics.clear()
    val rdd = ourSc.cassandraTable(ks, "leftjoin")
    val joined = rdd.withReadConf(rdd.readConf.copy(splitCount = Some(16)))
      .joinWithCassandraTable(ks, "rightjoin", joinColumns = PartitionKeyColumns)
    joined.withReadConf(joined.readConf.copy(splitCount = Some(16))).collect()
    Eventually.eventually {
      stagesMetrics.size() should be(1)
    }
    val metrics = stagesMetrics.poll()
    metrics.inputMetrics.recordsRead should be(200 + 100 * 200)
    metrics.inputMetrics.bytesRead should be(200 * 8 + 100 * 200 * 12)
  }

  it should "properly measure amount of data retrieved with CassandraLeftJoinRDD" in {
    stagesMetrics.clear()
    val rdd = ourSc.cassandraTable(ks, "leftjoin")
    val joined = rdd.withReadConf(rdd.readConf.copy(splitCount = Some(16)))
      .leftJoinWithCassandraTable(ks, "rightjoin", joinColumns = PartitionKeyColumns)
    joined.withReadConf(joined.readConf.copy(splitCount = Some(16))).collect()
    Eventually.eventually {
      stagesMetrics.size() should be(1)
    }
    val metrics = stagesMetrics.poll()
    metrics.inputMetrics.recordsRead should be(200 + 100 * 200)
    metrics.inputMetrics.bytesRead should be(200 * 8 + 100 * 200 * 12)
  }

  it should "properly measure amount of data retrieved with CassandraMergeJoinRDD" in {
    stagesMetrics.clear()
    val left = ourSc.cassandraTable(ks, "leftjoin")
    val right = ourSc.cassandraTable(ks, "rightjoin")
    val joined = new CassandraMergeJoinRDD(
      ourSc,
      left.withReadConf(left.readConf.copy(splitCount = Some(16))),
      right.withReadConf(right.readConf.copy(splitCount = Some(16))))
    joined.collect()
    Eventually.eventually {
      stagesMetrics.size() should be(1)
    }
    val metrics = stagesMetrics.poll()
    metrics.inputMetrics.recordsRead should be(200 + 100 * 200)
    metrics.inputMetrics.bytesRead should be(200 * (8 + 8) + 100 * 200 * (12 + 8))
  }

  it should "properly measure amount of data written to Cassandra" in {
    stagesMetrics.clear()
    val rdd = ourSc.makeRDD(1 to 200, 16).map(x => (x, x))
    rdd.saveToCassandra(ks, "leftjoin")
    Eventually.eventually {
      stagesMetrics.size() should be(1)
    }

    Eventually.eventually(Eventually.timeout(Span(20, Seconds))) {
      val metrics = stagesMetrics.poll()
      metrics.outputMetrics.recordsWritten should be(200)
      metrics.outputMetrics.bytesWritten should be(200 * 8)
    }
  }
}

class ConnectorMetricsListener(conf: SparkConf) extends SparkListener {
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val metrics = stageCompleted.stageInfo.taskMetrics
    stagesMetrics.offer(metrics)
  }
}

object ConnectorMetricsListener {
  val stagesMetrics = new LinkedTransferQueue[TaskMetrics]()
}
