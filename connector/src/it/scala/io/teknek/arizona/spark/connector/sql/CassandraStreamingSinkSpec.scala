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

package io.teknek.arizona.spark.connector.sql

import java.nio.file.Files

import _root_.io.teknek.arizona.spark.connector.SparkCassandraITFlatSpecBase
import _root_.io.teknek.arizona.spark.connector.cluster.DefaultCluster
import _root_.io.teknek.arizona.spark.connector.cql.CassandraConnector
import _root_.org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}

import scala.concurrent.Future
import scala.jdk.CollectionConverters._

class CassandraStreamingSinkSpec extends SparkCassandraITFlatSpecBase with DefaultCluster with Eventually {

  override lazy val conn = CassandraConnector(defaultConf)

  override def beforeClass {
    conn.withSessionDo { session =>
      createKeyspace(session)

      awaitAll(
        Future {
          session.execute(
            s"""CREATE TABLE IF NOT EXISTS $ks.kv (key int, value int, PRIMARY KEY (key))""".stripMargin)
        },
        Future {
          session.execute(
            s"""CREATE TABLE IF NOT EXISTS $ks.empty (key int, value int, PRIMARY KEY (key))""".stripMargin)
        },
        Future {
          session.execute(
            s"""CREATE TABLE IF NOT EXISTS $ks.update_mode (
               |content_id int,
               |bucket_start_time timestamp,
               |event_count bigint,
               |PRIMARY KEY (content_id, bucket_start_time))""".stripMargin)
        }

      )
    }
  }

  "CassandraStreamingSink" should " be able to written to from a stream" in {

    val checkpointDir = Files.createTempDirectory("ks")

    val source = spark
      .readStream
      .format("org.apache.spark.sql.datastax.test.monotonic")
      .load()
      .withColumn("value", col("key") + 1)
      .withColumn("key", col("key")) // SparkStreaming seems to rename "key" if we don't do this

    val query = source.writeStream
      .option("checkpointLocation", checkpointDir.toString)
      .cassandraFormat("kv", ks)
      .outputMode(OutputMode.Append())
      .start()

    eventually (timeout(Span(30, Seconds))) {
      val lastBatch = query.lastProgress.batchId
      if (query.exception.nonEmpty) {
        println(query.explain)
        println(query.exception)
      }
      lastBatch should be > 2L
    }

    query.stop()

    val rs = conn.withSessionDo( s => s.execute(s"SELECT Count(*) FROM $ks.kv").all() )
    rs.get(0).getLong(0) should be > 200L
  }


  it should " be able to written to from an empty stream" in {

    val checkpointDir = Files.createTempDirectory("ks")

    val source = spark
      .readStream
      .format("org.apache.spark.sql.datastax.test.empty")
      .load()
      .withColumn("value", col("key") + 1)
      .withColumn("key", col("key")) // SparkStreaming seems to rename "key" if we don't do this

    val query = source.writeStream
      .option("checkpointLocation", checkpointDir.toString)
      .cassandraFormat("empty", ks)
      .outputMode(OutputMode.Append())
      .start()

    eventually (timeout(Span(30, Seconds))) {
      if (query.exception.nonEmpty) {
        println(query.explain)
        println(query.exception)
      }
      val lastBatch = query.lastProgress.batchId
      lastBatch should be > 2L
    }

    query.stop()

    val rs = conn.withSessionDo( s => s.execute(s"SELECT Count(*) FROM $ks.empty").all() )
    rs.get(0).getLong(0) should be (0L)
  }

  it should "write changing window aggregates in update mode" in {
    val checkpointDir = Files.createTempDirectory("ks-update")
    val source = spark
      .readStream
      .format("rate")
      .option("rowsPerSecond", "10")
      .option("numPartitions", "1")
      .load()
      .selectExpr(
        "CAST(value % 2 AS INT) AS content_id",
        "timestamp AS event_time")
      .withWatermark("event_time", "10 minutes")
      .groupBy(window(col("event_time"), "10 minutes", "10 minutes"), col("content_id"))
      .count()
      .selectExpr(
        "content_id",
        "window.start AS bucket_start_time",
        "count AS event_count")

    val query = source.writeStream
      .option("checkpointLocation", checkpointDir.toString)
      .cassandraFormat("update_mode", ks)
      .outputMode(OutputMode.Update())
      .start()

    try {
      eventually(timeout(Span(45, Seconds))) {
        query.exception should be (None)
        val rows = conn.withSessionDo { session =>
          session.execute(s"SELECT content_id, bucket_start_time, event_count FROM $ks.update_mode").all()
        }
        rows.size() should be > 0
        rows.get(0).getLong("event_count") should be > 0L
      }

      val firstCount = conn.withSessionDo { session =>
        session.execute(s"SELECT event_count FROM $ks.update_mode WHERE content_id IN (0, 1)").all().asScala
          .map(_.getLong("event_count"))
          .max
      }

      eventually(timeout(Span(45, Seconds))) {
        query.exception should be (None)
        val latestCount = conn.withSessionDo { session =>
          session.execute(s"SELECT event_count FROM $ks.update_mode WHERE content_id IN (0, 1)").all().asScala
            .map(_.getLong("event_count"))
            .max
        }
        latestCount should be > firstCount
      }
    } finally {
      query.stop()
    }
  }
}
