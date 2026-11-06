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

package org.apache.spark.metrics

import java.util.concurrent.CountDownLatch

import _root_.io.teknek.arizona.spark.connector.writer.RichStatement.DriverStatement
import _root_.io.teknek.arizona.spark.connector.writer.{RichStatement, WriteConf}
import org.apache.spark.executor.TaskMetrics
import _root_.org.apache.spark.metrics.source.Source
import org.apache.spark.{SparkConf, TaskContext}
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

class OutputMetricsUpdaterSpec extends FlatSpec with Matchers with MockitoSugar {

  val ts = System.currentTimeMillis()

  private def newTaskContext(useTaskMetrics: Boolean = true)(sources: Source*): TaskContext = {
    val tc = mock[TaskContext]
    if (useTaskMetrics) {
      when(tc.taskMetrics()) thenReturn new TaskMetrics
    }
    when(tc.getMetricsSources(MetricsUpdater.cassandraConnectorSourceName)) thenReturn sources
    tc
  }

  private def newRichStatement(): RichStatement = {
    new RichStatement() {
      override val bytesCount = 100
      override val rowsCount = 10
      override def stmt: DriverStatement = ???
      override def executeAs(executeAs: Option[String]): RichStatement = ???
    }
  }

  it should "create updater which uses task metrics" in {
    val tc = newTaskContext()()
    val conf = new SparkConf(loadDefaults = false)
      .set("spark.cassandra.output.metrics", "true")
    val updater = OutputMetricsUpdater(tc, WriteConf.fromSparkConf(conf))
    val rc = newRichStatement()

    updater.batchFinished(success = true, rc, ts, ts)
    tc.taskMetrics().outputMetrics.bytesWritten shouldBe 100L // change registered when success
    tc.taskMetrics().outputMetrics.recordsWritten shouldBe 10L

    updater.batchFinished(success = false, rc, ts, ts)
    tc.taskMetrics().outputMetrics.bytesWritten shouldBe 100L // change not regsitered when failure
    tc.taskMetrics().outputMetrics.recordsWritten shouldBe 10L
  }

  it should "create updater which does not use task metrics" in {
    val tc = newTaskContext(useTaskMetrics = false)()
    val conf = new SparkConf(loadDefaults = false)
      .set("spark.cassandra.output.metrics", "false")
    val updater = OutputMetricsUpdater(tc, WriteConf.fromSparkConf(conf))
    val rc = newRichStatement()

    updater.batchFinished(success = true, rc, ts, ts)
    updater.batchFinished(success = false, rc, ts, ts)

    verify(tc, never).taskMetrics()
  }

  it should "create updater which uses Codahale metrics" in {
    val ccs = new CassandraConnectorSource
    val tc = newTaskContext()(ccs)
    val conf = new SparkConf(loadDefaults = false)
    val updater = OutputMetricsUpdater(tc, WriteConf.fromSparkConf(conf))
    val rc = newRichStatement()

    updater.batchFinished(success = true, rc, ts, ts)
    ccs.writeRowMeter.getCount shouldBe 10L
    ccs.writeByteMeter.getCount shouldBe 100L
    ccs.writeSuccessCounter.getCount shouldBe 1L
    ccs.writeFailureCounter.getCount shouldBe 0L
    ccs.writeBatchSizeHistogram.getSnapshot.getMedian shouldBe 10.0
    ccs.writeBatchSizeHistogram.getCount shouldBe 1L

    updater.batchFinished(success = false, rc, ts, ts)
    ccs.writeRowMeter.getCount shouldBe 10L
    ccs.writeByteMeter.getCount shouldBe 100L
    ccs.writeSuccessCounter.getCount shouldBe 1L
    ccs.writeFailureCounter.getCount shouldBe 1L
    ccs.writeBatchSizeHistogram.getCount shouldBe 1L

    updater.finish()
    ccs.writeTaskTimer.getCount shouldBe 1L
  }

  it should "create updater which doesn't use Codahale metrics" in {
    val tc = newTaskContext()()
    val conf = new SparkConf(loadDefaults = false)
    val updater = OutputMetricsUpdater(tc, WriteConf.fromSparkConf(conf))
    val rc = newRichStatement()

    updater.batchFinished(success = true, rc, ts, ts)
    updater.batchFinished(success = false, rc, ts, ts)

    updater.finish()
  }

  it should "work correctly with multiple threads" in {
    val tc = newTaskContext()()
    val conf = new SparkConf(loadDefaults = false)
      .set("spark.cassandra.output.metrics", "true")
    val updater = OutputMetricsUpdater(tc, WriteConf.fromSparkConf(conf))
    val rc = newRichStatement()

    val latch = new CountDownLatch(32)
    class TestThread extends Thread {
      override def run(): Unit = {
        latch.countDown()
        latch.await()
        for (i <- 1 to 100000)
          updater.batchFinished(success = true, rc, ts, ts)
      }
    }

    val threads = Array.fill(32)(new TestThread)
    threads.foreach(_.start())
    threads.foreach(_.join())

    tc.taskMetrics().outputMetrics.bytesWritten shouldBe 320000000L
    tc.taskMetrics().outputMetrics.recordsWritten shouldBe 32000000L
  }

}
