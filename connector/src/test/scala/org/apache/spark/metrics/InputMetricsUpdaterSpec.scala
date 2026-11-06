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

import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.duration._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.metrics.source.Source
import org.apache.spark.{SparkConf, TaskContext}
import org.mockito.Mockito._
import org.scalatest.concurrent.Eventually
import org.scalatest.{FlatSpec, Matchers}
import com.datastax.spark.connector.test.RowMock
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector.util.RuntimeUtil.toParallelIterable
import org.scalatestplus.mockito.MockitoSugar

class InputMetricsUpdaterSpec extends FlatSpec with Matchers with MockitoSugar {

  implicit val defaultPatienceConfig = Eventually.PatienceConfig(
    Eventually.scaled(20.seconds), Eventually.scaled(500.milliseconds))

  private def newTaskContext(useTaskMetrics: Boolean = true)(sources: Source*): TaskContext = {
    val tc = mock[TaskContext]
    if (useTaskMetrics) {
      when(tc.taskMetrics()) thenReturn new TaskMetrics
    }
    when(tc.getMetricsSources(MetricsUpdater.cassandraConnectorSourceName)) thenReturn sources
    tc
  }

  it should "create updater which uses task metrics" in {
    val tc = newTaskContext()()
    val conf = new SparkConf(loadDefaults = false)
        .set("spark.cassandra.input.metrics", "true")
    val updater = InputMetricsUpdater(tc, ReadConf.fromSparkConf(conf))
    val row = new RowMock(Some(1), Some(2), Some(3), None, Some(4))

    updater.updateMetrics(row)
    tc.taskMetrics().inputMetrics.bytesRead shouldBe 0L
    tc.taskMetrics().inputMetrics.recordsRead shouldBe 0L

    updater.updateMetrics(row)
    updater.updateMetrics(row)
    updater.updateMetrics(row)
    tc.taskMetrics().inputMetrics.bytesRead shouldBe 0L
    tc.taskMetrics().inputMetrics.recordsRead shouldBe 0L

    updater.finish()
    tc.taskMetrics().inputMetrics.bytesRead shouldBe 40L
    tc.taskMetrics().inputMetrics.recordsRead shouldBe 4L
  }

  it should "be thread-safe when it uses task metrics" in {
    val tc = newTaskContext()()
    val conf = new SparkConf(loadDefaults = false)
        .set("spark.cassandra.input.metrics", "true")
    val updater = InputMetricsUpdater(tc, ReadConf.fromSparkConf(conf))
    val row = new RowMock(Some(1), Some(2), Some(3), None, Some(4))

    val range = toParallelIterable(1 to 1000).par
    range.tasksupport = new ForkJoinTaskSupport(new java.util.concurrent.ForkJoinPool(10))
    for (i <- range) updater.updateMetrics(row)
    updater.finish()
    tc.taskMetrics().inputMetrics.bytesRead shouldBe 10000L
    tc.taskMetrics().inputMetrics.recordsRead shouldBe 1000L
  }

  it should "create updater which does not use task metrics" in {
    val tc = newTaskContext(useTaskMetrics = false)()
    val conf = new SparkConf(loadDefaults = false)
        .set("spark.cassandra.input.metrics", "false")
    val updater = InputMetricsUpdater(tc, ReadConf.fromSparkConf(conf))
    val row = new RowMock(Some(1), Some(2), Some(3), None, Some(4))

    updater.updateMetrics(row)

    verify(tc, never).taskMetrics()
  }

  it should "create updater which uses Codahale metrics" in {
    val ccs = new CassandraConnectorSource
    val tc = newTaskContext()(ccs)
    val conf = new SparkConf(loadDefaults = false)
    val updater = InputMetricsUpdater(tc, ReadConf.fromSparkConf(conf))
    val row = new RowMock(Some(1), Some(2), Some(3), None, Some(4))

    updater.updateMetrics(row)
    Eventually.eventually {
      ccs.readRowMeter.getCount shouldBe 0
      ccs.readByteMeter.getCount shouldBe 0
    }

    updater.updateMetrics(row)
    updater.updateMetrics(row)
    updater.updateMetrics(row)

    Eventually.eventually {
      ccs.readRowMeter.getCount shouldBe 4L
      ccs.readByteMeter.getCount shouldBe 40L
    }

    updater.finish()
    ccs.readTaskTimer.getCount shouldBe 1L
  }

  it should "be thread-safe when using Codahale metrics" in {
    val ccs = new CassandraConnectorSource
    val tc = newTaskContext()(ccs)
    val conf = new SparkConf(loadDefaults = false)
    val updater = InputMetricsUpdater(tc, ReadConf.fromSparkConf(conf))
    val row = new RowMock(Some(1), Some(2), Some(3), None, Some(4))

    ccs.readRowMeter.getCount shouldBe 0
    ccs.readByteMeter.getCount shouldBe 0

    val range = toParallelIterable(1 to 1000).par
    range.tasksupport = new ForkJoinTaskSupport(new java.util.concurrent.ForkJoinPool(10))
    for (i <- range) updater.updateMetrics(row)
    updater.finish()

    ccs.readRowMeter.getCount shouldBe 1000L
    ccs.readByteMeter.getCount shouldBe 10000L
    ccs.readTaskTimer.getCount shouldBe 1L
  }

  it should "create updater which doesn't use Codahale metrics" in {
    val tc = newTaskContext()()
    val conf = new SparkConf(loadDefaults = false)
    val updater = InputMetricsUpdater(tc, ReadConf.fromSparkConf(conf))
    val row = new RowMock(Some(1), Some(2), Some(3), None, Some(4))

    updater.updateMetrics(row)
    updater.updateMetrics(row)
    updater.updateMetrics(row)
    updater.updateMetrics(row)

    updater.finish()
  }
}
