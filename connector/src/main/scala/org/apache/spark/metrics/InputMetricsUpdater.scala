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

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.LongAdder

import scala.concurrent.duration.{FiniteDuration, _}
import org.apache.spark.TaskContext
import org.apache.spark.executor.InputMetrics
import org.apache.spark.util.ThreadUtils
import _root_.io.teknek.arizona.spark.connector.cql._

import com.datastax.oss.driver.api.core.cql.Row
import _root_.io.teknek.arizona.spark.connector.rdd.ReadConf

/** A trait that provides a method to update read metrics which are collected for connector related tasks.
  * The appropriate instance is created by the companion object.
  *
  * Instances of this trait are not thread-safe. They do not need to because a single instance should be
  * created for each Cassandra read task. This remains valid as long as Cassandra read tasks are
  * single-threaded.
  */
sealed trait InputMetricsUpdater extends MetricsUpdater {
  /** Updates the metrics being collected for the connector after reading each single row. This method
    * is not thread-safe.
    *
    * @param row the row which has just been read
    */
  def updateMetrics(row: Row): Row = row

  /** For internal use only */
  private[metrics] def updateTaskMetrics(count: Long, dataLength: Long): Unit = {}

  /** For internal use only */
  private[metrics] def updateCodahaleMetrics(count: Long, dataLength: Long): Unit = {}
}

object InputMetricsUpdater {
  val DefaultInterval: FiniteDuration = 1.second

  /** Creates the appropriate instance of `InputMetricsUpdater`.
    *
    * If [[io.teknek.arizona.spark.connector.rdd.ReadConf.taskMetricsEnabled ReadConf.taskMetricsEnabled]]
    * is `true`, the created instance will be updating task metrics so
    * that Spark will report them in the UI. Remember that this is supported for Spark 1.2+.
    *
    * If [[org.apache.spark.metrics.CassandraConnectorSource CassandraConnectorSource]]
    * is registered in Spark metrics system, the created instance will be
    * updating the included Codahale metrics. In order to register `CassandraConnectorSource` you need
    * to add it to the metrics configuration file.
    *
    * @param taskContext task context of a task for which this metrics updater is created
    * @param readConf read configuration
    * @param interval allows to update Codahale metrics every the given interval in order to
    *                 decrease overhead
    */
  def apply(
    taskContext: TaskContext,
    readConf: ReadConf,
    interval: FiniteDuration = DefaultInterval
  ): InputMetricsUpdater = {

    val source = MetricsUpdater.getSource(taskContext)

    if (readConf.taskMetricsEnabled) {
      val tm = taskContext.taskMetrics()
      val inputMetrics = tm.inputMetrics

      if (source.isDefined)
        new CodahaleAndTaskMetricsUpdater(interval, source.get, inputMetrics)
      else
        new TaskMetricsUpdater(interval, inputMetrics)

    } else {
      if (source.isDefined)
        new CodahaleMetricsUpdater(interval, source.get)
      else
        new DummyInputMetricsUpdater()
    }
  }

  private abstract class CumulativeInputMetricsUpdater(interval: FiniteDuration)
    extends InputMetricsUpdater with Timer {

    require(interval.length > 0)

    private val cnt = new LongAdder()
    private val dataLength = new LongAdder()
    private val scheduledExecutor = ThreadUtils.newDaemonSingleThreadScheduledExecutor("input-metrics-updater")

    private val updateMetricsCmd = new Runnable {
      private var lastCnt: Long = 0
      private var lastDataLength: Long = 0
      
      override def run(): Unit = {
        // Codahale metrics introduce some overhead so in order to minimize it we can update them not
        // that often
        val (_cnt, _dataLength) = (cnt.sum(), dataLength.sum())
        updateCodahaleMetrics(_cnt - lastCnt, _dataLength - lastDataLength)
        lastCnt = _cnt
        lastDataLength = _dataLength
      }
    }
    
    private val schedule = scheduledExecutor
        .scheduleAtFixedRate(updateMetricsCmd, interval.toMillis, interval.toMillis, TimeUnit.MILLISECONDS)

    override def updateMetrics(row: Row): Row = {
      cnt.increment()
      dataLength.add(getRowBinarySize(row))
      row
    }

    def finish(): Long = {
      val t = stopTimer()
      scheduledExecutor.shutdown()
      scheduledExecutor.awaitTermination(interval.toMillis, TimeUnit.MILLISECONDS)
      updateMetricsCmd.run()
      updateTaskMetrics(cnt.sum(), dataLength.sum())
      t
    }
  }

  private trait CodahaleMetricsSupport extends InputMetricsUpdater {
    val source: CassandraConnectorSource

    @inline
    override def updateCodahaleMetrics(count: Long, dataLength: Long): Unit = {
      source.readByteMeter.mark(dataLength)
      source.readRowMeter.mark(count)
    }

    val timer: com.codahale.metrics.Timer.Context = source.readTaskTimer.time()
  }

  private trait TaskMetricsSupport extends InputMetricsUpdater {
    val inputMetrics: InputMetrics

    @inline
    override def updateTaskMetrics(count: Long, dataLength: Long): Unit = {
      inputMetrics.incBytesRead(dataLength)
      inputMetrics.incRecordsRead(count)
    }
  }

  /** The implementation of [[InputMetricsUpdater]] which does not update anything. */
  private class DummyInputMetricsUpdater extends InputMetricsUpdater with SimpleTimer {
    def finish(): Long = stopTimer()
  }

  /** The implementation of [[InputMetricsUpdater]] which updates only task metrics. */
  private class TaskMetricsUpdater(interval: FiniteDuration, val inputMetrics: InputMetrics)
    extends CumulativeInputMetricsUpdater(interval) with TaskMetricsSupport with SimpleTimer

  /** The implementation of [[InputMetricsUpdater]] which updates only Codahale metrics defined in
    * [[CassandraConnectorSource]]. */
  private class CodahaleMetricsUpdater(interval: FiniteDuration, val source: CassandraConnectorSource)
    extends CumulativeInputMetricsUpdater(interval) with CodahaleMetricsSupport with CCSTimer

  /** The implementation of [[InputMetricsUpdater]] which updates both Codahale and task metrics. */
  private class CodahaleAndTaskMetricsUpdater(
      interval: FiniteDuration,
      val source: CassandraConnectorSource,
      val inputMetrics: InputMetrics)
    extends CumulativeInputMetricsUpdater(interval)
    with TaskMetricsSupport
    with CodahaleMetricsSupport
    with CCSTimer

}
