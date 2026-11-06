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

import com.codahale.metrics.Timer.Context
import _root_.io.teknek.arizona.spark.connector.util.Logging
import _root_.io.teknek.arizona.spark.connector.writer.{RichStatement, WriteConf}
import org.apache.spark.TaskContext
import org.apache.spark.executor.OutputMetrics

/** A trait that provides a method to update write metrics which are collected for connector related tasks.
  * The appropriate instance is created by the companion object.
  *
  * Instances of `OutputMetricsUpdater` are thread-safe, because the Cassandra write task implementation
  * is multi-threaded.
  */
sealed trait OutputMetricsUpdater extends MetricsUpdater {
  /** Updates the metrics being collected for the connector after writing each single statement.
    * This method is thread-safe.
    *
    * @param success whether the statement succeeded or not
    * @param stmt the statement - either bound or batch
    * @param submissionTimestamp the time when the write task has been submitted
    * @param executionTimestamp the time when the write task has been really executed
    */
  def batchFinished(
    success: Boolean,
    stmt: RichStatement,
    submissionTimestamp: Long,
    executionTimestamp: Long
  ): Unit = {}

  /** For internal use only */
  private[metrics] def updateTaskMetrics(success: Boolean, count: Int, dataLength: Int): Unit = {}

  /** For internal use only */
  private[metrics] def updateCodahaleMetrics(
    success: Boolean,
    count: Int,
    dataLength: Int,
    submissionTimestamp: Long,
    executionTimestamp: Long
  ): Unit = {}

}

object OutputMetricsUpdater extends Logging {

  /** Creates the appropriate instance of `OutputMetricsUpdater`.
    *
    * If [[io.teknek.arizona.spark.connector.writer.WriteConf.taskMetricsEnabled WriteConf.taskWriteMetricsEnabled]]
    * is `true`, the created instance will be updating task metrics so
    * that Spark will report them in the UI. Remember that this is supported for Spark 1.2+.
    *
    * If [[org.apache.spark.metrics.CassandraConnectorSource CassandraConnectorSource]] is registered
    * in Spark metrics system, the created instance will be
    * updating the included Codahale metrics. In order to register `CassandraConnectorSource` you need
    * to add it to the metrics configuration file.
    *
    * @param taskContext task context of a task for which this metrics updater is created
    * @param writeConf write configuration
    */
  def apply(taskContext: TaskContext, writeConf: WriteConf): OutputMetricsUpdater = {
    val source = MetricsUpdater.getSource(taskContext)

    if (writeConf.taskMetricsEnabled) {
      val tm = taskContext.taskMetrics()

      if (source.isDefined)
        new CodahaleAndTaskMetricsUpdater(source.get, tm.outputMetrics)
      else
        new TaskMetricsUpdater(tm.outputMetrics)

    } else {
      if (source.isDefined)
        new CodahaleMetricsUpdater(source.get)
      else
        new DummyOutputMetricsUpdater
    }
  }

  private abstract class BaseOutputMetricsUpdater
    extends OutputMetricsUpdater with Timer {

    override def batchFinished(
      success: Boolean,
      stmt: RichStatement,
      submissionTimestamp: Long,
      executionTimestamp: Long
    ): Unit = {

      val dataLength = stmt.bytesCount
      val rowsCount = stmt.rowsCount
      updateTaskMetrics(success, rowsCount, dataLength)
      updateCodahaleMetrics(success, rowsCount, dataLength, submissionTimestamp, executionTimestamp)
    }

    def finish(): Long = stopTimer()
  }

  private trait TaskMetricsSupport extends OutputMetricsUpdater {
    val outputMetrics: OutputMetrics

    val dataLengthCounter = new LongAdder
    val rowsCounter = new LongAdder

    dataLengthCounter.add(outputMetrics.bytesWritten)
    rowsCounter.add(outputMetrics.recordsWritten)

    override private[metrics] def updateTaskMetrics(success: Boolean, count: Int, dataLength: Int): Unit = {
      if (success) {
        dataLengthCounter.add(dataLength)
        rowsCounter.add(count)
        outputMetrics.setBytesWritten(dataLengthCounter.longValue())
        outputMetrics.setRecordsWritten(rowsCounter.longValue())
      }
    }
  }

  private trait CodahaleMetricsSupport extends OutputMetricsUpdater {
    val source: CassandraConnectorSource

    override private[metrics] def updateCodahaleMetrics(success: Boolean, count: Int, dataLength: Int,
      submissionTimestamp: Long, executionTimestamp: Long): Unit = {

      if (success) {
        val t = System.nanoTime()
        source.writeBatchTimer.update(t - executionTimestamp, TimeUnit.NANOSECONDS)
        source.writeBatchWaitTimer.update(executionTimestamp - submissionTimestamp, TimeUnit.NANOSECONDS)
        source.writeBatchSizeHistogram.update(count)
        source.writeRowMeter.mark(count)
        source.writeByteMeter.mark(dataLength)
        source.writeSuccessCounter.inc()

      } else {
        source.writeFailureCounter.inc()
      }
    }

    val timer: Context = source.writeTaskTimer.time()
  }

  /** The implementation of [[OutputMetricsUpdater]] which does not update anything. */
  private class DummyOutputMetricsUpdater extends OutputMetricsUpdater with SimpleTimer {
    def finish(): Long = stopTimer()
  }

  /** The implementation of [[OutputMetricsUpdater]] which updates only task metrics. */
  private class TaskMetricsUpdater(val outputMetrics: OutputMetrics)
    extends BaseOutputMetricsUpdater with TaskMetricsSupport with SimpleTimer {
  }

  /** The implementation of [[OutputMetricsUpdater]] which updates only Codahale metrics defined in
    * [[CassandraConnectorSource]]. */
  private class CodahaleMetricsUpdater(val source: CassandraConnectorSource)
    extends BaseOutputMetricsUpdater with CodahaleMetricsSupport with CCSTimer

  /** The implementation of [[OutputMetricsUpdater]] which updates both Codahale and task metrics. */
  private class CodahaleAndTaskMetricsUpdater(
      val source: CassandraConnectorSource,
      val outputMetrics: OutputMetrics)
    extends BaseOutputMetricsUpdater with TaskMetricsSupport with CodahaleMetricsSupport with CCSTimer {
  }

}
