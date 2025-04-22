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

import java.util.Properties
import java.util.concurrent.{Executors, TimeUnit}
import java.util.function.BiConsumer

import com.codahale.metrics.{Counting, Gauge, Metered, Metric, MetricRegistry, Sampling}
import org.apache.spark.metrics.sink.Sink
import org.apache.spark.{SecurityManager, SparkConf, SparkEnv}
import com.datastax.oss.driver.api.core.cql.{AsyncResultSet, ResultSet}
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.util.Logging
import scala.jdk.CollectionConverters._

class CassandraSink(val properties: Properties, val registry: MetricRegistry, securityMgr: SecurityManager)
  extends Sink with Runnable with Logging {

  val ttl = properties.getProperty("ttl", "15").toInt
  val refreshRate = properties.getProperty("period", "5").toLong

  val executor = Executors.newSingleThreadScheduledExecutor()

  @volatile private var connector: Option[CassandraConnector] = None
  @volatile private var writer: Option[CassandraSink.Writer] = None

  override def start: Unit = {
    logInfo("CassandraSink started")
    executor.scheduleAtFixedRate(this, refreshRate, refreshRate, TimeUnit.SECONDS)
  }

  override def stop: Unit = {
    logInfo("CassandraSink finished")
    executor.shutdown()
  }

  override def run(): Unit = {
    report()
  }

  val warnOnError = new BiConsumer[AsyncResultSet, Throwable] {
    override def accept(s: AsyncResultSet, t: Throwable): Unit =
      Option(t).foreach(_ => logWarning(s"Metrics write failed. The exception was: ${t.getMessage}"))
  }

  override def report(): Unit = {
    lazy val conf = getSparkConf
    for (connector <- tryGetOrCreateConnector(conf); writer <- tryGetOrCreateWriter(conf)) {
      logDebug("Generating snapshot")
      connector.withSessionDo { session =>
        val stmt = session.prepare(writer.insertStatement)

        for ((MetricName(appId, componentId, metricId), metric) <- registry.getMetrics.asScala.iterator) {
          val bndStmt = stmt.bind(writer.build(componentId, metricId, metric): _*)
          session.executeAsync(bndStmt).whenComplete(warnOnError)
        }
      }
    }
  }

  object MetricName {
    // TODO consider improving performance of this method if it turns out slow
    val executorPattern =
      """([^\.]+?)\.(\d+?)\.([^\.]+?)\.(.*)""".r
    val otherComponentPattern = """([^\.]+?)\.([^\.]+?)\.(.*)""".r

    def unapply(metricName: String): Option[(String, String, String)] = {
      metricName match {
        case executorPattern(appId, componentNumber, componentName, metricId) =>
          Some((appId, s"$componentName-$componentNumber", metricId))
        case otherComponentPattern(appId, componentId, metricId) =>
          Some((appId, componentId, metricId))
        case _ =>
          logError(s"Unrecognized metric name: $metricName")
          None
      }
    }
  }

  def getSparkConf = {
    Option(SparkEnv.get).map { env => env.conf.clone() }
  }

  def tryGetOrCreateConnector(sparkConfOpt: => Option[SparkConf]) = {
    if (connector.isEmpty) {
      for (sparkConf <- sparkConfOpt) {
        connector = Some(CassandraConnector(sparkConf))
      }
    }
    connector
  }

  def tryGetOrCreateWriter(sparkConfOpt: => Option[SparkConf]) = {
    if (writer.isEmpty) {
      for (sparkConf <- sparkConfOpt) {
        writer = Some(new CassandraSink.Writer(sparkConf.getAppId, ttl))
      }
    }
    writer
  }
}

object CassandraSink {
  val TableName = "spark_apps_snapshot"
  val DSE_PERF_KEYSPACE = "dse_perf"

  object Fields extends Enumeration {
    val APPLICATION_ID = Value("application_id")
    val COMPONENT_ID = Value("component_id")
    val METRIC_ID = Value("metric_id")
    val METRIC_TYPE = Value("metric_type")
    val COUNT = Value("count")
    val RATE_1_MIN = Value("rate_1_min")
    val RATE_5_MIN = Value("rate_5_min")
    val RATE_15_MIN = Value("rate_15_min")
    val RATE_MEAN = Value("rate_mean")
    val SS_999TH = Value("snapshot_999th_percentile")
    val SS_99TH = Value("snapshot_99th_percentile")
    val SS_98TH = Value("snapshot_98th_percentile")
    val SS_95TH = Value("snapshot_95th_percentile")
    val SS_75TH = Value("snapshot_75th_percentile")
    val SS_MEDIAN = Value("snapshot_median")
    val SS_MEAN = Value("snapshot_mean")
    val SS_MIN = Value("snapshot_min")
    val SS_MAX = Value("snapshot_max")
    val SS_STDDEV = Value("snapshot_stddev")
    val VALUE = Value("value")
  }

  class Writer(appId: String, ttl: Int) {

    val insertStatement =
      s"""
         |INSERT INTO "$DSE_PERF_KEYSPACE"."$TableName" (${Fields.values.mkString(", ")})
         |VALUES (${(1 to Fields.values.size).map(_ => "?").mkString(", ")})
         |USING TTL $ttl
         |""".stripMargin

    def build(componentId: String, metricId: String, metric: Metric): Array[AnyRef] = {
      val metricType = if (metric.isInstanceOf[Gauge[_]])
        "Gauge" else metric.getClass.getSimpleName

      val buf = Array.fill[AnyRef](Fields.values.size)(null)
      buf(Fields.APPLICATION_ID.id) = appId
      buf(Fields.COMPONENT_ID.id) = componentId
      buf(Fields.METRIC_ID.id) = metricId
      buf(Fields.METRIC_TYPE.id) = metricType

      setAllFields(buf, metric)

      buf
    }

    private def setAllFields(buf: Array[AnyRef], metric: Metric) = {
      countingFields(buf, metric)
      meteredFields(buf, metric)
      gaugeFields(buf, metric)
      samplingFields(buf, metric)
    }

    private def countingFields(buf: Array[AnyRef], metric: Metric) = metric match {
      case counting: Counting =>
        buf(Fields.COUNT.id) = counting.getCount.asInstanceOf[AnyRef]
      case _ =>
    }

    private def meteredFields(buf: Array[AnyRef], metric: Metric) = metric match {
      case metered: Metered =>
        buf(Fields.RATE_1_MIN.id) = metered.getOneMinuteRate.asInstanceOf[AnyRef]
        buf(Fields.RATE_5_MIN.id) = metered.getFiveMinuteRate.asInstanceOf[AnyRef]
        buf(Fields.RATE_15_MIN.id) = metered.getFifteenMinuteRate.asInstanceOf[AnyRef]
        buf(Fields.RATE_MEAN.id) = metered.getMeanRate.asInstanceOf[AnyRef]
      case _ =>
    }

    private def gaugeFields(buf: Array[AnyRef], metric: Metric) = metric match {
      case gauge: Gauge[_] =>
        buf(Fields.VALUE.id) = String.valueOf(gauge.getValue)
      case _ =>
    }

    private def samplingFields(buf: Array[AnyRef], metric: Metric) = metric match {
      case sampling: Sampling =>
        buf(Fields.SS_MIN.id) = sampling.getSnapshot.getMin.asInstanceOf[AnyRef]
        buf(Fields.SS_MAX.id) = sampling.getSnapshot.getMax.asInstanceOf[AnyRef]
        buf(Fields.SS_MEAN.id) = sampling.getSnapshot.getMean.asInstanceOf[AnyRef]
        buf(Fields.SS_STDDEV.id) = sampling.getSnapshot.getStdDev.asInstanceOf[AnyRef]
        buf(Fields.SS_MEDIAN.id) = sampling.getSnapshot.getMedian.asInstanceOf[AnyRef]
        buf(Fields.SS_75TH.id) = sampling.getSnapshot.get75thPercentile().asInstanceOf[AnyRef]
        buf(Fields.SS_95TH.id) = sampling.getSnapshot.get95thPercentile().asInstanceOf[AnyRef]
        buf(Fields.SS_98TH.id) = sampling.getSnapshot.get98thPercentile().asInstanceOf[AnyRef]
        buf(Fields.SS_99TH.id) = sampling.getSnapshot.get99thPercentile().asInstanceOf[AnyRef]
        buf(Fields.SS_999TH.id) = sampling.getSnapshot.get999thPercentile().asInstanceOf[AnyRef]
      case _ =>
    }
  }

}
