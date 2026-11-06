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

package io.teknek.arizona.spark.connector.rdd

import com.datastax.oss.driver.api.core.{ConsistencyLevel, DefaultConsistencyLevel}
import _root_.io.teknek.arizona.spark.connector.util.{ConfigCheck, ConfigParameter, DeprecatedConfigParameter, Logging}
import _root_.io.teknek.arizona.spark.connector.writer.WriteConf.{ReferenceSection, ThroughputMiBPSParam}
import org.apache.spark.SparkConf

/** Read settings for RDD
  *
  * @param splitCount number of partitions to divide the data into; unset by default
  * @param splitSizeInMB size of Cassandra data to be read in a single Spark task; 
  *                      determines the number of partitions, but ignored if `splitCount` is set
  * @param fetchSizeInRows number of CQL rows to fetch in a single round-trip to Cassandra
  * @param consistencyLevel consistency level for reads, default LOCAL_ONE;
  *                         higher consistency level will disable data-locality
  * @param taskMetricsEnabled whether or not enable task metrics updates (requires Spark 1.2+)
  * @param readsPerSec maximum read throughput allowed per single core in requests/s while
  *                                  joining an RDD with C* table (joinWithCassandraTable operation)
  *                                  also used by enterprise integrations*/
case class ReadConf(
  splitCount: Option[Int] = None,
  splitSizeInMB: Int = ReadConf.SplitSizeInMBParam.default,
  fetchSizeInRows: Int = ReadConf.FetchSizeInRowsParam.default,
  consistencyLevel: ConsistencyLevel = ReadConf.ConsistencyLevelParam.default,
  taskMetricsEnabled: Boolean = ReadConf.TaskMetricParam.default,
  throughputMiBPS: Option[Double] = None,
  readsPerSec: Option[Int] = ReadConf.ReadsPerSecParam.default,
  parallelismLevel: Int = ReadConf.ParallelismLevelParam.default,
  executeAs: Option[String] = None)


object ReadConf extends Logging {
  val ReferenceSection = "Read Tuning Parameters"

  val ThroughputMiBPSParam = ConfigParameter[Option[Double]] (
    name = "spark.cassandra.input.throughputMBPerSec",
    section = ReferenceSection,
    default = None,
    description = """*(Floating points allowed)* <br> Maximum read throughput allowed
                    | per single core in MB/s. Effects point lookups as well as full
                    | scans.""".stripMargin)

  val SplitSizeInMBParam = ConfigParameter[Int](
    name = "spark.cassandra.input.split.sizeInMB",
    section = ReferenceSection,
    default = 512,
    description =
      """Approx amount of data to be fetched into a Spark partition. Minimum number of resulting Spark
        | partitions is <code>1 + 2 * SparkContext.defaultParallelism</code>
        |""".stripMargin.filter(_ >= ' '))

  val DeprecatedSplitSizeInMBParam = DeprecatedConfigParameter(
    name = "spark.cassandra.input.split.size_in_mb",
    replacementParameter = Some(SplitSizeInMBParam),
    deprecatedSince = "DSE 6.0.0"
  )

  val FetchSizeInRowsParam = ConfigParameter[Int](
    name = "spark.cassandra.input.fetch.sizeInRows",
    section = ReferenceSection,
    default = 1000,
    description = """Number of CQL rows fetched per driver request""")

  val DeprecatedFetchSizeInRowsParam = DeprecatedConfigParameter(
    name = "spark.cassandra.input.fetch.size_in_rows",
    replacementParameter = Some(FetchSizeInRowsParam),
    deprecatedSince = "DSE 6.0.0"
  )

  val ConsistencyLevelParam = ConfigParameter[ConsistencyLevel](
    name = "spark.cassandra.input.consistency.level",
    section = ReferenceSection,
    default = DefaultConsistencyLevel.LOCAL_ONE,
    description = """Consistency level to use when reading	""")

  val TaskMetricParam = ConfigParameter[Boolean](
    name = "spark.cassandra.input.metrics",
    section = ReferenceSection,
    default = true,
    description = """Sets whether to record connector specific metrics on write"""
  )

  val ReadsPerSecParam = ConfigParameter[Option[Int]] (
    name = "spark.cassandra.input.readsPerSec",
    section = ReferenceSection,
    default = None,
    description =
      """Sets max requests or pages per core per second, unlimited by default."""
  )

  val DeprecatedReadsPerSecParam = DeprecatedConfigParameter(
    name = "spark.cassandra.input.reads_per_sec",
    replacementParameter = Some(ReadsPerSecParam),
    deprecatedSince = "DSE 6.0.0"
  )

  val ThroughputJoinQueryPerSecParam = DeprecatedConfigParameter (
    name = "spark.cassandra.input.join.throughput_query_per_sec",
    replacementParameter = Some(ReadsPerSecParam),
    deprecatedSince = "DSE 5.1.5")


  val ParallelismLevelParam = ConfigParameter[Int] (
    name = "spark.cassandra.concurrent.reads",
    section = ReferenceSection,
    default = 512,
    description =
      """Sets read parallelism for joinWithCassandra tables"""
  )

  def fromSparkConf(conf: SparkConf): ReadConf = {

    ConfigCheck.checkConfig(conf)

    ReadConf(
      fetchSizeInRows = conf.getInt(FetchSizeInRowsParam.name, FetchSizeInRowsParam.default),
      splitSizeInMB = conf.getInt(SplitSizeInMBParam.name, SplitSizeInMBParam.default),
      consistencyLevel = DefaultConsistencyLevel.valueOf(
        conf.get(ConsistencyLevelParam.name, ConsistencyLevelParam.default.name)),
      taskMetricsEnabled = conf.getBoolean(TaskMetricParam.name, TaskMetricParam.default),
      throughputMiBPS = conf.getOption(ThroughputMiBPSParam.name).map(_.toDouble),
      readsPerSec = conf.getOption(ReadsPerSecParam.name).map(_.toInt),
      parallelismLevel = conf.getInt(ParallelismLevelParam.name, ParallelismLevelParam.default)
    )
  }

}

