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

package io.teknek.arizona.spark.connector.embedded

import java.nio.file.Files

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf

object SparkTemplate {

  val DefaultParallelism = 2

  /** Default configuration for [[org.apache.spark.SparkContext SparkContext]]. */
  private val _defaultConf = new SparkConf(true)
    .set("spark.cassandra.connection.keepAliveMS", "5000")
    .set("spark.cassandra.connection.timeoutMS", "30000")
    .set("spark.ui.showConsoleProgress", "false")
    .set("spark.ui.enabled", "false")
    .set("spark.cleaner.ttl", "3600")
    .set("spark.sql.extensions","io.teknek.arizona.spark.connector.CassandraSparkExtensions")
    .setMaster(sys.env.getOrElse("IT_TEST_SPARK_MASTER", s"local[$DefaultParallelism]"))
    .setAppName("Test")


  def defaultConf = _defaultConf.clone()

  def withoutLogging[T]( f: => T): T={
    val level = Logger.getRootLogger.getLevel
    Logger.getRootLogger.setLevel(Level.OFF)
    val ret = f
    Logger.getRootLogger.setLevel(level)
    ret
  }

}
