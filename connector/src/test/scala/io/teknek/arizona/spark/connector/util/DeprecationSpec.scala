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

package io.teknek.arizona.spark.connector.util

import _root_.io.teknek.arizona.spark.connector.TableRef
import _root_.io.teknek.arizona.spark.connector.cql.{CassandraConnector, CassandraConnectorConf}
import _root_.io.teknek.arizona.spark.connector.datasource.CassandraSourceUtil
import _root_.io.teknek.arizona.spark.connector.rdd.ReadConf
import _root_.io.teknek.arizona.spark.connector.types.ColumnTypeConf
import _root_.io.teknek.arizona.spark.connector.writer.WriteConf
import org.apache.spark.SparkConf
import _root_.org.apache.spark.sql.cassandra.CassandraSourceRelation
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class DeprecationSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  val conf = new SparkConf()

  //Cassandra Connector Conf
  conf.set(CassandraConnectorConf.DeprecatedConnectionTimeoutParam.name, "2500")
  conf.set(CassandraConnectorConf.DeprecatedKeepAliveMillisParam.name, "2500")
  conf.set(CassandraConnectorConf.DeprecatedLocalDCParam.name, "otherdc")
  conf.set(CassandraConnectorConf.DeprecatedMaxReconnectionDelayParam.name, "2500")
  conf.set(CassandraConnectorConf.DeprecatedMinReconnectionDelayParam.name, "2500")
  conf.set(CassandraConnectorConf.DeprecatedReadTimeoutParam.name, "2500")

  //Read Conf
  conf.set(ReadConf.DeprecatedFetchSizeInRowsParam.name, "2500")
  conf.set(ReadConf.DeprecatedReadsPerSecParam.name, "2500")
  conf.set(ReadConf.DeprecatedSplitSizeInMBParam.name, "2500")

  //Write Conf
  conf.set(WriteConf.DeprecatedThroughputMiBPSParam.name, "2500")

  //CassandraSourceRelation
  conf.set(CassandraSourceRelation.SolrPredciateOptimizationParam.name, "true")


  "The Spark Cassandra Connector" should "deprecate ConnectionConf" in {
    val cassandraConnectorConf = CassandraConnectorConf(conf)
    cassandraConnectorConf.connectTimeoutMillis should be(2500)
    cassandraConnectorConf.keepAliveMillis should be (2500)
    cassandraConnectorConf.localDC should be (Some("otherdc"))
    cassandraConnectorConf.maxReconnectionDelayMillis should be (2500)
    cassandraConnectorConf.minReconnectionDelayMillis should be (2500)
    cassandraConnectorConf.readTimeoutMillis should be (2500)
  }

  it should "deprecate ReadConf" in {
    val readConf = ReadConf.fromSparkConf(conf)
    readConf.fetchSizeInRows should be (2500)
    readConf.readsPerSec.get should be (2500)
    readConf.splitSizeInMB should be (2500)
  }

  it should "deprecate WriteConf" in {
    val writeConf = WriteConf.fromSparkConf(conf)
    writeConf.throughputMiBPS should be (Some(2500.0))
  }

  it should "deprecate SoureConf" in {
    val consolidateConf = CassandraSourceUtil.consolidateConfs(conf, Map.empty, "default", "ks")
    consolidateConf.get(CassandraSourceRelation.SolrPredciateOptimizationParam.replacementParameter.get.name) should be ("true")
  }

}
