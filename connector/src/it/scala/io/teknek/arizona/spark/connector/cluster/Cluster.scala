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

package io.teknek.arizona.spark.connector.cluster

import java.net.InetSocketAddress

import com.datastax.oss.driver.api.core.Version
import _root_.io.teknek.arizona.spark.connector.ccm.{CcmBridge, CcmConfig}

/** Cluster facade used by test code. */
case class Cluster(
    name: String,
    addresses: Seq[InetSocketAddress],
    private val closeCluster: () => Unit,
    private val flushNode: Int => Unit,
    private val refreshSizeEstimatesForNode: Int => Unit,
    private val cassandraVersion: () => Version,
    private val dseVersion: () => Option[Version],
    private val nodeConnectionParams: InetSocketAddress => Map[String, String]) {

  def close(): Unit = closeCluster()

  def flush(): Unit = addresses.indices.foreach(i => flushNode(i + 1))

  def getCassandraVersion: Version = cassandraVersion()
  def getDseVersion: Option[Version] = dseVersion()

  def refreshSizeEstimates(): Unit = {
    flush()
    addresses.indices.foreach(i => refreshSizeEstimatesForNode(i + 1))
  }

  def getConnectionHost: String = addresses.head.getHostName

  def getConnectionPort: String = addresses.head.getPort.toString

  def connectionParameters: Map[String, String] = {
    connectionParameters(nodeNo = 0)
  }

  def connectionParameters(nodeNo: Int): Map[String, String] = {
    if (nodeNo >= 0 && nodeNo < addresses.size)
      nodeConnectionParams(addresses(nodeNo))
    else
      throw new IllegalArgumentException(s"Cluster $name has ${addresses.size} nodes, node $nodeNo does not exist.")
  }
}

object Cluster {

  def fromCcm(
      name: String,
      config: CcmConfig,
      ccmBridge: CcmBridge,
      nodeConnectionParams: InetSocketAddress => Map[String, String]): Cluster = {
    Cluster(
      name,
      config.nodeAddresses(),
      () => ccmBridge.close(),
      ccmBridge.flush,
      ccmBridge.refreshSizeEstimates,
      () => ccmBridge.getCassandraVersion,
      () => ccmBridge.getDseVersion,
      nodeConnectionParams)
  }
}
