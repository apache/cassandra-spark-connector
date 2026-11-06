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

package io.teknek.arizona.spark.connector.rdd.partitioner

import java.net.InetAddress

import _root_.io.teknek.arizona.spark.connector.cql.CassandraConnector
import _root_.io.teknek.arizona.spark.connector.util.DriverUtil.{toName, toOption}
import scala.jdk.CollectionConverters._

/** Looks up listen address of a cluster node given its Native Transport address.
  * Uses system.peers table as the source of information.
  * If such information for a node is missing, it assumes its listen
  * address equals its RPC address */
class NodeAddresses(conn: CassandraConnector) extends Serializable {

  /** Maps native transport addresses to listen addresses for every cluster node.
    * If native transport address is not known, returns the same address. */
  lazy val nativeTransportAddressToListenAddress: InetAddress => InetAddress = {
    conn.withSessionDo { session =>
      val table = "system.peers"
      val listenAddressColumnName = "peer"

      val transportAddressColumn = toOption(session.getMetadata.getKeyspace("system"))
        .flatMap(k => toOption(k.getTable("peers")))
        .flatMap(t => toOption(t.getColumn("native_transport_address")))

      val nativeTransportAddressColumnName = transportAddressColumn.map(c => toName(c.getName)).getOrElse("rpc_address")

      // TODO: fetch information about the local node from system.local, when CASSANDRA-9436 is done
      val rs = session.execute(s"SELECT $nativeTransportAddressColumnName, $listenAddressColumnName FROM $table")
      for {
        row <- rs.all().asScala
        nativeTransportAddress <- Option(row.getInetAddress(nativeTransportAddressColumnName))
        listenAddress = row.getInetAddress(listenAddressColumnName)
      } yield (nativeTransportAddress, listenAddress)
    }.toMap.withDefault(identity)
  }

  /** Returns a list of IP-addresses and host names that identify a node.
    * Useful for giving Spark the list of preferred nodes for the Spark partition. */
  def hostNames(nativeTransportAddress: InetAddress): Set[String] = {
    val listenAddress = nativeTransportAddressToListenAddress(nativeTransportAddress)
    Set(
      nativeTransportAddress.getHostAddress,
      nativeTransportAddress.getHostName,
      listenAddress.getHostAddress,
      listenAddress.getHostName
    )
  }
}
