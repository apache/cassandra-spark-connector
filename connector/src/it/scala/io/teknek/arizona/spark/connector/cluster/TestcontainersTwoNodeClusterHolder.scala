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
import java.time.Duration

import com.datastax.oss.driver.api.core.Version
import _root_.io.teknek.arizona.spark.connector.util.{Logging, SerialShutdownHooks}
import org.testcontainers.containers.{GenericContainer, Network}
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

object TestcontainersTwoNodeClusterHolder extends Logging {

  private val ImageName = "cassandra:5.0.2"
  private val CassandraVersion = Version.parse("5.0.2")

  SerialShutdownHooks.add("Connector test two-node Testcontainers cluster shutdown hook", 100)(() => {
    close()
  })

  private var cluster: Option[Cluster] = None
  private var network: Option[Network] = None

  private def close(): Unit = synchronized {
    cluster.foreach { c =>
      try {
        c.close()
      } catch {
        case t: Throwable => logWarning(s"Closing a ${c.name} failed", t)
      }
    }
    network.foreach { n =>
      try {
        n.close()
      } catch {
        case t: Throwable => logWarning("Closing two-node Testcontainers network failed", t)
      }
    }
    cluster = None
    network = None
  }

  def get(connectionParameters: Seq[InetSocketAddress] => Map[String, String]): Cluster = synchronized {
    cluster.getOrElse {
      val net = Network.newNetwork()
      network = Some(net)

      val node1 = cassandraNode(net, "cassandra-1")
        .withEnv("CASSANDRA_SEEDS", "cassandra-1")
      node1.start()

      val node2 = cassandraNode(net, "cassandra-2")
        .withEnv("CASSANDRA_SEEDS", "cassandra-1")
      node2.start()

      val containers = Seq(node1, node2)
      val addresses = containers.map(c => new InetSocketAddress(c.getHost, c.getMappedPort(9042)))
      val testCluster = Cluster(
        "testcontainers_two_node",
        addresses,
        () => containers.reverse.foreach(_.stop()),
        n => containers(n - 1).execInContainer("nodetool", "flush"),
        n => containers(n - 1).execInContainer("nodetool", "refreshsizeestimates"),
        () => CassandraVersion,
        () => None,
        _ => connectionParameters(addresses))

      cluster = Some(testCluster)
      testCluster
    }
  }

  private def cassandraNode(network: Network, alias: String): GenericContainer[_] = {
    new GenericContainer[GenericContainer[_]](DockerImageName.parse(ImageName))
      .withNetwork(network)
      .withNetworkAliases(alias)
      .withExposedPorts(9042)
      .withEnv("CASSANDRA_CLUSTER_NAME", "scc-testcontainers")
      .withEnv("CASSANDRA_DC", "datacenter1")
      .withEnv("CASSANDRA_RACK", "rack1")
      .withEnv("CASSANDRA_ENDPOINT_SNITCH", "GossipingPropertyFileSnitch")
      .withEnv("CASSANDRA_NUM_TOKENS", "16")
      .withEnv("MAX_HEAP_SIZE", "512M")
      .withEnv("HEAP_NEWSIZE", "128M")
      .waitingFor(Wait.forLogMessage(".*Startup complete.*", 1))
      .withStartupTimeout(Duration.ofMinutes(5))
  }
}
