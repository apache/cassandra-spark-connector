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
import _root_.io.teknek.arizona.spark.connector.util.{Logging, SerialShutdownHooks}
import org.testcontainers.cassandra.CassandraContainer
import org.testcontainers.utility.DockerImageName

object TestcontainersClusterHolder extends Logging {

  private val ImageName = "cassandra:5.0.2"
  private val CassandraVersion = Version.parse("5.0.2")

  SerialShutdownHooks.add("Connector test Testcontainers cluster shutdown hook", 100)(() => {
    close()
  })

  private var cluster: Option[Cluster] = None

  private def close(): Unit = synchronized {
    cluster.foreach { c =>
      try {
        c.close()
      } catch {
        case t: Throwable => logWarning(s"Closing a ${c.name} failed", t)
      }
    }
    cluster = None
  }

  def get(connectionParameters: InetSocketAddress => Map[String, String]): Cluster = synchronized {
    cluster.getOrElse {
      val cassandra = new CassandraContainer(DockerImageName.parse(ImageName))
      cassandra.start()

      val address = new InetSocketAddress(cassandra.getHost, cassandra.getMappedPort(9042))
      val testCluster = Cluster(
        "testcontainers_1",
        Seq(address),
        () => cassandra.stop(),
        _ => cassandra.execInContainer("nodetool", "flush"),
        _ => cassandra.execInContainer("nodetool", "refreshsizeestimates"),
        () => CassandraVersion,
        () => None,
        connectionParameters)

      cluster = Some(testCluster)
      testCluster
    }
  }
}
