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

package io.teknek.arizona.spark.connector.ccm.mode

import java.nio.file.{Files, Path, Paths}

import _root_.io.teknek.arizona.spark.connector.ccm.CcmConfig

class DeveloperModeExecutor(val config: CcmConfig) extends DefaultExecutor with ClusterModeExecutor {

  private val clusterName = "ccm_" + hash(config.toString)

  override val dir: Path = Files.createDirectories(Paths.get("/tmp/scc_ccm"))

  Runtime.getRuntime.addShutdownHook(new Thread("Serial shutdown hooks thread") {
    override def run(): Unit = {
      println(s"\nCCM is running in developer mode. Cluster $clusterName will not be shutdown. It has to be shutdown " +
        s"manually.\nUse `ccm list --config-dir=$dir` to see the list of developer mode clusters.\nActive cluster may " +
        s"be switched with `ccm switch $clusterName --config-dir=$dir` and then destroyed with " +
        s"`ccm remove --config-dir=$dir`.\n")
    }
  })

  override def remove(): Unit = {
    /* noop */
  }

  private def hash(s: String): String = {
    import java.math.BigInteger
    import java.security.MessageDigest
    val md = MessageDigest.getInstance("MD5")
    val digest = md.digest(s.getBytes)
    val bigInt = new BigInteger(1,digest)
    val hashedString = bigInt.toString(16)
    hashedString
  }

  private def activate(cluster: String): Unit = {
    execute("switch", cluster)
  }

  private def exists(cluster: String): Boolean = {
    val output = execute("list")
    output.exists(_.contains(cluster))
  }

  private def isActive(cluster: String): Boolean = {
    val output = execute("list")
    output.exists(_.contains("*" + cluster))
  }

  override def create(disregardThisName: String): Unit = {
    if (exists(clusterName)) {
      if (!isActive(clusterName)) {
        activate(clusterName)
      }
    } else {
      super.create(clusterName)
    }
  }

  private def isStarted(cluster: String, nodeNo: Int): Boolean = {
    val output = execute("status")
    output.exists(_.contains(s"node$nodeNo: UP"))
  }

  override def start(nodeNo: Int): Unit = {
    if (!isStarted(clusterName, nodeNo)) {
      super.start(nodeNo)
    }
  }
}
