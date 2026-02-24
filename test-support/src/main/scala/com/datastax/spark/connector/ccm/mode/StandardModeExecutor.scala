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

package com.datastax.spark.connector.ccm.mode

import java.io.File
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.atomic.AtomicBoolean
import com.datastax.oss.driver.api.core.Version
import com.datastax.spark.connector.ccm.CcmConfig
import com.datastax.spark.connector.ccm.CcmConfig.DSE_V6_8_5
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration.DurationInt
import scala.util.Try
import scala.util.control.NonFatal

private[mode] trait DefaultExecutor extends ClusterModeExecutor {
  private val logger: Logger = LoggerFactory.getLogger(classOf[StandardModeExecutor])

  private val created = new AtomicBoolean()

  private def waitForNode(nodeNo: Int): Unit = {
    logger.info(s"Waiting for node $nodeNo to become alive...")
    val t0 = System.currentTimeMillis()
    if (!waitForNode(nodeNo, 2.minutes)) {
      throw new IllegalStateException(s"Timeouted on waiting for node $nodeNo")
    }

    logger.info(s"Node $nodeNo is alive in ${(System.currentTimeMillis() - t0) / 1000} s")
  }

  private def logStdErr(nodeNo: Int): Unit = {
    Try {
      val linesCount = 1000
      val logsDir = new File(s"${dir}/ccm_1/node${nodeNo}/logs/")

      if (logsDir.exists() && logsDir.isDirectory) {
        val stdErrFile = logsDir.listFiles().filter(_.getName.endsWith("stderr.log")).head
        val stdOutFile = logsDir.listFiles().filter(_.getName.endsWith("stdout.log")).head
        logger.error(s"logs dir: \n" + logsDir.listFiles().map(_.getName).mkString("\n"))
        logger.error(s"Start command failed, here is the last $linesCount lines of startup-stderr and startup-stdout files: \n" +
          getLastLogLines(stdErrFile.getAbsolutePath, linesCount).mkString("\n") + getLastLogLines(stdOutFile.getAbsolutePath, linesCount).mkString("\n"))
      }
    }
  }

  override def start(nodeNo: Int): Unit = {
    val jvmArgs = nodeNo match {
      case 1 => config.jvmArgs :+ "-Dcassandra.superuser_setup_delay_ms=0" :+ "-Dcassandra.ring_delay_ms=1000" :+ "-Dcassandra.skip_sync=true"
      case _ => config.jvmArgs :+ "-Dcassandra.ring_delay_ms=5000" :+ "-Dcassandra.skip_sync=true"
    }
    // Scylla CCM does not support jvm version option and --skip-wait-other-notice, since it is skipped by default
    val formattedJvmArgs = if (config.scyllaEnabled) Seq.empty else jvmArgs.map(arg => s"--jvm_arg=$arg")
    val formattedJvmVersion = if (config.scyllaEnabled) Seq.empty else javaVersion.map(v => s"--jvm-version=$v").toSeq
    val waitOthersNotice = if (config.scyllaEnabled) "" else "--skip-wait-other-notice"
    try {
      execute(Seq(s"node$nodeNo", "start", "-v", waitOthersNotice) ++ formattedJvmArgs ++ formattedJvmVersion :_*)
      waitForNode(nodeNo)
    } catch {
      case NonFatal(e) =>
        logStdErr(nodeNo)
        throw e
    }
  }

  private def eventually[T](hint: String = "", f: =>T ): T = {
    val start = System.currentTimeMillis()
    val timeoutInSeconds = 20
    val intervalInMs = 500
    val end = start + timeoutInSeconds * 1000

    while ( System.currentTimeMillis() < end) {
      try {
        return f
      } catch { case e: Throwable =>
        logger.warn(s"Tried to execute code, will retry : ${e.getMessage}")
        Thread.sleep(intervalInMs)
      }
    }
    throw new IllegalStateException(s"Unable to complete function in $timeoutInSeconds: $hint")
  }

  /**
    * This is a workaround that allows running it:test against X.Y.0-betaZ and X.Y.0-rcZ. These C* versions are
    * published as X.Y-betaZ and X.Y-rcZ, lack of patch version breaks versioning convention used in integration tests.
    */
  private def adjustCassandraBetaVersion(version: String): String = {
    val betaOrRC = "(\\d+).(\\d+).0-(beta|rc)(\\d+)".r
    version match {
      case betaOrRC(x, y, t, n) => s"$x.$y-$t$n"
      case other => other
    }
  }

  override def create(clusterName: String): Unit = {
    if (created.compareAndSet(false, true)) {
      val options = config.installDirectory
        .map(dir => config.createOptions :+ s"--install-dir=${new File(dir).getAbsolutePath}")
        .orElse(config.installBranch.map(branch => config.createOptions ++ Seq("-v", s"git:${branch.trim().replaceAll("\"", "")}")))
        .getOrElse(config.createOptions ++ Seq("-v", adjustCassandraBetaVersion(config.rawVersion)))

      val dseFlag = if (config.dseEnabled) Some("--dse") else None
      val scyllaFlag = if (config.scyllaEnabled) Some("--scylla") else None

      val createArgs = Seq("create", clusterName, "-i", config.ipPrefix) ++ options ++ dseFlag ++ scyllaFlag

      // Check installed Directory
      val repositoryDir = Paths.get(
        sys.props.get("user.home").get,
        ".ccm",
        "repository",
        adjustCassandraBetaVersion(config.getDseVersion.getOrElse(config.getCassandraVersion).toString))

      if (Files.exists(repositoryDir)) {
        logger.info(s"Found cached repository dir: $repositoryDir")
        logger.info("Checking for appropriate bin dir")
        eventually(f = Files.exists(repositoryDir.resolve("bin")))
      }

      try {
        execute(createArgs: _*)
      } catch {
        case NonFatal(e) =>
          Try(logger.error("Create command failed, here is the last 500 lines of ccm repository log: \n" +
              getLastRepositoryLogLines(500).mkString("\n")))
          throw e
      }

      eventually("Checking to make sure repository was correctly expanded", {
        Files.exists(repositoryDir.resolve("bin"))
      })

      config.nodes.foreach { i =>
        val node = s"node$i"
        val addArgs = Seq ("add",
          "-s", // every node is a seed node
          "-b", // autobootstrap is enabled
          "-j", config.jmxPort(i).toString,
          "-i", config.ipOfNode(i),
          "--remote-debug-port=0") ++
          dseFlag ++
          scyllaFlag :+
          node

        execute(addArgs: _*)

        if (config.dseEnabled && config.getDseVersion.exists(_.compareTo(DSE_V6_8_5) >= 0)) {
          execute(node, "updateconf", s"metadata_directory:${dir.toFile.getAbsolutePath}/metadata$i")
        }
      }

      config.cassandraConfiguration.foreach { case (key, value) =>
        execute("updateconf", s"$key:$value")
      }
      if (config.scyllaEnabled) {
        execute("updateconf", "enable_user_defined_functions:true")
        execute("updateconf", "experimental_features:[udf]")
      } else if (config.getCassandraVersion.compareTo(Version.V2_2_0) >= 0 && config.getCassandraVersion.compareTo(CcmConfig.V4_1_0) < 0) {
        execute("updateconf", "enable_user_defined_functions:true")
      } else if (config.getCassandraVersion.compareTo(CcmConfig.V4_1_0) >= 0) {
        execute("updateconf", "user_defined_functions_enabled:true")
      }
      if (config.dseEnabled) {
        config.dseConfiguration.foreach { case (key, value) =>
          execute("updatedseconf", s"$key:$value")
        }
        config.dseRawYaml.foreach { yaml =>
          executeUnsanitized("updatedseconf", "-y", yaml)
        }
        if (config.dseWorkloads.nonEmpty) {
          execute("setworkload", config.dseWorkloads.mkString(","))
        }
      } else {
        // C* 4.0.0 has materialized views disabled by default
        if (config.getCassandraVersion.compareTo(Version.V4_0_0) >= 0 && config.getCassandraVersion.compareTo(CcmConfig.V4_1_0) < 0) {
          execute("updateconf", "enable_materialized_views:true")
        } else if (config.getCassandraVersion.compareTo(CcmConfig.V4_1_0) >= 0) {
          execute("updateconf", "materialized_views_enabled:true")
        }
      }
    }
  }
}

private[ccm] class StandardModeExecutor(val config: CcmConfig) extends DefaultExecutor {
  override val dir: Path = Files.createTempDirectory("ccm")

  // remove config directory on shutdown
  dir.toFile.deleteOnExit()
  // remove db artifacts
  override def remove(): Unit = {
    execute("remove")
  }
}
