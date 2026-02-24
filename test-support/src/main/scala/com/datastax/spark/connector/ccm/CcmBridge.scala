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

package com.datastax.spark.connector.ccm

import java.io.IOException
import java.util.concurrent.TimeUnit

import com.datastax.oss.driver.api.core.Version
import org.apache.commons.exec.{CommandLine, ExecuteWatchdog, LogOutputStream, _}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.language.reflectiveCalls
import scala.util.Try

class CcmBridge(config: CcmConfig) extends AutoCloseable {

  CcmBridge.logger.info(s"CcmBridge is running in ${config.mode} mode.")

  private val executor = config.mode.executor(config)

  def getDseVersion: Option[Version] = config.getDseVersion

  def getCassandraVersion: Version = config.getCassandraVersion

  def execute(args: String*): Unit = executor.execute(args: _*)

  def executeUnsanitized(args: String*): Unit = executor.executeUnsanitized(args: _*)

  def create(clusterName: String): Unit = executor.create(clusterName)

  def remove(): Unit = executor.remove()

  override def close(): Unit = {
    remove()
  }

  def start(): Unit = {
    config.nodes.foreach(executor.start)
  }

  def stop(): Unit = {
    execute("stop")
  }

  def pause(n: Int): Unit = {
    execute(s"node$n", "pause")
  }

  def resume(n: Int): Unit = {
    execute(s"node$n", "resume")
  }

  def dsetool(n: Int, args: String*): Unit = {
    execute(Seq(s"node$n", "dsetool") ++ args: _*)
  }

  def nodetool(n: Int, args: String*): Unit = {
    execute(Seq(s"node$n", "nodetool") ++ args: _*)
  }

  def refreshSizeEstimates(n: Int): Unit = {
    // Scylla does not support nodetool refreshsizeestimates
    if (!config.scyllaEnabled) {
      nodetool(n, "refreshsizeestimates")
    } else {
      CcmBridge.logger.info("Skipping refreshsizeestimates - not supported on Scylla")
    }
  }

  def flush(n: Int): Unit = {
    execute("node" + n, "flush")
  }

}

object CcmBridge {

  private val logger: Logger = LoggerFactory.getLogger(classOf[CcmBridge])

  def execute(cli: Seq[String]): Seq[String] = {
    val cmdLine = new CommandLine(cli.head)
    cli.tail.foreach(cmdLine.addArgument)
    execute(cmdLine)
  }

  def execute(cli: CommandLine): Seq[String] = {
    logger.info("Executing: " + cli)

    val watchDog: ExecuteWatchdog = new ExecuteWatchdog(TimeUnit.MINUTES.toMillis(10))

    val outStream = new LogOutputStream() {
      val lines: mutable.Buffer[String] = mutable.Buffer[String]()
      override def processLine(line: String, logLevel: Int): Unit = {
        lines += line
        logger.debug("ccmout> {}", line)
      }
    }
    val errStream = new LogOutputStream() {
      override def processLine(line: String, logLevel: Int): Unit = logger.error("ccmerr> {}", line)
    }

    try {
      val executor = new DefaultExecutor()
      val streamHandler = new PumpStreamHandler(outStream, errStream)
      executor.setStreamHandler(streamHandler)
      executor.setWatchdog(watchDog)
      val env = {
        val base = if (sys.env.contains("CCM_JAVA_HOME")) {
          val ccmJavaHome = sys.env("CCM_JAVA_HOME")
          // Prepend CCM Java's bin to PATH so `java` on PATH matches JAVA_HOME.
          // CCM validates that PATH's java version matches JAVA_HOME.
          val updatedPath = s"$ccmJavaHome/bin${java.io.File.pathSeparator}${sys.env.getOrElse("PATH", "")}"
          sys.env + ("JAVA_HOME" -> ccmJavaHome) + ("PATH" -> updatedPath)
        } else {
          sys.env
        }
        // Clear JAVA_TOOL_OPTIONS to prevent Java 17+ flags (e.g. --add-opens)
        // from being passed to older JVMs that don't understand them.
        base - "JAVA_TOOL_OPTIONS" - "_JAVA_OPTIONS"
      }

      val retValue = executor.execute(cli, env.asJava)
      if (retValue != 0) {
        logger.error(
          "Non-zero exit code ({}) returned from executing ccm command: {}", retValue, cli)
      }
      outStream.lines.toSeq
    } catch {
      case _: IOException if watchDog.killedProcess() =>
        throw new RuntimeException(s"The command $cli was killed after 10 minutes")
      case ex: IOException =>
        throw new RuntimeException(s"The command $cli failed to execute", ex)
    } finally {
      Try(outStream.close())
      Try(errStream.close())
    }
  }
}