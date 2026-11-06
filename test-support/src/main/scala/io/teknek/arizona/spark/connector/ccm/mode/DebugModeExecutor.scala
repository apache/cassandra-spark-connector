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
import org.slf4j.{Logger, LoggerFactory}

private[ccm] class DebugModeExecutor(val config: CcmConfig) extends DefaultExecutor {

  private val logger: Logger = LoggerFactory.getLogger(classOf[StandardModeExecutor])

  private val Cwd = Paths.get("").toAbsolutePath().toString();

  override val dir: Path = {
    sys.env.get("PRESERVE_LOGS") match {
      case Some(dir) =>
        val subPath = s"$Cwd/$dir/ccm_${config.ipPrefix
          .replace(".","_")
          .stripSuffix("_")}"

        val path = Files.createDirectories(Paths.get(subPath))
        logger.debug(s"Preserving CCM Install Directory at [$path]. It will not be removed")
        logger.debug(s"Checking directory exists [${Files.exists(path)}]")
        path
      case None =>
        val tmp = Files.createTempDirectory("ccm")
        tmp.toFile.deleteOnExit()
        tmp
    }
  }

  // stop nodes, don't remove logs
  override def remove(): Unit = {
     execute("stop")
  }

}
