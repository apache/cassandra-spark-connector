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
import _root_.io.teknek.arizona.spark.connector.ccm.CcmConfig

import java.nio.file.{Files, Path}

/**
 * A special ClusterModeExecutor which bypasses ccm and assumes a Cassandra instance on localhost
 * with default ports and no authentication.
 * */
private[ccm] class ExistingModeExecutor(val config: CcmConfig) extends ClusterModeExecutor {
  override protected val dir: Path = Files.createTempDirectory("test")

  override def create(clusterName: String): Unit = {
    // do nothing
  }

  override def start(nodeNo: Int): Unit = {
    // do nothing
  }

  override def remove(): Unit = {
    // do nothing
  }
}
