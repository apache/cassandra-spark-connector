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

package io.teknek.arizona.spark.connector.ccm

import _root_.io.teknek.arizona.spark.connector.ccm.mode.{ClusterModeExecutor, DebugModeExecutor, DeveloperModeExecutor, ExistingModeExecutor, StandardModeExecutor}

sealed trait ClusterMode {
  def executor(config: CcmConfig): ClusterModeExecutor
}

object ClusterModes {

  /** Default behaviour of CCM Cluster.
    *
    * The cluster is created in a dedicated configuration directory which contains configuration files, data and logs.
    * The directory is removed on JVM shutdown.
    */
  case object Standard extends ClusterMode {
    def executor(config: CcmConfig): ClusterModeExecutor = new StandardModeExecutor(config)
  }

  /** CCM Cluster mode that allows to inspect DB artifacts.
    *
    * The cluster is created in a dedicated directory that is not removed on shutdown. This mode allows to inspect DB
    * artifacts after test execution.
    */
  case object Debug extends ClusterMode {
    def executor(config: CcmConfig): ClusterModeExecutor = new DebugModeExecutor(config)
  }

  /** CCM Cluster mode for fixing or developing tests.
    *
    * It can not be used to run tests from different test groups.
    *
    * The cluster is created and started only if it does not exist during test boostrap. The cluster is not stopped on
    * JVM shutdown, it needs manual stop.
    */
  case object Developer extends ClusterMode {
    def executor(config: CcmConfig): ClusterModeExecutor = new DeveloperModeExecutor(config)
  }

  case object Existing extends ClusterMode {
    override def executor(config: CcmConfig): ClusterModeExecutor = new ExistingModeExecutor(config)
  }

  def fromEnvVar: ClusterMode = {
    // we could use Reflection lib or scala-reflect (both are not present on CP at the moment)
    val knownModes = Seq(Standard, Debug, Developer, Existing)

    sys.env.get("CCM_CLUSTER_MODE").flatMap { modeName =>
      knownModes.collectFirst {
        case c if c.getClass.getName.toLowerCase.contains(modeName.toLowerCase) => c
      }
    }.getOrElse(Standard)
  }

}