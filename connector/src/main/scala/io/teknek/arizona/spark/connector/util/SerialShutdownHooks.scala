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

package io.teknek.arizona.spark.connector.util

import scala.collection.mutable

private[connector] object SerialShutdownHooks extends Logging {

  private case class PriorityShutdownHook(
      name: String,
      priority: Int,
      task: () => Unit
  )

  private val hooks = mutable.ListBuffer[PriorityShutdownHook]()
  private var isShuttingDown = false

  /** Adds given hook with given priority. The higher the priority, the sooner the hook is executed. */
  def add(name: String, priority: Int)(task: () => Unit): Unit = SerialShutdownHooks.synchronized {
    if (isShuttingDown) {
      logError(s"Adding shutdown hook ($name) during shutting down is not allowed.")
    } else {
      hooks.append(PriorityShutdownHook(name, priority, task))
    }
  }

  Runtime.getRuntime.addShutdownHook(new Thread("Serial shutdown hooks thread") {
    override def run(): Unit = {
      SerialShutdownHooks.synchronized {
        isShuttingDown = true
      }
      val prioritizedHooks = hooks.sortBy(-_.priority)
      for (hook <- prioritizedHooks) {
        try {
          logDebug(s"Running shutdown hook: ${hook.name}")
          hook.task()
          logInfo(s"Successfully executed shutdown hook: ${hook.name}")
        } catch {
          case exc: Throwable =>
            logError(s"Shutdown hook (${hook.name}) failed", exc)
        }
      }
    }
  })
}
