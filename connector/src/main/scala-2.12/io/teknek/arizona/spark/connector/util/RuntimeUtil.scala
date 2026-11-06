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

import org.apache.spark.repl.SparkILoop
import scala.tools.nsc.Settings
import java.io.{BufferedReader, PrintWriter}
import scala.collection.parallel.ParIterable

class Scala213SparkILoop(in: BufferedReader, out: PrintWriter) extends SparkILoop(in, out) {

  def run(interpreterSettings: Settings): Boolean = {
    super.process(interpreterSettings)
  }
}


object RuntimeUtil {

  def toParallelIterable[A](iterable: Iterable[A]): ParIterable[A] = {
    iterable.par
  }

  def createSparkILoop(in: BufferedReader, out: PrintWriter): Scala213SparkILoop = {
    new Scala213SparkILoop(in, out)
  }
}
