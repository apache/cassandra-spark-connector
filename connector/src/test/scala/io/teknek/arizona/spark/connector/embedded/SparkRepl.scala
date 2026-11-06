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

package io.teknek.arizona.spark.connector.embedded

import _root_.io.teknek.arizona.spark.connector.util.RuntimeUtil

import java.io._
import java.net.URLClassLoader
import org.apache.spark.SparkConf
import org.apache.spark.repl.{Main, SparkILoop}

import scala.collection.mutable.ArrayBuffer
import scala.tools.nsc.GenericRunnerSettings

object SparkRepl {

  def runInterpreter(input: String, conf: SparkConf): String = {
    val in = new BufferedReader(new StringReader(input + "\n"))
    val out = new StringWriter()
    val cl = getClass.getClassLoader
    var paths = new ArrayBuffer[String]
    cl match {
      case urlLoader: URLClassLoader =>
        for (url <- urlLoader.getURLs) {
          if (url.getProtocol == "file") {
            paths += url.getFile
          }
        }
      case _ =>
    }

    Main.conf.setAll(conf.getAll)
    val interp = RuntimeUtil.createSparkILoop(in, new PrintWriter(out))
    Main.interp = interp
    val separator = System.getProperty("path.separator")
    val settings = new GenericRunnerSettings(s => throw new RuntimeException(s"Scala options error: $s"))
    settings.processArguments(List("-classpath", paths.mkString(separator)), processAll = true)
    interp.run(settings) // Repl starts and goes in loop of R.E.P.L
    Main.interp = null
    Option(Main.sparkContext).foreach(_.stop())
    System.clearProperty("spark.driver.port")
    out.toString
  }

}