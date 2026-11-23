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

package com.datastax.spark.connector.doc

import java.io.File

import com.datastax.spark.connector.util.{ConfigCheck, RefBuilder}
import org.scalatest.{FlatSpec, Matchers}

class DocCheck extends FlatSpec with Matchers {

  val subprojectRoot = System.getenv("PWD") + "/spark-connector"

  val reRunMessage =
    """******* re-run sbt connector/run to regenerate properties file
      |*******.
    """.stripMargin

  val refFile = scala.io.Source.fromFile(new File(s"doc/reference.md")).mkString

  "The Reference Parameters File" should "contain all of the current properties" in withClue(reRunMessage){
    val missingProperties =
      for (propertyName <- ConfigCheck.validStaticPropertyNames
        if !refFile.contains(propertyName.stripPrefix("spark.cassandra."))) yield propertyName

    missingProperties should be ('empty)

    info(s"Reference contains ${ConfigCheck.validStaticPropertyNames.size} entries")
  }

  it should "match a freshly created reference file" in withClue(reRunMessage){
    RefBuilder.getMarkDown() should be(refFile)

  }

  case class ParameterFound (parameter: String, fileName : String)
  it should "only reference current parameters" in {
    val docFiles = new java.io.File(s"doc").listFiles()
    val allDocs = docFiles.map( file => (file, scala.io.Source.fromFile(file).mkString))

    val SparkParamRegex = """spark\.cassandra\.\w+""".r.unanchored
    val parameterUsages = for (
      doc <- allDocs;
      m <- SparkParamRegex findAllMatchIn doc._2
    ) yield {
      ParameterFound(m.matched, doc._1.getName)
    }

    val PropertyNames = ConfigCheck.validStaticPropertyNames

    val matchesByFile = parameterUsages.groupBy(x => x.fileName).toList.sortBy( x => x._1)
    for ( fileToMatches <- matchesByFile) {
      info( s"${fileToMatches._1} had ${fileToMatches._2.length} parameter usages")
    }

    info(s"Docs contains ${parameterUsages.length} references to spark.cassandra properties")

    val unknownParameters: Seq[ParameterFound] =
      for (foundParameter <- parameterUsages
        if PropertyNames.contains(foundParameter.parameter)) yield {
      foundParameter
    }
    unknownParameters should be ('empty)
  }

}
