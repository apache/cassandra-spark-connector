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

import _root_.io.teknek.arizona.spark.connector.cql.{AuthConfFactory, CassandraConnectionFactory}

object RefBuilder {

  val Header =
    """# Configuration Reference
      |
      |
    """.stripMargin
  val Footer = "\n"

  val HtmlTableHeader =
    """<table class="table">
      |<tr><th>Property Name</th><th>Default</th><th>Description</th></tr>""".stripMargin

  val allConfigs = ConfigCheck.validStaticProperties

  def getMarkDown(): String = {
    val configBySection = allConfigs.groupBy(x => x.section)
    val sections = configBySection.keys.toSeq.sorted
    val markdown = for (section <- sections) yield {
      val parameters = configBySection(section)
      val paramTable = parameters.toList.sortBy(_.name).map { case parameter: ConfigParameter[_] =>
        val default = parameter.default match {
          case x: CassandraConnectionFactory => x.getClass.getSimpleName.stripSuffix("$")
          case x: AuthConfFactory => x.getClass.getSimpleName.stripSuffix("$")
          case x: Seq[_] => x.mkString(",")
          case Some(defaultValue) => defaultValue
          case None => None
          case value => value
        }
        s"""<tr>
            |  <td><code>${parameter.name}</code></td>
            |  <td>$default</td>
            |  <td>${parameter.description}</td>
            |</tr>""".stripMargin
      }.mkString("\n")

      s"""
         |## $section
         |$HtmlTableHeader
         |$paramTable
         |</table>""".stripMargin
    }
    Header + markdown.mkString("\n\n") + Footer
  }

}
