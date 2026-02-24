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

package com.datastax.spark.connector.util

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import scala.annotation.nowarn
import org.apache.spark.sql.cassandra.CassandraSourceRelation
import com.datastax.spark.connector.cql.{AuthConfFactory, CassandraConnectionFactory, CassandraConnectorConf}
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector.types.ColumnTypeConf
import com.datastax.spark.connector.writer.WriteConf

import scala.collection.mutable

/**
  * Helper class to throw exceptions if there are environment variables in the spark.cassandra
  * namespace which don't map to Spark Cassandra Connector known properties.
  */
object ConfigCheck {

  val MatchThreshold = 0.85
  val Prefix = "spark.cassandra."



  /** Set of valid static properties hardcoded in the connector.
    * Custom CassandraConnectionFactory and AuthConf properties are not listed here. */
  val validStaticProperties: mutable.Set[ConfigParameter[_]] = ConfigParameter.staticParameters

  val validStaticPropertyNames = validStaticProperties.map(_.name)

  val deprecatedProperties: mutable.Set[DeprecatedConfigParameter[_]] = DeprecatedConfigParameter.deprecatedParameters

 /**
    * Checks the SparkConf Object for any unknown spark.cassandra.* properties and throws an exception
    * with suggestions if an unknown property is found.
    *
    * @param conf SparkConf object to check
    */
  def checkConfig(conf: SparkConf): Unit = {
    for (deprecatedProperty <- deprecatedProperties) {
      deprecatedProperty.maybeReplace(conf)
    }

    val connectionFactory = CassandraConnectionFactory.fromSparkConf(conf)
    val authConfFactory = AuthConfFactory.fromSparkConf(conf)
    val extraProps = connectionFactory.properties ++ authConfFactory.properties

    /** We no longer support snake_case properties, so we are going to fail
      * all the tests if a snake case parameter is added back.
      */
    val invalidProperties =
      (validStaticPropertyNames ++ extraProps)
        .filter( property => property.contains("_"))

    if (invalidProperties.nonEmpty) throw new IllegalArgumentException(
      s"Developer Error: These properties will not work in SparkSql/DataSets " +
        s"because of capitals: ${invalidProperties.mkString(",")}")

    val unknownProps = unknownProperties(conf, extraProps)
    if (unknownProps.nonEmpty) {
      val suggestions =
        for (u <- unknownProps) yield (u, suggestedProperties(u))
      throw new ConnectorConfigurationException(unknownProps, suggestions.toMap)
    }
  }

  def unknownProperties(conf: SparkConf, extraProps: Set[String] = Set.empty): Seq[String] = {
    val validProps = validStaticPropertyNames ++ extraProps
    val scEnv = for ((key, value) <- conf.getAll if key.startsWith(Prefix)) yield key
    (for (key <- scEnv if !validProps.contains(key)) yield key).toIndexedSeq
  }

  /**
    * For a given unknown property determine if we have any guesses as
    * to what the property should be. Suggestions are found by
    * breaking the unknown property into fragments.
    * spark.cassandra.foo.bar => [foo,bar]
    * Any known property which has some fragments which fuzzy match
    * all of the fragments of the unknown property are considered a
    * suggestion.
    *
    * Fuzziness is determined by MatchThreshold
    */
  def suggestedProperties(unknownProp: String, extraProps: Set[String] = Set.empty): Seq[String] = {
    val validProps = validStaticPropertyNames ++ extraProps
    val unknownFragments = unknownProp.stripPrefix(Prefix).split("\\.")
    validProps.toSeq.filter { knownProp =>
      val knownFragments = knownProp.stripPrefix(Prefix).split("\\.")
      unknownFragments.forall { unknown =>
        knownFragments.exists { known =>
          val matchScore = StringUtils.getJaroWinklerDistance(unknown, known): @nowarn("cat=deprecation")
          matchScore >= MatchThreshold
        }
      }
    }
  }

  class ConnectorConfigurationException(message: String) extends Exception(message) {
    /**
      * Exception to be thrown when unknown properties are found in the SparkConf
      *
      * @param unknownProps Properties that have no mapping to known Spark Cassandra Connector properties
      * @param suggestionMap A map possibly containing suggestions for each of of the unknown properties
      */
    def this(unknownProps: Seq[String], suggestionMap: Map[String, Seq[String]]) = this({
      val intro =
        "Invalid Config Variables\n" +
          "Only known spark.cassandra.* variables are allowed when using the Spark Cassandra Connector.\n"
      val body = unknownProps.map { unknown =>
        suggestionMap.get(unknown) match {
          case Some(Seq()) | None =>
            s"""$unknown is not a valid Spark Cassandra Connector variable.
               |No likely matches found.""".stripMargin
          case Some(suggestions) =>
            s"""$unknown is not a valid Spark Cassandra Connector variable.
               |Possible matches:
               |${suggestions.mkString("\n")}""".stripMargin
        }
      }.mkString("\n")
      intro + body
    })
  }

}

