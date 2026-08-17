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

import sbt.{ModuleID, _}

object Dependencies
{
  import Versions._

  object Spark {
    val sparkCore = "org.apache.spark" %% "spark-core" % ApacheSpark % "provided" // ApacheV2
    val sparkRepl = "org.apache.spark" %% "spark-repl" % ApacheSpark % "provided" // ApacheV2
    val sparkUnsafe = "org.apache.spark" %% "spark-unsafe" % ApacheSpark % "provided" // ApacheV2
    val sparkStreaming = "org.apache.spark" %% "spark-streaming" % ApacheSpark % "provided" // ApacheV2
    val sparkSql = "org.apache.spark" %% "spark-sql" % ApacheSpark % "provided" // ApacheV2
    val sparkCatalyst = "org.apache.spark" %% "spark-catalyst" % ApacheSpark % "provided" // ApacheV2
    val sparkHive = "org.apache.spark" %% "spark-hive" % ApacheSpark % "provided" // ApacheV2


    val dependencies = Seq(
      sparkCore,
      sparkRepl,
      sparkUnsafe,
      sparkStreaming,
      sparkSql,
      sparkHive,
      sparkCatalyst)
  }

  implicit class Exclude(module: ModuleID) {
    def logbackExclude(): ModuleID = module
      .exclude("ch.qos.logback", "logback-classic")
      .exclude("ch.qos.logback", "logback-core")
      .exclude("org.slf4j", "log4j-over-slf4j")

    def driverCoreExclude(): ModuleID = module
      .exclude("org.apache.cassandra", "java-driver-core") // doesn't shade guava
      .exclude("org.apache.tinkerpop", "*")
      // until SPARK-20075 is fixed we fallback to java workarounds for native calls
      .exclude("com.github.jnr", "jnr-posix")
  }

  object TestCommon {
    val mockito = "org.mockito" % "mockito-core" % Mockito
    val junit = "junit" % "junit" % JUnit
    val junitInterface = "com.novocode" % "junit-interface" % JUnitInterface
    val scalaTest = "org.scalatest" %% "scalatest" % ScalaTest
    val driverMapperProcessor = "org.apache.cassandra" % "java-driver-mapper-processor" % CassandraJavaDriver
    val esriGeometry = "com.esri.geometry" % "esri-geometry-api" % EsriGeometry
  }

  object TestConnector {
    val commonsIO         = "commons-io"              %  "commons-io"                   % CommonsIO    % "test,it"       // ApacheV2
    val scalaCheck        = "org.scalacheck"          %% "scalacheck"                   % ScalaCheck   % "test,it"      // BSD
    val sparkCoreT        = "org.apache.spark"        %% "spark-core"                   % ApacheSpark  % "test,it" classifier "tests"
    val sparkStreamingT   = "org.apache.spark"        %% "spark-streaming"              % ApacheSpark  % "test,it" classifier "tests"
    val solrj             = "org.apache.solr"         %  "solr-solrj"                   % SolrJ      % "test,it"
    val testcontainers    = "org.testcontainers"      %  "testcontainers"               % "2.0.5"   % "test,it"
    val cassandraContainer = "org.testcontainers"     %  "testcontainers-cassandra"     % "2.0.5"   % "test,it"

    val dependencies = Seq(
      scalaCheck,
      sparkCoreT,
      sparkStreamingT,
      solrj,
      testcontainers,
      cassandraContainer,
      TestCommon.driverMapperProcessor % "test,it" driverCoreExclude(),
      TestCommon.scalaTest % "test,it",
      TestCommon.mockito % "test,it",
      TestCommon.junit % "test,it",
      TestCommon.junitInterface % "test,it",
      TestCommon.esriGeometry % "test,it").map(_.logbackExclude())
  }

  // Required for metrics
  object Jetty {
    val jettyServer       = "org.eclipse.jetty"       % "jetty-server"            % SparkJetty % "provided"
    val jettyServlet      = "org.eclipse.jetty"       % "jetty-servlet"           % SparkJetty % "provided"

    val dependencies = Seq(jettyServer, jettyServlet)
  }

  object Driver {
    val driverCore = "org.apache.cassandra" % "java-driver-core-shaded" % CassandraJavaDriver driverCoreExclude()
    val driverMapper = "org.apache.cassandra" % "java-driver-mapper-runtime" % CassandraJavaDriver driverCoreExclude()

    val commonsLang3 = "org.apache.commons" % "commons-lang3" % Versions.CommonsLang3
    val paranamer = "com.thoughtworks.paranamer" % "paranamer" % Versions.Paranamer


    val dependencies = Seq(driverCore, driverMapper, commonsLang3, paranamer)
  }

  object Compatibility {
    val scalaCompat = "org.scala-lang.modules" %% "scala-collection-compat" % Versions.ScalaCompat
    val parallelCollections = "org.scala-lang.modules" %% "scala-parallel-collections" % Versions.ParallelCollections

    def dependencies(version: String): Seq[ModuleID] = {
      CrossVersion.partialVersion(version) match {
        case Some((2, scalaMajor)) if scalaMajor == 13 => Seq(scalaCompat, parallelCollections)
        case _ => Seq(scalaCompat)
      }
    }
  }

  object TestDriver {
    val dependencies = Seq(
      TestCommon.scalaTest % "test",
      TestCommon.mockito % "test",
      TestCommon.junit % "test",
      TestCommon.junitInterface % "test",
      TestCommon.driverMapperProcessor % "test" driverCoreExclude()
    )
  }

  object TestSupport {
    val commonsExec = "org.apache.commons" % "commons-exec" % CommonsExec

    val dependencies = Seq(
      commonsExec,
      Dependencies.Driver.driverCore)
  }
}
