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

package com.datastax.spark.connector

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.util.concurrent.Executors

import com.datastax.dse.driver.api.core.metadata.DseNodeProperties
import com.datastax.oss.driver.api.core.config.DefaultDriverOption.{CONNECTION_MAX_REQUESTS, CONNECTION_POOL_LOCAL_SIZE}
import com.datastax.oss.driver.api.core.cql.{AsyncResultSet, BoundStatement, SimpleStatement, Statement}
import com.datastax.oss.driver.api.core.cql.SimpleStatement._
import com.datastax.oss.driver.api.core.{CqlSession, ProtocolVersion, Version}
import com.datastax.spark.connector.cluster.ClusterProvider
import com.datastax.spark.connector.cql.{CassandraConnector, DefaultAuthConfFactory}
import com.datastax.spark.connector.datasource.{CassandraCatalog, CassandraScan, CassandraTable}
import com.datastax.spark.connector.embedded.SparkTemplate
import com.datastax.spark.connector.testkit.AbstractSpec
import com.datastax.spark.connector.util.Logging
import com.datastax.spark.connector.writer.AsyncExecutor
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.internal.SQLConf
import org.scalatest._
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.{Seconds, Span}

import scala.jdk.CollectionConverters._
import scala.collection.compat._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try

trait SparkCassandraITFlatSpecBase extends FlatSpec with SparkCassandraITSpecBase {
}

trait SparkCassandraITWordSpecBase extends WordSpec with SparkCassandraITSpecBase {
}

trait SparkCassandraITAbstractSpecBase extends AbstractSpec with SparkCassandraITSpecBase {
}

trait SparkCassandraITSpecBase
  extends TestSuite
  with Matchers
  with BeforeAndAfterAll
  with ClusterProvider
  with Logging
  with Alerting {

  final def defaultConf: SparkConf = {
    SparkTemplate.defaultConf
      .setAll(cluster.connectionParameters)
  }
  final def sparkConf = defaultConf

  lazy val spark = SparkSession.builder()
    .config(sparkConf)
    .withExtensions(new CassandraSparkExtensions).getOrCreate().newSession()

  lazy val sc = spark.sparkContext

  val originalProps = sys.props.clone()

  private  def isSerializable(e: Throwable): Boolean =
    Try(new ObjectOutputStream(new ByteArrayOutputStream()).writeObject(e)).isSuccess

  // Exceptions thrown by test code are serialized and sent back to test framework main process.
  // Unserializable exceptions break communication between forked test and main test process".
  private def wrapUnserializableExceptions[T](f: => T): T = {
    try {
      f
    } catch {
      case e: Throwable =>
        if (isSerializable(e)) {
          throw e
        } else {
          logError(s"$this failed due to unserializable exception", e)
          throw new java.io.NotSerializableException(s"Unserializable exception was thrown by $this. The exception " +
            s"message was: ${ExceptionUtils.getMessage(e)}, with root cause: ${ExceptionUtils.getRootCauseMessage(e)}." +
            s"Full stack trace should be logged above.")
        }
    }
  }

  final override def beforeAll(): Unit = wrapUnserializableExceptions {
    beforeClass
  }

  def beforeClass: Unit = {}

  def afterClass: Unit = {}

  final override def afterAll(): Unit = wrapUnserializableExceptions {
    afterClass
    restoreSystemProps()
  }

  override def withFixture(test: NoArgTest): Outcome = wrapUnserializableExceptions {
      super.withFixture(test)
  }

  def getKsName = {
    val className = this.getClass.getSimpleName
    val suffix = StringUtils.splitByCharacterTypeCamelCase(className.filter(_.isLetterOrDigit)).mkString("_")
    s"test_$suffix".toLowerCase()
  }

  def conn: CassandraConnector = ???

  lazy val executor = getExecutor(CassandraConnector(sc.getConf).openSession)

  def getExecutor(session: CqlSession): AsyncExecutor[Statement[_], AsyncResultSet] = {
    val profile = session.getContext.getConfig.getDefaultProfile
    val maxConcurrent = profile.getInt(CONNECTION_POOL_LOCAL_SIZE) * profile.getInt(CONNECTION_MAX_REQUESTS)
    new AsyncExecutor[Statement[_], AsyncResultSet](
      stmt => stmt match {
        //Handling Types
        case bs: BoundStatement => session.executeAsync(bs.setIdempotent(true))
        case ss: SimpleStatement => session.executeAsync(ss.setIdempotent(true))
        case unknown => throw new IllegalArgumentException(
          s"""Extend SparkCassandraITFlatSpecBase to utilize statement type,
             | currently does not support ${unknown.getClass}""".stripMargin)
      },
      maxConcurrent,
      None,
      None
    )
  }

  def pv = conn.withSessionDo(_.getContext.getProtocolVersion)

  def report(message: String): Unit = alert(message)

  val ks = getKsName

  def skipIfProtocolVersionGTE(protocolVersion: ProtocolVersion)(f: => Unit): Unit = {
    if (!(pv.getCode >= protocolVersion.getCode)) f
    else report(s"Skipped Because ProtocolVersion $pv >= $protocolVersion")
  }

  def skipIfProtocolVersionLT(protocolVersion: ProtocolVersion)(f: => Unit): Unit = {
    if (!(pv.getCode < protocolVersion.getCode)) f
    else report(s"Skipped Because ProtocolVersion $pv < $protocolVersion")
  }

  /** Runs the given test only if the cluster type and version matches.
   *
   * @param cassandra run the test if the cluster is Cassandra >= the given version;
   *                  if `None`, never run the test for Cassandra clusters
   * @param dse       run the test if the cluster is DSE >= the given version;
   *                  if `None`, never run the test for DSE clusters
   * @param f         the test to run
   */
  def from(cassandra: Version, dse: Version)(f: => Unit): Unit = from(Option(cassandra), Option(dse))(f)

  def from(cassandra: Option[Version] = None, dse: Option[Version] = None)(f: => Unit): Unit = {
    if (isDse(conn)) {
      dse match {
        case Some(dseVersion) => from(dseVersion)(f)
        case None => report(s"Skipped because not DSE")
      }
    } else {
      cassandra match {
        case Some(cassandraVersion) => from(cassandraVersion)(f)
        case None => report(s"Skipped because not Cassandra")
      }
    }
  }

  /** Skips the given test if the Cluster Version is lower than the given version */
  private def from(version: Version)(f: => Unit): Unit = {
    skip(cluster.getCassandraVersion, version) { f }
  }

  /** Skips the given test if the cluster is not DSE */
  def dseOnly(f: => Unit): Unit = {
    if (isDse(conn)) f
    else report(s"Skipped because not DSE")
  }

  /** Skips the given test if the cluster is not Cassandra */
  def cassandraOnly(f: => Unit): Unit = {
    if (isDse(conn)) report(s"Skipped because not Cassandra")
    else f
  }

  /** Skips the given test if the Cluster Version is lower than the given version or the cluster is not DSE */
  def dseFrom(version: Version)(f: => Any): Unit = {
    dseOnly {
      skip(cluster.getDseVersion.get, version) { f }
    }
  }

  private def skip(clusterVersion: Version, minVersion: Version)(f: => Unit): Unit = {
    val verOrd = implicitly[Ordering[Version]]
    import verOrd._
    if (clusterVersion >= minVersion) f
    else report(s"Skipped because cluster Version ${cluster.getCassandraVersion} < $minVersion")
  }

  private def isDse(connector: CassandraConnector): Boolean = {
    val firstNodeExtras = connector.withSessionDo(_.getMetadata.getNodes.values().asScala.head.getExtras)
    firstNodeExtras.containsKey(DseNodeProperties.DSE_VERSION)
  }

  implicit val ec = SparkCassandraITSpecBase.ec

  def await[T](unit: Future[T]): T = {
    Await.result(unit, Duration.Inf)
  }

  def awaitAll[T](units: Future[T]*): Seq[T] = {
    Await.result(Future.sequence(units), Duration.Inf)
  }

  def awaitAll[T](units: IterableOnce[Future[T]]): IterableOnce[T] = {
    Await.result(Future.sequence(units.iterator), Duration.Inf)
  }

  def keyspaceCql(name: String = ks) =
    s"""
       |CREATE KEYSPACE IF NOT EXISTS $name
       |WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }
       |AND durable_writes = false
       |""".stripMargin

  def createKeyspace(session: CqlSession, name: String = ks): Unit = {
    val ks_ex = getExecutor(session)
    ks_ex.execute(newInstance(s"DROP KEYSPACE IF EXISTS $name"))
    ks_ex.execute(newInstance(keyspaceCql(name)))
  }

  /**
    * Ensures that the tables exist in the metadata object for this session. This can be
    * an issue with some schema debouncing.
    */
  def awaitTables(tableNames: String*): Unit = {
    eventually(timeout(Span(2, Seconds))) {
      conn.withSessionDo(session =>
        session
          .getMetadata
          .getKeyspace(ks).get()
          .getTables().keySet()
          .containsAll(tableNames.asJava)
      )
    }
  }


  def restoreSystemProps(): Unit = {
    sys.props ++= originalProps
    sys.props --= (sys.props.keySet -- originalProps.keySet)
  }

  def setupCassandraCatalog: Unit = {
    spark.conf.set(s"spark.sql.catalog.cassandra", classOf[CassandraCatalog].getCanonicalName)
    spark.conf.set(SQLConf.DEFAULT_CATALOG.key, "cassandra")
    SparkSession.setActiveSession(spark)
  }

  def getCassandraScan(plan: SparkPlan): CassandraScan = {
    plan.collectLeaves.collectFirst{
      case BatchScanExec(_, cassandraScan: CassandraScan, _, _, _, _) => cassandraScan
    }.getOrElse(throw new IllegalArgumentException("No Cassandra Scan Found"))
  }

  protected def withConfig(params: (String, Any)*)(testFun: => Unit): Unit = {
    SparkSession.setActiveSession(spark)
    val runtimeConf = spark.conf.getAll
    params.foreach { case (k, v) => spark.conf.set(k, v.toString) }
    try {
      testFun
    } finally {
      params.map(_._1).map(spark.conf.unset)
      runtimeConf.foreach{
        case (k, v) if spark.conf.isModifiable(k) =>  spark.conf.set(k, v)
        case _ =>
      }
    }
  }

  protected def withConfig(key: String, value: Any)(testFun: => Unit): Unit = withConfig((key, value)){testFun}


}

object SparkCassandraITSpecBase {
  val executor = Executors.newFixedThreadPool(100)
  val ec = ExecutionContext.fromExecutor(executor)
}
