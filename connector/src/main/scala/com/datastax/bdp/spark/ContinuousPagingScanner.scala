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

package com.datastax.bdp.spark

import java.io.IOException

import com.datastax.dse.driver.api.core.config.DseDriverOption
import com.datastax.dse.driver.api.core.cql.continuous.ContinuousResultSet
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.`type`.codec.TypeCodec
import com.datastax.oss.driver.api.core.cql.{BoundStatement, Statement}
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException
import com.datastax.spark.connector.CassandraRowMetadata
import com.datastax.spark.connector.cql.{CassandraConnectorConf, _}
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector.util.DriverUtil.toName
import com.datastax.spark.connector.util._

import scala.jdk.CollectionConverters._

case class ContinuousPagingScanner(
  readConf: ReadConf,
  connConf: CassandraConnectorConf,
  columnNames: IndexedSeq[String],
  cqlSession: CqlSession) extends Scanner with Logging {

  val TARGET_PAGE_SIZE_IN_BYTES: Int = 5000 * 50 // 5000 rows * 50 bytes per row
  val MIN_PAGES_PER_SECOND = 1000

  private lazy val cpProfile = readConf.throughputMiBPS match {
    case Some(throughput) =>
      val bytesPerSecond = (throughput * 1024 * 1024).toLong
      val fallBackPagesPerSecond = math.max(MIN_PAGES_PER_SECOND, bytesPerSecond / TARGET_PAGE_SIZE_IN_BYTES)
      val pagesPerSecond = readConf.readsPerSec.map(_.toLong).getOrElse(fallBackPagesPerSecond)
      if (readConf.readsPerSec.isEmpty) {
        logInfo(s"Using a pages per second of $pagesPerSecond since " +
          s"${ReadConf.ReadsPerSecParam.name} is not set")
      }
      val bytesPerPage = bytesPerSecond / pagesPerSecond

      if (bytesPerPage <= 0 || bytesPerPage > Int.MaxValue) {
        throw new IllegalArgumentException(
          s"""Read Throttling set to $throughput MBPS, but with the current
             | ${ReadConf.ReadsPerSecParam.name} value of $pagesPerSecond that equates to
             | $bytesPerPage bytes per page.
             | This number must be positive, non-zero and smaller than ${Int.MaxValue}.
           """.stripMargin)
      }

      logDebug(s"Read Throttling set to $throughput mbps. Pages of $bytesPerPage with ${readConf.readsPerSec} max" +
        s"pages per second. ${ReadConf.FetchSizeInRowsParam.name} will be ignored.")
      cqlSession.getContext.getConfig.getDefaultProfile
        .withBoolean(DseDriverOption.CONTINUOUS_PAGING_PAGE_SIZE_BYTES, true)
        .withInt(DseDriverOption.CONTINUOUS_PAGING_PAGE_SIZE, bytesPerPage.toInt)
        .withInt(DseDriverOption.CONTINUOUS_PAGING_MAX_PAGES_PER_SECOND, pagesPerSecond.toInt)

    case None =>
      cqlSession.getContext.getConfig.getDefaultProfile
        .withBoolean(DseDriverOption.CONTINUOUS_PAGING_PAGE_SIZE_BYTES, false)
        .withInt(DseDriverOption.CONTINUOUS_PAGING_PAGE_SIZE, readConf.fetchSizeInRows)
        .withInt(DseDriverOption.CONTINUOUS_PAGING_MAX_PAGES_PER_SECOND, readConf.readsPerSec.getOrElse(0))
  }

  private val codecRegistry = cqlSession.getContext.getCodecRegistry

  private def asBoundStatement(statement: Statement[_]): Option[BoundStatement] = {
    statement match {
      case s: BoundStatement => Some(s)
      case _ => None
    }
  }

  def isSolr(statement: Statement[_]): Boolean = {
    asBoundStatement(statement).exists(_.getPreparedStatement.getQuery.contains("solr_query"))
  }

  /**
    * Calls SessionProxy Close which issues a deferred close request on the session if no
    * references are requested to it in the next keep_alive ms
    */
  override def close(): Unit = cqlSession.close()

  override def getSession(): CqlSession = cqlSession

  override def scan[StatementT <: Statement[StatementT]](statement: StatementT): ScanResult = {
    val authStatement = maybeExecutingAs(statement, readConf.executeAs)

    if (isSolr(authStatement)) {
      logDebug("Continuous Paging doesn't work with Search, Falling back to default paging")
      val regularResult = cqlSession.execute(authStatement)
      val regularIterator = regularResult.iterator().asScala
      ScanResult(regularIterator, CassandraRowMetadata.fromResultSet(columnNames, regularResult, codecRegistry))

    } else {
      try {
        val cpResult = cqlSession.executeContinuously(authStatement.setExecutionProfile(cpProfile))
        val cpIterator = cpResult.iterator().asScala
        ScanResult(cpIterator, getMetaData(cpResult))
      } catch {
        case e: InvalidQueryException if e.getMessage.contains("too many continuous paging sessions are already running") =>
          throw new IOException(s"${e.getMessage}. This error may be intermittent, if there are other applications " +
            s"using continuous paging wait for them to finish and re-execute. If the error persists adjust your DSE " +
            s"server setting `continuous_paging.max_concurrent_sessions` or lower the parallelism level of this job " +
            s"(reduce the number of executors and/or assigned cores) or disable continuous paging for this app " +
            s"with ${CassandraConnectionFactory.continuousPagingParam.name}.", e)
      }
    }
  }

  private def getMetaData(result: ContinuousResultSet): CassandraRowMetadata = {
    import scala.jdk.CollectionConverters._
    val columnDefs = result.getColumnDefinitions.asScala.toSeq
    val rsColumnNames = columnDefs.map(c => toName(c.getName))
    val codecs = columnDefs
      .map(col => codecRegistry.codecFor(col.getType))
      .asInstanceOf[Seq[TypeCodec[AnyRef]]]

    CassandraRowMetadata(columnNames, Some(rsColumnNames.toIndexedSeq), codecs.toIndexedSeq)
  }
}

object ContinuousPagingScanner {
  def apply(
      readConf: ReadConf,
      connConf: CassandraConnectorConf,
      columnNames: IndexedSeq[String]): ContinuousPagingScanner = {
    /**
      * Attempts to get or create a session for this execution thread.
      */
    val cqlSession = new CassandraConnector(connConf).openSession()
    new ContinuousPagingScanner(readConf, connConf, columnNames, cqlSession)
  }
}
