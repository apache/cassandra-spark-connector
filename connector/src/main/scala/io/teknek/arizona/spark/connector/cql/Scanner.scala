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

package io.teknek.arizona.spark.connector.cql

import _root_.io.teknek.arizona.spark.bdp.ScalaJavaUtil.asScalaFuture
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{Row, Statement}
import _root_.io.teknek.arizona.spark.connector.CassandraRowMetadata
import _root_.io.teknek.arizona.spark.connector.rdd.ReadConf
import _root_.io.teknek.arizona.spark.connector.rdd.reader.PrefetchingResultSetIterator
import _root_.io.teknek.arizona.spark.connector.util.maybeExecutingAs
import _root_.io.teknek.arizona.spark.connector.writer.RateLimiter

import scala.concurrent.duration.Duration
import scala.concurrent.{Await}

/**
  * Object which will be used in Table Scanning Operations.
  * One Scanner will be created per Spark Partition, it will be
  * created at the beginning of the compute method and Closed at the
  * end of the compute method.
  */
trait Scanner {
  def close(): Unit
  def getSession(): CqlSession
  def scan[StatementT <: Statement[StatementT]](statement: StatementT): ScanResult
}

case class ScanResult (rows: Iterator[Row], metadata: CassandraRowMetadata)

class DefaultScanner (
    readConf: ReadConf,
    connConf: CassandraConnectorConf,
    columnNames: IndexedSeq[String]) extends Scanner {

  private val session = new CassandraConnector(connConf).openSession()
  private val codecRegistry = session.getContext.getCodecRegistry

  override def close(): Unit = {
    session.close()
  }

  override def scan[StatementT <: Statement[StatementT]](statement: StatementT): ScanResult = {
    import _root_.io.teknek.arizona.spark.connector.util.Threads.BlockingIOExecutionContext

    val rs = session.executeAsync(maybeExecutingAs(statement, readConf.executeAs))
    val scanResult = asScalaFuture(rs).map { rs =>
      val columnMetaData = CassandraRowMetadata.fromResultSet(columnNames, rs, codecRegistry)
      val prefetchingIterator = new PrefetchingResultSetIterator(rs)
      val rateLimitingIterator = readConf.throughputMiBPS match {
        case Some(throughput) =>
          val rateLimiter = new RateLimiter((throughput * 1024 * 1024).toLong, 1024 * 1024)
          prefetchingIterator.map { row =>
            rateLimiter.maybeSleep(getRowBinarySize(row))
            row
          }
        case None =>
          prefetchingIterator
      }
      ScanResult(rateLimitingIterator, columnMetaData)
    }
    Await.result(scanResult, Duration.Inf)
  }

  override def getSession(): CqlSession = session
}
