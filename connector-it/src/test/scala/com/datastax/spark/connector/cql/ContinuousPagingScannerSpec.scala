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

package com.datastax.spark.connector.cql

import com.datastax.bdp.spark.ContinuousPagingScanner
import com.datastax.dse.driver.api.core.config.DseDriverOption
import com.datastax.oss.driver.api.core.cql.Statement
import com.datastax.spark.connector._
import com.datastax.spark.connector.cluster.DefaultCluster
import com.datastax.spark.connector.rdd.ReadConf
import org.mockito.ArgumentCaptor
import org.mockito.Mockito._
import org.scalatest.concurrent.Eventually

import scala.concurrent.Future

class ContinuousPagingScannerSpec extends SparkCassandraITFlatSpecBase with DefaultCluster with Eventually {

  sparkConf.set(CassandraConnectionFactory.continuousPagingParam.name, "true")

  override lazy val conn = CassandraConnector(sparkConf)

  override val ks = "continuous_paging"
  val table = "atab"

  override def beforeClass {
    conn.withSessionDo { session =>
      createKeyspace(session)

      awaitAll(
        Future {
          session.execute(s"""CREATE TABLE $ks.test1 (a INT, b INT, c INT, d INT, e INT, f INT, g INT, h INT, PRIMARY KEY ((a, b, c), d , e, f))""")
          session.execute(s"""INSERT INTO $ks.test1 (a, b, c, d, e, f, g, h) VALUES (1, 1, 1, 1, 1, 1, 1, 1)""")
          session.execute(s"""INSERT INTO $ks.test1 (a, b, c, d, e, f, g, h) VALUES (1, 1, 1, 1, 2, 1, 1, 2)""")
          session.execute(s"""INSERT INTO $ks.test1 (a, b, c, d, e, f, g, h) VALUES (1, 1, 1, 2, 1, 1, 2, 1)""")
          session.execute(s"""INSERT INTO $ks.test1 (a, b, c, d, e, f, g, h) VALUES (1, 1, 1, 2, 2, 1, 2, 2)""")
          session.execute(s"""INSERT INTO $ks.test1 (a, b, c, d, e, f, g, h) VALUES (1, 2, 1, 1, 1, 2, 1, 1)""")
          session.execute(s"""INSERT INTO $ks.test1 (a, b, c, d, e, f, g, h) VALUES (1, 2, 1, 1, 2, 2, 1, 2)""")
        }
      )

      session.execute(
        s"""CREATE KEYSPACE IF NOT EXISTS $ks
           |WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }"""
          .stripMargin)
      session.execute(s"CREATE TABLE $ks.$table (p int, c int, d int, PRIMARY KEY (p,c))")
      session.execute(s"INSERT INTO $ks.$table (p,c,d) VALUES (1,1,1)")
    }
  }

  private def executeContinuousPagingScan(readConf: ReadConf): Statement[_] = {
    // we don't want to use the session from CC as mockito is unable to spy on a Proxy
    val cqlSession = conn.conf.connectionFactory.createSession(conn.conf)
    try {
      val sessionSpy = spy(cqlSession)
      val scanner = ContinuousPagingScanner(readConf, conn.conf, IndexedSeq.empty, sessionSpy)
      val stmt = sessionSpy.prepare(s"SELECT * FROM $ks.test1").bind()
      val statementCaptor = ArgumentCaptor.forClass(classOf[Statement[_]])

      scanner.scan(stmt)
      verify(sessionSpy).executeContinuously(statementCaptor.capture())
      statementCaptor.getValue
    } finally {
      cqlSession.close()
    }
  }

  "ContinuousPagingScanner" should "re-use a session in the same thread" in {
    val sessions = for (x <- 1 to 10) yield {
      val cps = ContinuousPagingScanner(ReadConf(), conn.conf, IndexedSeq.empty)
      cps.close()
      cps.getSession()
    }
    sessions.forall(session => session == sessions(0))
  }

  it should "use a different session than the one provided by the default connector" in {
    val scanner = ContinuousPagingScanner(ReadConf(), conn.conf, IndexedSeq.empty)
    scanner.close()
    conn.withSessionDo(session => session shouldNot be(scanner.getSession()))
  }

  it should "use a single CP session for all threads" in {
    CassandraConnector.evictCache()
    eventually {
      CassandraConnector.sessionCache.cache.isEmpty
    }
    val rdd = sc.cassandraTable(ks, table).withReadConf(ReadConf(splitCount = Some(400)))
    rdd.partitions.length should be > 100 //Sanity check that we will have to reuse sessions
    rdd.count
    val sessions = CassandraConnector
      .sessionCache
      .cache
      .keys

    withClue(sessions.map(_.toString).mkString("\n"))(sessions.size should be(1))
  }

  it should "apply MB/s throughput limit" in dseOnly {
    val readConf = ReadConf(throughputMiBPS = Some(32.0))
    val executedStmt = executeContinuousPagingScan(readConf)

    executedStmt.getExecutionProfile.getBoolean(DseDriverOption.CONTINUOUS_PAGING_PAGE_SIZE_BYTES) should be(true)
    executedStmt.getExecutionProfile.getInt(DseDriverOption.CONTINUOUS_PAGING_MAX_PAGES_PER_SECOND) should be(1000)
    executedStmt.getExecutionProfile.getInt(DseDriverOption.CONTINUOUS_PAGING_PAGE_SIZE) should be(33554) // 32MB/s
  }

  it should "apply reads/s throughput limit" in dseOnly {
    val readConf = ReadConf(fetchSizeInRows = 999, readsPerSec = Some(5))
    val executedStmt = executeContinuousPagingScan(readConf)

    executedStmt.getExecutionProfile.getBoolean(DseDriverOption.CONTINUOUS_PAGING_PAGE_SIZE_BYTES) should be(false)
    executedStmt.getExecutionProfile.getInt(DseDriverOption.CONTINUOUS_PAGING_MAX_PAGES_PER_SECOND) should be(5)
    executedStmt.getExecutionProfile.getInt(DseDriverOption.CONTINUOUS_PAGING_PAGE_SIZE) should be(999)
  }

  it should "throw a meaningful exception when pages per second does not fall int (0, Int.MaxValue)" in dseOnly {
    val readConfs = Seq(
      ReadConf(throughputMiBPS = Some(1.0 + Int.MaxValue), readsPerSec = Some(1)),
      ReadConf(throughputMiBPS = Some(-1)),
      ReadConf(throughputMiBPS = Some(0)))

    for (readConf <- readConfs) {
      withClue(s"Expected IllegalArgumentException for invalid throughput argument: ${readConf.throughputMiBPS}.") {
        val exc = intercept[IllegalArgumentException] {
          executeContinuousPagingScan(readConf)
        }
        exc.getMessage should include(s"This number must be positive, non-zero and smaller than ${Int.MaxValue}")
      }
    }
  }

}
