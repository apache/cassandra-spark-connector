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

package com.datastax.spark.connector.writer

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.config.DefaultDriverOption.{CONNECTION_MAX_REQUESTS, CONNECTION_POOL_LOCAL_SIZE}
import com.datastax.oss.driver.api.core.config.DriverConfigLoader
import com.datastax.oss.driver.internal.core.context.InternalDriverContext
import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cluster.DefaultCluster
import com.datastax.spark.connector.cql.{CassandraConnector, CassandraConnectorConf, DefaultConnectionFactory}

import scala.jdk.CollectionConverters._

/** The session's connection pool can be shared with requests that do not go through a given AsyncExecutor
  * (another executor, a direct session call, a Spark task of the same JVM). When the executor's limit is as
  * large as the pool, the pool runs out of stream ids before the executor reaches its limit, and the driver
  * fails the request with an AllNodesFailedException holding only NodeUnavailableExceptions. */
class AsyncExecutorPoolExhaustionSpec extends SparkCassandraITFlatSpecBase with DefaultCluster {

  override lazy val conn = CassandraConnector(defaultConf)

  private val RequestsPerConnection = 32
  private val RowCount = 2000

  override def beforeClass {
    conn.withSessionDo { session =>
      createKeyspace(session)
      session.execute(s"CREATE TABLE $ks.kv (key INT PRIMARY KEY, value INT)")
    }
  }

  /** A session with a single connection of `RequestsPerConnection` stream ids, so the pool is easy to fill. */
  private def withSmallPoolSession[T](f: CqlSession => T): T = {
    val loader = DefaultConnectionFactory
      .connectorConfigBuilder(CassandraConnectorConf(defaultConf), DriverConfigLoader.programmaticBuilder())
      .withInt(CONNECTION_POOL_LOCAL_SIZE, 1)
      .withInt(CONNECTION_MAX_REQUESTS, RequestsPerConnection)
      .build()
    val session = CqlSession.builder().withConfigLoader(loader).build()
    try f(session) finally session.close()
  }

  private def insertAll(session: CqlSession): Unit = {
    session.execute(s"TRUNCATE $ks.kv")
    val executor = getExecutor(session)
    val insert = session.prepare(s"INSERT INTO $ks.kv (key, value) VALUES (?, ?)")
    awaitAll((0 until RowCount).map(i => executor.executeAsync(insert.bind(Int.box(i), Int.box(i)))))
    session.execute(s"SELECT COUNT(*) FROM $ks.kv").one().getLong(0) shouldBe RowCount
  }

  "AsyncExecutor" should "write all rows when it alone uses the connection pool" in {
    withSmallPoolSession(insertAll)
  }

  it should "write all rows when another request holds a stream id of the pool" in {
    withSmallPoolSession { session =>
      val pools = session.getContext.asInstanceOf[InternalDriverContext].getPoolManager.getPools.values.asScala
      pools.size shouldBe 1
      // ChannelPool.next() reserves a stream id like a request in flight outside the executor; it is never
      // released, so the pool can only run RequestsPerConnection - 1 requests at a time from now on.
      pools.head.next() should not be null
      insertAll(session)
    }
  }
}
