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

package com.datastax.spark.connector.rdd.reader

import com.codahale.metrics.Timer
import com.datastax.oss.driver.api.core.cql.SimpleStatement.newInstance
import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cluster.DefaultCluster
import com.datastax.spark.connector.cql.CassandraConnector
import org.scalatest.concurrent.Eventually.{eventually, timeout}
import org.scalatest.time.{Seconds, Span}

class PrefetchingResultSetIteratorSpec extends SparkCassandraITFlatSpecBase with DefaultCluster {

  private val table = "prefetching"
  private val emptyTable = "empty_prefetching"
  override lazy val conn = CassandraConnector(sparkConf)

  override def beforeClass {
    conn.withSessionDo { session =>
      session.execute(
        s"CREATE KEYSPACE IF NOT EXISTS $ks WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }")

      session.execute(
        s"CREATE TABLE IF NOT EXISTS $ks.$table (key INT, x INT, PRIMARY KEY (key))")

      session.execute(
        s"CREATE TABLE IF NOT EXISTS $ks.$emptyTable (key INT, x INT, PRIMARY KEY (key))")

      awaitAll(
        for (i <- 1 to 999) yield {
          executor.executeAsync(newInstance(s"INSERT INTO $ks.$table (key, x) values ($i, $i)"))
        }
      )
    }
  }

  "PrefetchingResultSetIterator" should "return all rows regardless of the  page sizes" in {
    val pageSizes = Seq(1, 2, 5, 111, 998, 999, 1000, 1001)
    for (pageSize <- pageSizes) {
      withClue(s"Prefetching iterator failed for the page size: $pageSize") {
        val statement = newInstance(s"select * from $ks.$table").setPageSize(pageSize)
        val result = executor.executeAsync(statement).map(new PrefetchingResultSetIterator(_))
        await(result).toList should have size 999
      }
    }
  }

  it should "be empty for an empty table" in {
    val statement = newInstance(s"select * from $ks.$emptyTable")
    val result = executor.executeAsync(statement).map(new PrefetchingResultSetIterator(_))

    await(result).hasNext should be(false)
    intercept[NoSuchElementException] {
      await(result).next()
    }
  }

  it should "update the provided timer" in {
    val statement = newInstance(s"select * from $ks.$table").setPageSize(200)
    val timer = new Timer()
    val result = executor.executeAsync(statement).map(rs => new PrefetchingResultSetIterator(rs, Option(timer)))
    await(result).toList

    eventually(timeout(Span(2, Seconds))) {
      timer.getCount should be(4)
    }
  }
}