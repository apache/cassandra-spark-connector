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

package com.datastax.spark.connector.sql

import com.datastax.spark.connector._
import com.datastax.spark.connector.cluster.DefaultCluster
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.util.CatalystUtil
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.cassandra._
import org.apache.spark.storage.StorageLevel

/**
  * Behaviours of Apache Spark 4.3 that touch the connector: nondeterministic filters are no longer pushed into
  * DSv2 scans, `spark.rdd.compress` is on by default and equivalent scans of one relation can be planned together.
  * None of them may change what the connector returns.
  */
class CassandraSpark43BehaviorSpec extends SparkCassandraITFlatSpecBase with DefaultCluster {

  override lazy val conn = CassandraConnector(defaultConf)

  private val rowCount = 1000

  override def beforeClass {
    conn.withSessionDo { session =>
      createKeyspace(session)
      session.execute(s"CREATE TABLE $ks.kv (k INT PRIMARY KEY, v TEXT)")
      session.execute(s"CREATE TABLE $ks.events (pk INT, ck INT, v INT, PRIMARY KEY (pk, ck))")
    }
    sc.parallelize(1 to rowCount).map(i => (i, s"v$i")).saveToCassandra(ks, "kv", SomeColumns("k", "v"))
    sc.parallelize(for (pk <- 1 to 10; ck <- 1 to 20) yield (pk, ck, pk * 100 + ck))
      .saveToCassandra(ks, "events", SomeColumns("pk", "ck", "v"))
  }

  private def table(name: String): DataFrame = spark.read.cassandraFormat(name, ks).load()

  private def pushedToCassandra(df: DataFrame): String =
    CatalystUtil.findCassandraScan(df.queryExecution.executedPlan)
      .map(_.cqlQueryParts.whereClause.predicates.mkString(" "))
      .getOrElse(fail(s"No Cassandra scan found in\n${df.queryExecution.executedPlan}"))

  "A DataFrame read" should "keep pushing a deterministic filter and not push a nondeterministic one" in {
    val df = table("kv").filter("k = 7 AND rand() >= 0.0")

    df.collect().map(r => (r.getInt(0), r.getString(1))).toList should be(List((7, "v7")))

    val pushed = pushedToCassandra(df)
    pushed should include("k")
    pushed.toLowerCase should not include "rand"
  }

  it should "return the same rows for a nondeterministic filter that is always true or always false" in {
    table("kv").filter("rand() >= 0.0").count() should be(rowCount)
    table("kv").filter("rand() < 0.0").count() should be(0)
    table("events").filter("pk = 3 AND ck > 5 AND rand() >= 0.0").count() should be(15)
  }

  it should "give the same result for two scans of one table that differ in filters and columns" in {
    val one = table("kv").filter("k = 1").select("k", "v")
    val two = table("kv").filter("k = 2").select("k")

    one.union(two.selectExpr("k", "'x' AS v")).collect().map(r => (r.getInt(0), r.getString(1))).toSet should be(
      Set((1, "v1"), (2, "x")))

    val all = table("kv")
    all.as("a").join(all.as("b"), "k").count() should be(rowCount)
    all.filter("k <= 10").union(all.filter("k > 990")).count() should be(20)
  }

  "An RDD read" should "round trip through serialized caching and a shuffle" in {
    val rdd = sc.cassandraTable[(Int, String)](ks, "kv").select("k", "v")
    val expected = (1 to rowCount).map(i => (i, s"v$i")).toSet

    val cached = rdd.persist(StorageLevel.MEMORY_ONLY_SER)
    try {
      cached.collect().toSet should be(expected)
      cached.collect().toSet should be(expected) // second read comes from the (compressed) cache
      cached.repartition(7).collect().toSet should be(expected)
      cached.map { case (k, _) => k.toLong }.sum().toLong should be(rowCount.toLong * (rowCount + 1) / 2)
    } finally {
      cached.unpersist()
    }
  }
}
