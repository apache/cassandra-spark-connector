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

package org.apache.spark.sql.cassandra

import java.nio.file.Files

import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cluster.DefaultCluster
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.cassandra.CassandraSourceRelation.WriteClusteringParam
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, RebalancePartitions, Sort}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.functions.{col, rand}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.sql.{DataFrame, Encoder, Row}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

/** DataFrame writes clustered by partition key and sorted by primary key (spark.cassandra.sql.writeClustering). */
class CassandraWriteClusteringSpec extends SparkCassandraITFlatSpecBase with DefaultCluster {

  override lazy val conn = CassandraConnector(defaultConf)

  private val tables = Seq("wide_off", "wide_on", "wide_auto", "wide_ttl", "wide_overwrite", "wide_sql",
    "wide_stream")

  override def beforeClass {
    setupCassandraCatalog
    conn.withSessionDo { session =>
      createKeyspace(session)
      for (t <- tables) session.execute(s"CREATE TABLE $ks.$t (p int, c int, v text, PRIMARY KEY (p, c))")
      session.execute(s"CREATE TABLE $ks.narrow (k int PRIMARY KEY, v text)")
      session.execute(s"CREATE TABLE $ks.counters (k int, c int, n counter, PRIMARY KEY (k, c))")
      session.execute(
        s"""CREATE TABLE $ks.mixed ("Part" int, part2 text, "Clu" int, "Value" text,
           |PRIMARY KEY (("Part", part2), "Clu"))""".stripMargin)
    }
  }

  import spark.implicits._

  /** 2000 partitions x 5 rows in random order, some rows twice with the same values */
  private def wideInput: DataFrame = spark.range(10000).union(spark.range(500))
    .selectExpr("cast(id % 2000 as int) as p", "cast(id div 2000 as int) as c", "concat('v', id) as v")
    .orderBy(rand(7))

  private def expectedWide: Seq[String] = (0 until 10000).map(id => s"${id % 2000}|${id / 2000}|v$id").sorted

  private def read(table: String): Seq[String] =
    spark.read.cassandraFormat(table, ks).load().select("p", "c", "v").collect().toSeq
      .map(r => s"${r.getInt(0)}|${r.getInt(1)}|${r.getString(2)}").sorted

  /** The optimized plans of the queries run by `f` */
  private def plansOf(f: => Unit): Seq[LogicalPlan] = {
    val plans = mutable.ArrayBuffer.empty[LogicalPlan]
    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit =
        plans.synchronized(plans += qe.optimizedPlan)
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}
    }
    spark.listenerManager.register(listener)
    try {
      f
      spark.sparkContext.listenerBus.waitUntilEmpty()
    } finally spark.listenerManager.unregister(listener)
    plans.synchronized(plans.toList)
  }

  private def names(expressions: Seq[org.apache.spark.sql.catalyst.expressions.Expression]): Seq[String] =
    expressions.map {
      case a: Attribute => a.name
      case SortOrder(a: Attribute, _, _, _) => a.name
      case other => other.sql
    }

  /** Partition expressions of the write's rebalance and sort order, if the write was clustered */
  private def clusteringOf(plans: Seq[LogicalPlan]): Option[(Seq[String], Seq[String])] = {
    val rebalances = plans.flatMap(_.collect { case r: RebalancePartitions => names(r.partitionExpressions) })
    val sorts = plans.flatMap(_.collect { case s: Sort if !s.global => names(s.order) })
    rebalances.headOption.map(r => (r, sorts.headOption.getOrElse(Seq.empty)))
  }

  private def write(df: DataFrame, table: String, options: (String, String)*): Seq[LogicalPlan] =
    plansOf(df.write.cassandraFormat(table, ks).options(options.toMap).mode("append").save())

  "A DataFrame write" should "not be clustered by default" in {
    clusteringOf(write(wideInput, "wide_off")) shouldBe None
    read("wide_off") shouldBe expectedWide
  }

  it should "be clustered by partition key and sorted by primary key when on" in {
    clusteringOf(write(wideInput, "wide_on", WriteClusteringParam.name -> "on")) shouldBe
      Some((Seq("p"), Seq("p", "c")))
    read("wide_on") shouldBe expectedWide
  }

  it should "be clustered in auto mode for a table with clustering columns" in
    withConfig(WriteClusteringParam.name, "auto") {
      clusteringOf(write(wideInput, "wide_auto")) shouldBe Some((Seq("p"), Seq("p", "c")))
      read("wide_auto") shouldBe expectedWide
    }

  it should "not be clustered in auto mode for a table without clustering columns" in
    withConfig(WriteClusteringParam.name, "auto") {
      val input = (1 to 100).map(i => (i, s"v$i")).toDF("k", "v")
      clusteringOf(write(input, "narrow")) shouldBe None
      spark.read.cassandraFormat("narrow", ks).load().count() shouldBe 100
    }

  it should "keep the per-row and constant TTL" in withConfig(WriteClusteringParam.name, "on") {
    val input = wideInput.withColumn("t", (col("p") + 1000).cast("int"))
    clusteringOf(write(input, "wide_ttl", "ttl" -> "t")) shouldBe defined
    val ttls = conn.withSessionDo(_.execute(s"SELECT p, ttl(v) FROM $ks.wide_ttl").all().asScala
      .map(r => (r.getInt(0), r.getInt(1))))
    ttls should have size 10000
    all(ttls.map { case (p, ttl) => ttl <= p + 1000 && ttl > p + 900 }) shouldBe true

    write(wideInput.filter(col("p") === 1), "wide_ttl", "ttl" -> "50000")
    conn.withSessionDo(_.execute(s"SELECT ttl(v) FROM $ks.wide_ttl WHERE p = 1").all().asScala.map(_.getInt(0)))
      .foreach(_ should be > 49000)
  }

  it should "work with overwrite (truncate)" in withConfig(WriteClusteringParam.name, "on") {
    write(wideInput.filter(col("p") < 10), "wide_overwrite")
    val plans = plansOf(wideInput.write.cassandraFormat("wide_overwrite", ks)
      .option("confirm.truncate", "true").mode("overwrite").save())
    clusteringOf(plans) shouldBe defined
    read("wide_overwrite") shouldBe expectedWide
  }

  it should "work with SQL INSERT through the catalog" in withConfig(WriteClusteringParam.name, "on") {
    wideInput.createOrReplaceTempView("wc_input")
    clusteringOf(plansOf(spark.sql(s"INSERT INTO cassandra.$ks.wide_sql SELECT p, c, v FROM wc_input"))) shouldBe
      defined
    read("wide_sql") shouldBe expectedWide
  }

  it should "work with a composite partition key and quoted mixed case names" in
    withConfig(WriteClusteringParam.name, "on") {
      val input = (0 until 1000).map(i => (i % 50, s"x${i % 3}", i, s"v$i")).toDF("Part", "part2", "Clu", "Value")
        .orderBy(rand(3))
      clusteringOf(write(input, "mixed")) shouldBe Some((Seq("Part", "part2"), Seq("Part", "part2", "Clu")))
      spark.read.cassandraFormat("mixed", ks).load().count() shouldBe 1000
    }

  it should "keep counter updates exact" in {
    val input = (0 until 3000).map(i => (i % 30, i % 7, 1L)).toDF("k", "c", "n")
    withConfig(WriteClusteringParam.name, "auto") {
      clusteringOf(write(input, "counters")) shouldBe None
    }
    withConfig(WriteClusteringParam.name, "on") {
      clusteringOf(write(input, "counters")) shouldBe defined
    }
    val counts = spark.read.cassandraFormat("counters", ks).load().collect()
      .map(r => ((r.getAs[Int]("k"), r.getAs[Int]("c")), r.getAs[Long]("n"))).toMap
    val expected = (0 until 3000).groupBy(i => (i % 30, i % 7)).view.mapValues(_.size * 2L).toMap
    counts shouldBe expected
  }

  "A streaming write" should "be clustered per micro-batch" in withConfig(WriteClusteringParam.name, "on") {
    implicit val encoder: Encoder[(Int, Int, String)] = spark.implicits.newProductEncoder[(Int, Int, String)]
    val memory = MemoryStream[(Int, Int, String)](encoder, spark.sqlContext)
    val query = memory.toDF().toDF("p", "c", "v").writeStream
      .option("checkpointLocation", Files.createTempDirectory("wc").toString)
      .cassandraFormat("wide_stream", ks)
      .outputMode(OutputMode.Append())
      .start()
    try {
      memory.addData((0 until 5000).map(id => (id % 1000, id / 1000, s"v$id")))
      query.processAllAvailable()
      memory.addData((5000 until 10000).map(id => (id % 1000, id / 1000, s"v$id")))
      query.processAllAvailable()
    } finally query.stop()
    query.exception shouldBe None
    read("wide_stream") shouldBe (0 until 10000).map(id => s"${id % 1000}|${id / 1000}|v$id").sorted
  }
}
