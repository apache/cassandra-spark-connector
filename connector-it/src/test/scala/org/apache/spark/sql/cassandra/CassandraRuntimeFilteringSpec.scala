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

import java.util.UUID

import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cluster.DefaultCluster
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.datasource.{CassandraInJoin, CassandraKeyLookupPartition, CassandraScan}
import com.datastax.spark.connector.rdd.partitioner.CassandraPartition
import org.apache.spark.sql.cassandra.CassandraSourceRelation._
import org.apache.spark.sql.cassandra.execution.CassandraDirectJoinExec
import org.apache.spark.sql.catalyst.expressions.{DynamicPruning, DynamicPruningExpression, Literal}
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.functions.{broadcast, col}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{DataFrame, Row}

import scala.concurrent.Future

/** Runtime filtering (dynamic partition pruning) of Cassandra scans. Every query is also run with Spark's dynamic
  * partition pruning disabled, and must return the same rows. */
class CassandraRuntimeFilteringSpec extends SparkCassandraITFlatSpecBase with DefaultCluster
  with AdaptiveSparkPlanHelper {

  override lazy val conn = CassandraConnector(defaultConf)

  private val Rows = 300
  private val ClusteringRows = 3

  /** Partition key types under test: name, CQL literal of value `i` (or None for a server-side function) */
  private val keyTypes: Seq[(String, Int => String)] = Seq(
    "ascii" -> (i => s"'a$i'"),
    "bigint" -> (i => s"${i * 1000000000000L}"),
    "blob" -> (i => f"0xcafe$i%04x"),
    "boolean" -> (i => s"${i % 2 == 0}"),
    "decimal" -> (i => s"$i.5"),
    "double" -> (i => s"$i.25"),
    "float" -> (i => s"$i.75"),
    "inet" -> (i => if (i % 2 == 0) s"'10.0.0.$i'" else s"'::$i'"),
    "int" -> (i => s"$i"),
    "text" -> (i => s"'t$i'"),
    "timestamp" -> (i => f"'2024-01-01 00:00:$i%02d.123+0000'"),
    "uuid" -> (i => UUID.nameUUIDFromBytes(Array(i.toByte)).toString),
    "varchar" -> (i => s"'v$i'"),
    "varint" -> (i => s"${BigInt(10).pow(30) + i}"),
    "timeuuid" -> (_ => "now()"),
    "smallint" -> (i => s"$i"),
    "tinyint" -> (i => s"$i"),
    "date" -> (i => f"'2024-01-$i%02d'"),
    "time" -> (i => f"'08:00:$i%02d.123456789'")
  )
  /** Cassandra treats these as different keys where Spark sees equal values, so they are never runtime filtered */
  private val notFilteredTypes = Set("decimal", "double", "float")

  override def beforeClass {
    setupCassandraCatalog
    conn.withSessionDo { session =>
      val executor = getExecutor(session)
      createKeyspace(session)
      awaitAll(
        Future {
          session.execute(s"CREATE TABLE $ks.big (k int, c int, v text, PRIMARY KEY (k, c))")
          val ps = session.prepare(s"INSERT INTO $ks.big (k, c, v) VALUES (?, ?, ?)")
          awaitAll(for (k <- 1 to Rows; c <- 1 to ClusteringRows) yield
            executor.executeAsync(ps.bind(k: Integer, c: Integer, s"v$k-$c")))
        },
        Future {
          session.execute(s"CREATE TABLE $ks.composite (a int, b text, c int, v int, PRIMARY KEY ((a, b), c))")
          val ps = session.prepare(s"INSERT INTO $ks.composite (a, b, c, v) VALUES (?, ?, ?, ?)")
          awaitAll(for (a <- 1 to 30; b <- Seq("x", "y"); c <- 1 to 2) yield
            executor.executeAsync(ps.bind(a: Integer, b, c: Integer, (a * c): Integer)))
        },
        Future {
          session.execute(s"""CREATE TABLE $ks.mixed ("MyKey" text PRIMARY KEY, "Value" int)""")
          for (i <- 1 to 20) session.execute(s"""INSERT INTO $ks.mixed ("MyKey", "Value") VALUES ('m$i', $i)""")
        },
        Future {
          session.execute(s"CREATE TABLE $ks.durations (k int PRIMARY KEY, d duration)")
          for (i <- 1 to 20) session.execute(s"INSERT INTO $ks.durations (k, d) VALUES ($i, ${i}h${i}m)")
        },
        Future {
          session.execute(s"CREATE TABLE $ks.counters (k int PRIMARY KEY, n counter)")
          for (i <- 1 to 20) session.execute(s"UPDATE $ks.counters SET n = n + $i WHERE k = $i")
        },
        Future {
          for ((name, literal) <- keyTypes) {
            session.execute(s"CREATE TABLE $ks.key_$name (k $name PRIMARY KEY, v int)")
            for (i <- 1 to 20) session.execute(s"INSERT INTO $ks.key_$name (k, v) VALUES (${literal(i)}, $i)")
          }
          // Different Cassandra keys that Spark reads as equal values
          session.execute(s"INSERT INTO $ks.key_decimal (k, v) VALUES (1.50, 1)")
          session.execute(s"INSERT INTO $ks.key_double (k, v) VALUES (-0.0, 1)")
          session.execute(s"INSERT INTO $ks.key_double (k, v) VALUES (0.0, 1)")
          session.execute(s"INSERT INTO $ks.key_float (k, v) VALUES (-0.0, 1)")
          session.execute(s"INSERT INTO $ks.key_float (k, v) VALUES (0.0, 1)")
        }
      )
    }
  }

  import spark.implicits._

  private def table(name: String): DataFrame = spark.read.cassandraFormat(name, ks).load()

  private def big = table("big")

  /** Small, local key side: Spark plans a broadcast join with it and prunes the Cassandra side. */
  private def keys(values: Int*): DataFrame = values.toDF("k")

  private val NoDirectJoin = DirectJoinSettingParam.name -> "off"

  private def rowString(row: Row): String = row.toSeq.map {
    case bytes: Array[Byte] => bytes.map("%02x".format(_)).mkString("0x", "", "")
    case other => String.valueOf(other)
  }.mkString("|")

  private def rows(df: DataFrame): Seq[String] = df.collect().toSeq.map(rowString)

  private def cassandraScans(df: DataFrame): Seq[BatchScanExec] = collect(df.queryExecution.executedPlan) {
    case scan @ BatchScanExec(_, _: CassandraScan, _, _, _, _) => scan
  }

  /** A dynamic pruning filter Spark did not drop, or a scalar subquery filter */
  private def hasRuntimeFilter(scan: BatchScanExec): Boolean = scan.runtimeFilters.exists {
    case DynamicPruningExpression(Literal.TrueLiteral) => false
    case _ => true
  }

  /** Runs `query` with and without dynamic partition pruning, checks both return the same rows, and returns the
    * partitions of the runtime filtered Cassandra scans. */
  private def runtimeFiltered(query: => DataFrame): Seq[Array[InputPartition]] = {
    val expected = withDpp(enabled = false) { rows(query) }
    val df = query
    val actual = rows(df)
    actual should contain theSameElementsAs expected
    cassandraScans(df).filter(hasRuntimeFilter).map(_.scan.asInstanceOf[CassandraScan].planInputPartitions())
  }

  private def withDpp[T](enabled: Boolean)(f: => T): T = {
    val key = SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key
    val old = spark.conf.getOption(key)
    spark.conf.set(key, enabled.toString)
    try f finally old match {
      case Some(v) => spark.conf.set(key, v)
      case None => spark.conf.unset(key)
    }
  }

  private def lookupKeys(partitions: Array[InputPartition]): Seq[Seq[AnyRef]] = partitions.toSeq.flatMap {
    case p: CassandraKeyLookupPartition => p.keys.toSeq.map(_.toSeq)
    case other => fail(s"Expected key lookups, got $other")
  }

  private def shouldBeKeyLookups(partitions: Seq[Array[InputPartition]], keyCount: Int): Unit = {
    partitions should have size 1
    lookupKeys(partitions.head) should have size keyCount
  }

  private def fullScanPartitions(df: DataFrame): Array[InputPartition] = {
    df.collect()
    cassandraScans(df).head.scan.asInstanceOf[CassandraScan].planInputPartitions()
  }

  private def tokenRangeCount(partitions: Array[InputPartition]): Int = partitions.map {
    case p: CassandraPartition[_, _] => p.tokenRanges.size
    case other => fail(s"Expected token range partitions, got $other")
  }.sum

  for (aqe <- Seq(true, false)) {
    val mode = if (aqe) "with AQE" else "without AQE"

    "A broadcast join on the partition key" should s"read only the joined keys $mode" in
      withConfig(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe, NoDirectJoin) {
        shouldBeKeyLookups(runtimeFiltered(big.join(keys(3, 7, 250, 99999), "k")), keyCount = 4)
      }

    "A semi join on the partition key" should s"read only the joined keys $mode" in
      withConfig(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe) {
        shouldBeKeyLookups(runtimeFiltered(big.join(keys(1 to 10: _*), Seq("k"), "left_semi")), keyCount = 10)
      }
  }

  "A SQL IN subquery on the partition key" should "read only the matching keys" in {
    keys(5, 6, 7).createOrReplaceTempView("rf_in_keys")
    shouldBeKeyLookups(runtimeFiltered(
      spark.sql(s"SELECT * FROM cassandra.$ks.big WHERE k IN (SELECT k FROM rf_in_keys)")), keyCount = 3)
  }

  "A SQL EXISTS subquery on the partition key" should "read only the matching keys" in {
    keys(8, 9).createOrReplaceTempView("rf_exists_keys")
    shouldBeKeyLookups(runtimeFiltered(spark.sql(
      s"SELECT * FROM cassandra.$ks.big b WHERE EXISTS (SELECT 1 FROM rf_exists_keys e WHERE e.k = b.k)")),
      keyCount = 2)
  }

  "A direct join" should "still be planned, without runtime filters left in it" in
    withConfig(DirectJoinSettingParam.name -> "auto") {
      val query = () => big.join(keys(1, 2, 3), "k")
      // Spark adds a runtime filter for this join, which the direct join must drop
      query().queryExecution.optimizedPlan.exists(_.expressions.exists(_.exists(_.isInstanceOf[DynamicPruning]))) shouldBe
        true
      runtimeFiltered(query()) shouldBe empty
      val plan = query().queryExecution.executedPlan
      val directJoins = collect(plan) { case j: CassandraDirectJoinExec => j }
      directJoins should have size 1
      directJoins.head.cassandraPlan.runtimeFilters.exists(_.exists(_.isInstanceOf[DynamicPruning])) shouldBe false
      rows(query()) should have size 3 * ClusteringRows
    }

  "A join on a composite partition key" should "read only the joined keys" in withConfig(NoDirectJoin) {
    val keySide = Seq((1, "x"), (2, "y"), (3, "z")).toDF("a", "b")
    // The cross product of the runtime values: 3 * 3 keys, of which 2 exist
    shouldBeKeyLookups(runtimeFiltered(table("composite").join(keySide, Seq("a", "b"))), keyCount = 9)
  }

  it should "scan all token ranges if only part of the key is joined" in withConfig(NoDirectJoin) {
    val all = fullScanPartitions(table("composite"))
    val partitions = runtimeFiltered(table("composite").join(keys(1, 2).toDF("a"), "a"))
    partitions.foreach(p => p.length shouldBe all.length)
  }

  for ((name, _) <- keyTypes) {
    val filtered = !notFilteredTypes.contains(name)
    s"A semi join on a $name partition key" should (if (filtered) "read only the joined keys" else
      "scan all token ranges") in {
      val t = table(s"key_$name")
      // boolean has two keys only, holding the last two values written
      val (selected, keyCount) = if (name == "boolean") (col("v") >= 19, 2) else (col("v") <= 5, 5)
      val partitions = runtimeFiltered(t.join(broadcast(t.filter(selected).select("k")), Seq("k"), "left_semi"))
      if (filtered) {
        shouldBeKeyLookups(partitions, keyCount)
      } else {
        partitions shouldBe empty
      }
    }
  }

  "A semi join on a quoted mixed case partition key" should "read only the joined keys" in {
    val t = table("mixed")
    shouldBeKeyLookups(
      runtimeFiltered(t.join(broadcast(Seq("m1", "m2", "nope").toDF("MyKey")), Seq("MyKey"), "left_semi")),
      keyCount = 3)
  }

  "Key lookups" should "read duration and counter columns" in withConfig(NoDirectJoin) {
    shouldBeKeyLookups(runtimeFiltered(table("durations").join(keys(1, 2, 3), "k")), keyCount = 3)
    shouldBeKeyLookups(runtimeFiltered(table("counters").join(keys(4, 5), "k")), keyCount = 2)
  }

  they should "apply the pushed down clustering column and regular column filters" in withConfig(NoDirectJoin) {
    val partitions = runtimeFiltered(big.filter(col("c") === 2 && col("v") =!= "v4-2").join(keys(3, 4, 5), "k"))
    shouldBeKeyLookups(partitions, keyCount = 3)
    big.filter(col("c") === 2 && col("v") =!= "v4-2").join(keys(3, 4, 5), "k").count() shouldBe 2
  }

  they should "be read once for duplicate keys and ignore null keys" in withConfig(NoDirectJoin) {
    val keySide = Seq(Some(1), None, Some(1), Some(2), None).toDF("k")
    shouldBeKeyLookups(runtimeFiltered(big.join(keySide, "k")), keyCount = 2)
  }

  they should "return the right count" in withConfig(NoDirectJoin) {
    big.join(keys(1, 2, 3, 99999), "k").count() shouldBe 3 * ClusteringRows
    big.join(keys(1, 2, 3, 99999), Seq("k"), "left_semi").count() shouldBe 3 * ClusteringRows
  }

  "A runtime filter matching no key" should "read nothing" in withConfig(NoDirectJoin) {
    val partitions = runtimeFiltered(big.join(keys(1, 2).filter(col("k") > 100), "k"))
    partitions.foreach(_ shouldBe empty)
  }

  "A runtime filter with more keys than maxKeyLookups" should "scan only the token ranges holding them" in
    withConfig(NoDirectJoin, RuntimeFilteringMaxKeyLookupsParam.name -> 0) {
      val all = fullScanPartitions(big)
      val partitions = runtimeFiltered(big.join(keys(1, 2, 3), "k"))
      partitions should have size 1
      partitions.head.foreach(_ shouldBe a[CassandraPartition[_, _]])
      tokenRangeCount(partitions.head) should be <= 3
      tokenRangeCount(partitions.head) should be < tokenRangeCount(all)
    }

  it should "scan all token ranges if nearly all of them hold a key" in
    withConfig(NoDirectJoin, RuntimeFilteringMaxKeyLookupsParam.name -> 0) {
      val all = fullScanPartitions(big)
      val manyKeys = keys(1 to 3 * tokenRangeCount(all) + 1: _*)
      val partitions = runtimeFiltered(big.join(manyKeys, "k"))
      partitions should have size 1
      partitions.head.length shouldBe all.length
      tokenRangeCount(partitions.head) shouldBe tokenRangeCount(all)
    }

  "A join with a filtered Cassandra table" should "read only the joined keys" in withConfig(NoDirectJoin) {
    val dimension = big.filter(col("k") <= 4 && col("c") === 1).select("k")
    shouldBeKeyLookups(runtimeFiltered(big.join(broadcast(dimension), "k")), keyCount = 4)
  }

  "A scalar subquery on the partition key" should "return the right rows" in {
    keys(11, 12, 13).createOrReplaceTempView("rf_scalar_keys")
    val partitions = runtimeFiltered(
      spark.sql(s"SELECT * FROM cassandra.$ks.big WHERE k = (SELECT max(k) FROM rf_scalar_keys)"))
    shouldBeKeyLookups(partitions, keyCount = 1)
  }

  "A static partition key restriction" should "not get a runtime filter" in withConfig(NoDirectJoin) {
    runtimeFiltered(big.join(keys(1, 2), "k")) should have size 1
    runtimeFiltered(big.filter(col("k").isin(1, 2, 3)).join(keys(1, 2), "k")) shouldBe empty
    runtimeFiltered(big.filter(col("k") === 1).join(keys(1, 2), Seq("k"), "left_semi")) shouldBe empty
  }

  it should "still be converted to a join with the table above the IN threshold" in
    withConfig(NoDirectJoin, InClauseToJoinWithTableConversionThreshold.name -> 2) {
      val query = big.filter(col("k").isin(1, 2, 3)).join(keys(1, 2), Seq("k"), "left_semi")
      runtimeFiltered(query) shouldBe empty
      collect(query.queryExecution.executedPlan) { case s @ BatchScanExec(_, _: CassandraInJoin, _, _, _, _) => s } should
        have size 1
      rows(query) should have size 2 * ClusteringRows
    }

  "Runtime filtering" should "be off when disabled" in
    withConfig(NoDirectJoin, RuntimeFilteringEnabledParam.name -> false) {
      runtimeFiltered(big.join(keys(1, 2), "k")) shouldBe empty
      runtimeFiltered(big.join(keys(1, 2), Seq("k"), "left_semi")) shouldBe empty
      withConfig(RuntimeFilteringEnabledParam.name -> true) {
        runtimeFiltered(big.join(keys(1, 2), "k")) should have size 1
      }
    }

  it should "be off without pushdown" in withConfig(NoDirectJoin) {
    val noPushdown = spark.read.cassandraFormat("big", ks, pushdownEnable = false).load()
    runtimeFiltered(noPushdown.join(keys(1, 2), "k")) shouldBe empty
  }
}
