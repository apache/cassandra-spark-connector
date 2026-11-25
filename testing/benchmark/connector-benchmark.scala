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

// Connector benchmark, run inside spark-shell by run-benchmark.sh (see README.md).
// Every result line starts with "BENCH:" so summarize.sh can collect it.

import java.lang.management.ManagementFactory

import scala.jdk.CollectionConverters._

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions.{broadcast, sum}
import org.apache.spark.storage.StorageLevel

val benchTag = sys.env.getOrElse("BENCH_TAG", "local").replaceAll("[^A-Za-z0-9_]", "_")
val rows = sys.env.getOrElse("BENCH_ROWS", "500000").toInt
val runs = sys.env.getOrElse("BENCH_RUNS", "5").toInt
val ks = s"bench_$benchTag"

println(s"BENCH-INFO: tag=$benchTag rows=$rows runs=$runs java=${sys.props("java.version")} spark=${spark.version} " +
  s"rdd.compress=${sc.getConf.get("spark.rdd.compress", "<default>")}")

// In local mode the driver JVM is also the executor, so its GC time covers the whole job.
def gcMillis(): Long = ManagementFactory.getGarbageCollectorMXBeans.asScala.map(_.getCollectionTime.max(0L)).sum

def report(name: String, times: Seq[Long], gc: Long): Unit = {
  val sorted = times.sorted
  println(s"BENCH: $name median_ms=${sorted(sorted.size / 2)} min_ms=${sorted.head} gc_ms_per_run=${gc / times.size} " +
    s"all=${times.mkString(",")}")
}

/** One warm-up run, then `runs` timed runs. */
def time[T](name: String)(f: => T): Unit = {
  f
  val gc0 = gcMillis()
  val times = (1 to runs).map { _ =>
    val t0 = System.nanoTime(); f; (System.nanoTime() - t0) / 1000000
  }
  report(name, times, gcMillis() - gc0)
}

/** Runs `f` once (for writes, which change the data). */
def timeOnce[T](name: String)(f: => T): Unit = {
  val gc0 = gcMillis()
  val t0 = System.nanoTime(); f
  report(name, Seq((System.nanoTime() - t0) / 1000000), gcMillis() - gc0)
}

val connector = CassandraConnector(sc.getConf)
connector.withSessionDo { s =>
  s.execute(s"DROP KEYSPACE IF EXISTS $ks")
  s.execute(s"CREATE KEYSPACE $ks WITH replication = {'class':'SimpleStrategy','replication_factor':1}")
  s.execute(s"CREATE TABLE $ks.kv (k int PRIMARY KEY, v text, n int)")
  s.execute(s"CREATE TABLE $ks.kv2 (k int PRIMARY KEY, v text, n int)")
}

// Writes
val rddData = sc.parallelize(1 to rows, 8).map(i => (i, s"value-$i", i % 100))
timeOnce("rdd_write") { rddData.saveToCassandra(ks, "kv", SomeColumns("k", "v", "n")) }
val dfData = spark.range(1, rows + 1L)
  .selectExpr("cast(id as int) as k", "concat('w', id) as v", "cast(id % 100 as int) as n")
timeOnce("df_write") { dfData.write.cassandraFormat("kv2", ks).mode("append").save() }

val kv = spark.read.cassandraFormat("kv", ks).load()

// Scans
time("rdd_full_scan_count") { sc.cassandraTable(ks, "kv").count() }
time("rdd_full_scan_rows") { sc.cassandraTable(ks, "kv").map(_.getInt("n").toLong).sum() }
time("df_full_scan_count") { kv.count() }
time("df_full_scan_agg") { kv.agg(sum("n")).collect() }

// Pushdown and joins
val keys = (1 to rows by 10).toArray
time("df_in_pushdown_5k") { kv.filter(kv("k").isin(keys.take(5000): _*)).count() }
val keyDf = spark.createDataFrame(keys.toSeq.map(Tuple1(_))).toDF("k")
time("df_direct_join") { keyDf.join(kv, "k").count() }
time("rdd_join_with_cassandra") {
  sc.parallelize(keys.toSeq, 8).map(Tuple1(_)).joinWithCassandraTable(ks, "kv").count()
}
time("df_limit_100") { kv.limit(100).collect() }

// Runtime filtering: joins on the partition key with a small broadcast side that the direct join does not cover
// (semi joins, or direct join off). Spark prunes the scan at runtime; compare with
// --conf spark.sql.optimizer.dynamicPartitionPruning.enabled=false
for ((n, label) <- Seq(100 -> "100", 5000 -> "5k", 50000 -> "50k")) {
  val semiKeys = spark.createDataFrame(keys.take(n).toSeq.map(Tuple1(_))).toDF("k")
  time(s"df_semi_join_$label") { kv.join(semiKeys, Seq("k"), "left_semi").count() }
}
val kvNoDirectJoin = spark.read.cassandraFormat("kv", ks).option("directJoinSetting", "off").load()
val keys5k = spark.createDataFrame(keys.take(5000).toSeq.map(Tuple1(_))).toDF("k")
time("df_broadcast_join_5k_no_direct_join") { kvNoDirectJoin.join(broadcast(keys5k), "k").count() }

// Cache and shuffle of Cassandra rows: spark.rdd.compress only affects serialized storage levels
time("rdd_cache_ser_build_and_read") {
  val cached = sc.cassandraTable(ks, "kv").persist(StorageLevel.MEMORY_ONLY_SER)
  try { cached.count(); cached.map(_.getInt("n").toLong).sum() } finally cached.unpersist(blocking = true)
}
time("rdd_shuffle_reduce_by_key") { sc.cassandraTable(ks, "kv").map(r => (r.getInt("n"), 1L)).reduceByKey(_ + _).count() }
time("df_shuffle_group_by") { kv.groupBy("n").count().collect() }

connector.withSessionDo(_.execute(s"DROP KEYSPACE $ks"))
println("BENCH-INFO: done")
System.exit(0)
