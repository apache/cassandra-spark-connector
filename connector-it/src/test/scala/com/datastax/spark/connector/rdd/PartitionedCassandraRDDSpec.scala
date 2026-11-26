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

package com.datastax.spark.connector.rdd

import java.lang.{Integer => JInt}
import java.util.concurrent.CompletableFuture

import com.datastax.oss.driver.internal.core.metadata.token.Murmur3Token
import com.datastax.spark.connector._
import com.datastax.spark.connector.cluster.DefaultCluster
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.rdd.partitioner.ReplicaPartitioner
import org.apache.spark.rdd.RDD
import org.scalatest.OptionValues

import scala.concurrent.Future
import scala.reflect.ClassTag

case class PKey(key: Int)

case class PKeyCKey(key: Int, ckey: Int)

case class X(x: Int)

case class WeirdMapping(weirdkey: Int, weirdcol: Int)

class PartitionedCassandraRDDSpec extends SparkCassandraITFlatSpecBase with DefaultCluster with OptionValues {

  override lazy val conn = CassandraConnector(defaultConf)
  val rowCount = 100

  override def beforeClass {
    conn.withSessionDo { session =>
      createKeyspace(session)
      createKeyspace(session, """"partitionQuotedKeyspace"""")
      val executor = getExecutor(session)
      
      awaitAll(
        Future {
          session.execute("""CREATE TABLE "partitionQuotedKeyspace".test (key INT, value INT, PRIMARY KEY (key))""")
        },
        Future {
          session.execute(
            s"""CREATE TABLE $ks.table1 (key INT, ckey INT, value INT, PRIMARY KEY (key, ckey)
               |)""".stripMargin)
          val ps = session.prepare( s"""INSERT INTO $ks.table1 (key, ckey, value) VALUES (?, ?, ?)""")
          awaitAll (
            for (value <- 1 to rowCount) yield {
              executor.executeAsync(ps.bind(value: JInt, value: JInt, value: JInt))
            }
          )
        },
        Future {
          session.execute(
            s"""CREATE TABLE $ks.a (x INT, a INT, b INT, PRIMARY KEY (x)
               |)""".stripMargin)
          val ps = session.prepare( s"""INSERT INTO $ks.a (x, a, b) VALUES (?, ?, ?)""")
          awaitAll (
            for (value <- 1 to rowCount) yield {
              executor.executeAsync(ps.bind(value: JInt, value: JInt, value: JInt))
            }
          )
        },
        Future {
          session.execute(
            s"""CREATE TABLE $ks.b (y INT, c INT, d INT, PRIMARY KEY (y)
               |)""".stripMargin)
          val ps = session.prepare( s"""INSERT INTO $ks.b (y, c, d) VALUES (?, ?, ?)""")
          awaitAll (
            for (value <- 1 to rowCount) yield {
              executor.executeAsync(ps.bind(value: JInt, value: JInt, value: JInt))
            }
          )
        },
        Future {
          session.execute(
            s"""CREATE TABLE $ks.table2 (key INT, ckey INT, value INT, PRIMARY KEY
               |(key, ckey))""".stripMargin)
          val ps = session.prepare( s"""INSERT INTO $ks.table2 (key, ckey, value) VALUES (?, ?, ?)""")
          awaitAll (
            for (value <- 1 to rowCount) yield {
              executor.executeAsync(ps.bind(value: JInt, value: JInt, (rowCount - value): JInt))
            }
          )
        },
        Future {
          session.execute(
            s"""CREATE TABLE $ks.table3 (key INT, value INT, PRIMARY KEY (key))""".stripMargin)
        }
      )
      executor.waitForCurrentlyExecutingTasks()
    }
  }

  // Make sure that all tests have enough partitions to make things interesting
  val customReadConf = ReadConf(splitCount = Some(20))

  val testRDD = sc.cassandraTable(ks, "table1").withReadConf(customReadConf)
  val joinTarget = sc.cassandraTable(ks, "table2")

  def getPartitionMap[T](rdd: RDD[T]): Map[T, Int] = {
    rdd.mapPartitionsWithIndex { case (index, it) =>
      it.map(row => (row, index))
    }.collect.toMap
  }

  def checkPartitionerKeys[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]): Unit = {
    rdd.partitioner shouldBe defined
    val partitioner = rdd.partitioner.get
    val realPartitionMap = getPartitionMap(rdd.keys)
    for ((row, partId) <- realPartitionMap) {
      partitioner.getPartition(row) should be(partId)
    }
  }

  "A CassandraPartitioner" should " be creatable from a generic CassandraTableRDD" in {
    val rdd = testRDD
    val partitioner = rdd.partitionGenerator.partitioner[PKey](PartitionKeyColumns)
    partitioner.get.numPartitions should be(rdd.partitions.length)
  }

  "A ReplicaPartitioner" should "be able to handle tokens that hash to Integer.MIN_VALUE" in {
    val keyForMinValueHash = -1478051534 // corresponding token hashes to Integer.MIN_VALUE
    val testRDDWithMagicRow = sc.parallelize(Seq((keyForMinValueHash, 0)))
    testRDDWithMagicRow.repartitionByCassandraReplica(ks, "table3").saveToCassandra(ks, "table3")

    // verify token actually hashes to Integer.MIN_VALUE
    conn.withSessionDo { session =>
      val row = session.execute(s"SELECT token(key) as tt FROM $ks.table3 where key=$keyForMinValueHash").one
      row.getToken("tt").hashCode() shouldBe Integer.MIN_VALUE
    }
  }

  it should "be able to handle keyspace with quoted name" in {
    val partitioner = sc.parallelize(Seq((1, 0)))
      .repartitionByCassandraReplica("partitionQuotedKeyspace", "test")
      .partitioner
      .value
      .asInstanceOf[ReplicaPartitioner[_]]

    conn.withSessionDo { session =>
      session.getMetadata.getTokenMap.get()
        .getReplicas(partitioner._keyspace, new Murmur3Token(0L)) should not be empty
    }
  }

  "keyBy" should "create a partitioned RDD without any parameters" in {
    val keyedRDD = testRDD.keyBy[CassandraRow]
    checkPartitionerKeys(keyedRDD)
  }

  it should "create a partitioned RDD selecting the Partition Key" in {
    val keyedRDD = testRDD.keyBy[CassandraRow](PartitionKeyColumns)
    checkPartitionerKeys(keyedRDD)
  }

  it should "create a partitioned RDD when the partition key is mapped to something else" in {
    val keyedRDD = testRDD.keyBy[CassandraRow](SomeColumns("key" as "notkey"))
    checkPartitionerKeys(keyedRDD)
  }

  it should "create a partitioned RDD with a case class" in {
    val keyedRDD = testRDD.keyBy[PKey](SomeColumns("key"))
    checkPartitionerKeys(keyedRDD)
  }

  it should "create a partitioned RDD with a case class with more than the Partition Key" in {
    val keyedRDD = testRDD.keyBy[PKeyCKey]
    checkPartitionerKeys(keyedRDD)
  }

  it should "NOT create a partitioned RDD that does not cover the Partition Key" in {
    val keyedRDD = testRDD.keyBy[Tuple1[Int]](SomeColumns("ckey"))
    keyedRDD.partitioner.isEmpty should be(true)
  }

  "CassandraTableScanRDD " should " not have a partitioner by default" in {
    testRDD.partitioner should be(None)
  }

  it should " be able to be assigned a partitioner from RDD with the same key" in {
    val keyedRDD = testRDD.keyBy[PKey](SomeColumns("key"))
    val otherRDD = joinTarget.keyBy[PKey](SomeColumns("key")).applyPartitionerFrom(keyedRDD)
    checkPartitionerKeys(otherRDD)
  }

  it should " be joinable against an RDD without a partitioner" in {
    val keyedRDD = testRDD.keyBy[PKey](SomeColumns("key"))
    val joinedRDD = keyedRDD.join(sc.parallelize(1 to rowCount).map(x => Tuple2(PKey(x), -1 * x)))
    val results = joinedRDD.values.collect
    results should have length (rowCount)
    for (row <- results) {
      row._1.getInt("key") should be(-row._2)
    }
  }

  it should "not shuffle during a join with an RDD with the same partitioner" in {
    val keyedRDD = testRDD.keyBy[PKey](SomeColumns("key"))
    val otherRDD = joinTarget.keyBy[PKey](SomeColumns("key")).applyPartitionerFrom(keyedRDD)
    val joinRDD = keyedRDD.join(otherRDD)
    joinRDD.toDebugString should not contain ("+-")
    // "+-" in the debug string means there is more than 1 stage and thus a shuffle
  }

  it should "correctly join against an RDD with the same partitioner" in {
    val keyedRDD = testRDD.keyBy[PKey](SomeColumns("key"))
    val otherRDD = joinTarget.keyBy[PKey](SomeColumns("key")).applyPartitionerFrom(keyedRDD)
    val joinRDD = keyedRDD.join(otherRDD)
    val results = joinRDD.values.collect()
    results should have length (rowCount)
    for (row <- results) {
      row._1.getInt("key") should be(row._2.getInt("key"))
    }
  }

  it should "join against an RDD with different partition key names without a shuffle " in {
    val a = sc.cassandraTable[(Int, Int)](ks, "a")
      .withReadConf(customReadConf)
      .select("a" as "_1", "b" as "_2", "x")
      .keyBy[Tuple1[Int]]("x")

    val b = sc.cassandraTable[(Int, Int)](ks, "b")
      .select("c" as "_1", "d" as "_2", "y")
      .keyBy[Tuple1[Int]]("y")
      .applyPartitionerFrom(a)

    a.partitioner.get should be(b.partitioner.get)
    val joinRDD = a.join(b)
    val results = joinRDD.values.collect()
    results should have length (rowCount)
    joinRDD.toDebugString should not contain ("+-")
    for (row <- results) {
      row._1 should be(row._2)
    }
  }

  it should "join against an RDD with different names and mappings without a shuffle" in {
    val a = sc.cassandraTable[(Int, Int)](ks, "a")
      .withReadConf(customReadConf)
      .select("a" as "_1", "b" as "_2", "x")
      .keyBy[X]("x")

    val b_prime = sc.cassandraTable[(Int, Int)](ks, "b")
      .select("c" as "_1", "d" as "_2", "y" as "x")
      .keyBy[X]("y" as "x")

    b_prime.partitioner shouldBe defined

    val b = b_prime.applyPartitionerFrom(a)

    a.partitioner.get should be(b.partitioner.get)
    val joinRDD = a.join(b)
    val results = joinRDD.values.collect()
    results should have length (rowCount)
    joinRDD.toDebugString should not contain ("+-")
    for (row <- results) {
      row._1 should be(row._2)
    }
  }

  it should "join against an RDD with strange mapping without a shuffle" in {
    val a = sc.cassandraTable[(Int, Int)](ks, "a")
      .withReadConf(customReadConf)
      .select("a" as "_1", "b" as "_2", "x" as "weirdkey")
      .keyBy[WeirdMapping]("x" as "weirdkey", "b" as "weirdcol")

    a.partitioner shouldBe defined

    val b_prime = sc.cassandraTable[(Int, Int)](ks, "b")
      .select("c" as "_1", "d" as "_2", "y" as "weirdkey")
      .keyBy[WeirdMapping]("y" as "weirdkey", "d" as "weirdcol")

    b_prime.partitioner shouldBe defined

    val b = b_prime.applyPartitionerFrom(a)

    a.partitioner.get should be(b.partitioner.get)
    val joinRDD = a.join(b)
    val results = joinRDD.values.collect()
    results should have length (rowCount)
    joinRDD.toDebugString should not contain ("+-")
    for (row <- results) {
      row._1 should be(row._2)
    }
  }

  it should "not shuffle in a keyed self-join" in {
    val keyedRDD = testRDD.keyBy[PKey](SomeColumns("key"))
    val joinRDD = keyedRDD.join(keyedRDD)
    val results = joinRDD.values.collect()
    results should have length (rowCount)
    joinRDD.toDebugString should not contain ("+-")
    for (row <- results) {
      row._1.getInt("key") should be(row._2.getInt("key"))
    }
  }

  "CassandraTableScanPairRDDFunctions" should "not apply an empty partitioner" in {
    val keyedRDD = testRDD.keyBy[Tuple1[Int]](SomeColumns("ckey"))
    intercept[IllegalArgumentException] {
      joinTarget.keyBy[Tuple1[Int]](SomeColumns("key")).applyPartitionerFrom(keyedRDD)
    }
  }

  "CassandraTableScanRDDFunctions" should "key and apply another RDD" in {
    val keyedRDD = testRDD.keyBy[PKey]
    val otherRDD = joinTarget.keyAndApplyPartitionerFrom(keyedRDD)
    val joinRDD = keyedRDD.join(otherRDD)
    joinRDD.toDebugString should not contain ("+-")
    val results = joinRDD.values.collect()
    results should have length (rowCount)
    for (row <- results) {
      row._1.getInt("key") should be(row._2.getInt("key"))
    }


  }

}
