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

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.spark.connector._
import com.datastax.spark.connector.cluster.DefaultCluster
import com.datastax.spark.connector.cql.CassandraConnector

class InnerJoinSpec extends SparkCassandraITFlatSpecBase with DefaultCluster {

  override lazy val conn = CassandraConnector(sparkConf)

  override def beforeClass {
    conn.withSessionDo { session =>
      session.execute(
        s"""CREATE KEYSPACE IF NOT EXISTS $ks
            |WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }"""
          .stripMargin)
      makeSimpleJoinTables(session)
      makeComplexJoinTables(session)
    }
  }

  def makeSimpleJoinTables(session: CqlSession): Unit = {
    session.execute(
      s"""CREATE TABLE IF NOT EXISTS $ks.leftjoin( key INT, foo TEXT, PRIMARY KEY
         |(key))""".stripMargin)
    session.execute(
      s"""CREATE TABLE IF NOT EXISTS $ks.rightjoin( key INT, bar TEXT, PRIMARY KEY
         | (key))""".stripMargin)
    session.execute(
      s"""CREATE TABLE IF NOT EXISTS $ks.thirdjoin( key INT, third TEXT, PRIMARY KEY
         | (key))""".stripMargin)
    for (i <- 1 to 100) {
      session.execute(s"INSERT INTO $ks.leftjoin (key, foo) values ($i, 'foo$i')")
      session.execute(s"INSERT INTO $ks.rightjoin (key, bar) values ($i, 'bar$i')")
      session.execute(s"INSERT INTO $ks.thirdjoin (key, third) values ($i, 'third$i')")
    }
  }

  def makeComplexJoinTables(session: CqlSession): Unit = {
    session.execute(
      s"""CREATE TABLE IF NOT EXISTS $ks.complexleftjoin
         |(key1 INT, key2 INT, c TEXT, foo TEXT, PRIMARY KEY ((key1, key2), c))""".stripMargin)
    session.execute(
      s"""CREATE TABLE IF NOT EXISTS $ks.complexrightjoin
         |(key1 INT, key2 INT, c TEXT, bar TEXT, PRIMARY KEY ((key1, key2), c))""".stripMargin)
    session.execute(
      s"""CREATE TABLE IF NOT EXISTS $ks.complexthirdjoin
         |(key1 INT, key2 INT, c TEXT, third TEXT, PRIMARY KEY ((key1, key2), c))""".stripMargin)
    for (key1 <- 1 to 10; key2 <- 1 to 10; c <- 1 to 3) {
      session.execute(
        s"""INSERT INTO $ks.complexleftjoin (key1, key2, c, foo)
          |values ($key1, $key2, '$c', 'foo$c')""".stripMargin)
      if (key2 % 2 == 0)
        session.execute(
          s"""INSERT INTO $ks.complexrightjoin (key1, key2, c, bar)
            |values ($key1,  $key2, '$c', 'foo$c')""".stripMargin)
      else
        session.execute(
          s"""INSERT INTO $ks.complexthirdjoin (key1, key2, c, third)
             |values ($key1,  $key2, '$c', 'third$c')""".stripMargin)
    }
  }


  "MergeJoinRDD" should "be able to merge two C* tables together that share a partition key" in {
    val leftRDD = sc.cassandraTable(ks, "leftjoin")
    val rightRDD = sc.cassandraTable(ks, "rightjoin")
    val merged = new CassandraMergeJoinRDD(sc, leftRDD, rightRDD)
    val results = merged.collect()
    results should have size (100)
    for (i <- 0 until 100) {
      val key = results(i)._1(0).getInt("key")
      val left = results(i)._1
      val right = results(i)._2
      left(0).getString("foo") should be(s"foo$key")
      right(0).getString("bar") should be(s"bar$key")
    }
  }

  it should "be able to merge two C* tables together with multiple spark partitions" in {
    implicit val readConf = ReadConf(splitCount = Some(10))
    val leftRDD = sc.cassandraTable(ks, "leftjoin")
    val rightRDD = sc.cassandraTable(ks, "rightjoin")
    val merged = new CassandraMergeJoinRDD(sc, leftRDD, rightRDD)
    val results = merged.collect()
    results should have size (100)
    for (i <- 0 until 100) {
      val key = results(i)._1(0).getInt("key")
      val left = results(i)._1
      val right = results(i)._2
      left(0).getString("foo") should be(s"foo$key")
      right(0).getString("bar") should be(s"bar$key")
    }
  }

  it should " be able to merge two C* tables together with specific types" in {
    val leftRDD = sc.cassandraTable[(Int, String)](ks, "leftjoin")
    val rightRDD = sc.cassandraTable[(Int, String)](ks, "rightjoin")
    val merged = new CassandraMergeJoinRDD(sc, leftRDD, rightRDD)
    val results = merged.collect()
    results should have size (100)
    for (i <- 0 until 100) {
      val key = results(i)._1(0)._1
      val left = results(i)._1
      val right = results(i)._2
      left(0)._2 should be(s"foo$key")
      right(0)._2 should be(s"bar$key")
    }
  }

  it should " be able to merge two C* tables with composite keys" in {
    val leftRDD = sc.cassandraTable(ks, "complexleftjoin")
    val rightRDD = sc.cassandraTable(ks, "complexrightjoin")
    val leftResult = leftRDD.collect()
    val rightResult = rightRDD.collect()
    val merged = new CassandraMergeJoinRDD(sc, leftRDD, rightRDD)
    val results = merged.collect()
    results should have size (100)
    for (i <- 0 until 100) {
      val key1 = results(i)._1(0).getInt("key1")
      val key2 = results(i)._1(0).getInt("key2")
      results(i)._1 should have length 3
      results(i)._1.map(_.getString("c")) should contain allOf("1", "2", "3")
      if (key2 % 2 == 0) {
        results(i)._2 should have length 3
        results(i)._2.map(_.getString("c")) should contain allOf("1", "2", "3")
      }
      else results(i)._2 should be('empty)
    }
  }

  "CassandraCoGroupedRDD" should "be able to merge two C* tables together that share a partition key" in {
    val leftRDD = sc.cassandraTable(ks, "leftjoin")
    val rightRDD = sc.cassandraTable(ks, "rightjoin")
    val merged = new CassandraCoGroupedRDD(sc, Seq(leftRDD, rightRDD))
    val results: Array[Seq[Seq[CassandraRow]]] = merged.collect()
    results should have size (100)
    for (i <- 0 until 100) {
      val key = results(i)(0)(0).getInt("key")
      val left = results(i)(0)
      val right = results(i)(1)
      left(0).getString("foo") should be(s"foo$key")
      right(0).getString("bar") should be(s"bar$key")
    }
  }

  it should "be able to merge two C* tables together with multiple spark partitions" in {
    implicit val readConf = ReadConf(splitCount = Some(10))
    val leftRDD = sc.cassandraTable(ks, "leftjoin")
    val rightRDD = sc.cassandraTable(ks, "rightjoin")
    val merged = new CassandraCoGroupedRDD(sc, Seq(leftRDD, rightRDD))
    val results = merged.collect()
    results should have size (100)
    for (i <- 0 until 100) {
      val key = results(i)(0)(0).getInt("key")
      val left = results(i)(0)
      val right = results(i)(1)
      left(0).getString("foo") should be(s"foo$key")
      right(0).getString("bar") should be(s"bar$key")
    }
  }

  it should " be able to merge two C* tables together with specific types" in {
    val leftRDD = sc.cassandraTable[(Int, String)](ks, "leftjoin")
    val rightRDD = sc.cassandraTable[(Int, String)](ks, "rightjoin")
    val merged = new CassandraCoGroupedRDD(sc, Seq(leftRDD, rightRDD))
    val results = merged.collect()
    results should have size (100)
    for (i <- 0 until 100) {
      val key = results(i)(0)(0)._1
      val left = results(i)(0)
      val right = results(i)(1)
      left(0)._2 should be(s"foo$key")
      right(0)._2 should be(s"bar$key")
    }
  }

  it should " be able to merge two C* tables with composite keys" in {
    val leftRDD = sc.cassandraTable(ks, "complexleftjoin")
    val rightRDD = sc.cassandraTable(ks, "complexrightjoin")
    val leftResult = leftRDD.collect()
    val rightResult = rightRDD.collect()
    val merged = new CassandraCoGroupedRDD(sc, Seq(leftRDD, rightRDD))
    val results: Array[Seq[Seq[CassandraRow]]] = merged.collect()
    results should have size (100)
    for (i <- 0 until 100) {
      val key1 = results(i)(0)(0).getInt("key1")
      val key2 = results(i)(0)(0).getInt("key2")
      results(i)(0) should have length 3
      results(i)(0).map(_.getString("c")) should contain allOf("1", "2", "3")
      if (key2 % 2 == 0) {
        results(i)(1) should have length 3
        results(i)(1).map(_.getString("c")) should contain allOf("1", "2", "3")
      }
      else results(i)(1) should be('empty)
    }
  }

  it should " be able to merge three C* tables together with specific types" in {
    val leftRDD = sc.cassandraTable[(Int, String)](ks, "leftjoin")
    val rightRDD = sc.cassandraTable[(Int, String)](ks, "rightjoin")
    val thirdRDD = sc.cassandraTable[(Int, String)](ks, "thirdjoin")
    val merged = new CassandraCoGroupedRDD(sc, Seq(leftRDD, rightRDD, thirdRDD))
    val results = merged.collect()
    results should have size (100)
    for (i <- 0 until 100) {
      val key = results(i)(0)(0)._1
      val left = results(i)(0)
      val right = results(i)(1)
      val third = results(i)(2)
      left(0)._2 should be(s"foo$key")
      right(0)._2 should be(s"bar$key")
      third(0)._2 should be(s"third$key")
    }
  }

  it should " be able to merge three C* tables with composite keys" in {
    val leftRDD = sc.cassandraTable(ks, "complexleftjoin")
    val rightRDD = sc.cassandraTable(ks, "complexrightjoin")
    val thirdRDD = sc.cassandraTable(ks, "complexthirdjoin")
    val merged = new CassandraCoGroupedRDD(sc, Seq(leftRDD, rightRDD, thirdRDD))
    val results: Array[Seq[Seq[CassandraRow]]] = merged.collect()
    results should have size (100)
    for (i <- 0 until 100) {
      val key1 = results(i)(0)(0).getInt("key1")
      val key2 = results(i)(0)(0).getInt("key2")
      results(i)(0) should have length 3
      results(i)(0).map(_.getString("c")) should contain allOf("1", "2", "3")
      if (key2 % 2 == 0) {
        results(i)(1) should have length 3
        results(i)(1).map(_.getString("c")) should contain allOf("1", "2", "3")
        results(i)(2) should be('empty)
      } else {
        results(i)(1) should be('empty)
        results(i)(2) should have length 3
        results(i)(2).map(_.getString("c")) should contain allOf("1", "2", "3")
      }
    }
  }


  it should "not work for 0 and 1 tables" in {
    val leftRDD = sc.cassandraTable(ks, "complexleftjoin")
    new CassandraCoGroupedRDD(sc, Seq(leftRDD)).isValidMergeJoin() should be (false)
    new CassandraCoGroupedRDD(sc, Seq.empty).isValidMergeJoin() should be (false)
  }
}
