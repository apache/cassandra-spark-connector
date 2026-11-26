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

package com.datastax.spark.connector

import com.datastax.spark.connector.ccm.CcmBridge
import com.datastax.spark.connector.cluster.DefaultCluster

import scala.jdk.CollectionConverters._
import scala.concurrent.Future
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.japi.CassandraJavaUtil
import com.datastax.spark.connector.japi.CassandraJavaUtil._

class CassandraJavaUtilSpec extends SparkCassandraITFlatSpecBase with DefaultCluster {

  override lazy val conn = CassandraConnector(defaultConf)

  override def beforeClass {
    conn.withSessionDo { session =>
      createKeyspace(session)
      awaitAll(
        Future {
          session.execute(s"CREATE TABLE $ks.test_table (key INT, value TEXT, PRIMARY KEY (key))")
        },

        Future {
          session.execute(s"CREATE TABLE $ks.test_table_2 (key INT, value TEXT, sub_class_field TEXT, PRIMARY KEY (key))")
        },

        Future {
          session.execute(
            s"""
               |CREATE TABLE $ks.test_table_3 (
               | c1 INT PRIMARY KEY,
               | c2 TEXT,
               | c3 INT,
               | c4 TEXT,
               | c5 INT,
               | c6 TEXT,
               | c7 INT,
               | c8 TEXT,
               | c9 INT,
               | c10 TEXT,
               | c11 INT,
               | c12 TEXT,
               | c13 INT,
               | c14 TEXT,
               | c15 INT,
               | c16 TEXT,
               | c17 INT,
               | c18 TEXT,
               | c19 INT,
               | c20 TEXT,
               | c21 INT,
               | c22 TEXT
               |)
        """.stripMargin)
          session.execute(
            s"""
               |INSERT INTO $ks.test_table_3 (
               |  c1, c2, c3, c4, c5, c6, c7, c8, c9, c10,
               |  c11, c12, c13, c14, c15, c16, c17, c18, c19, c20,
               |  c21, c22
               |) VALUES (
               |  1, '2', 3, '4', 5, '6', 7, '8', 9, '10',
               |  11, '12', 13, '14', 15, '16', 17, '18', 19, '20',
               |  21, '22'
               |)
        """.stripMargin)
        },

        Future {
          session.execute(
            s"""
               |CREATE TABLE $ks.test_table_4 (
               | c1 INT PRIMARY KEY,
               | c2 TEXT,
               | c3 INT,
               | c4 TEXT,
               | c5 INT,
               | c6 TEXT,
               | c7 INT,
               | c8 TEXT,
               | c9 INT,
               | c10 TEXT,
               | c11 INT,
               | c12 TEXT,
               | c13 INT,
               | c14 TEXT,
               | c15 INT,
               | c16 TEXT,
               | c17 INT,
               | c18 TEXT,
               | c19 INT,
               | c20 TEXT,
               | c21 INT,
               | c22 TEXT
               |)
        """.stripMargin)
        })
    }
  }

  "CassandraJavaUtil" should "allow to save beans (with multiple constructors) to Cassandra" in {
    conn.withSessionDo(_.execute(s"TRUNCATE $ks.test_table"))

    val beansRdd: RDD[SampleJavaBeanWithMultipleCtors] = sc.parallelize(Seq(
      new SampleJavaBeanWithMultipleCtors(1, "one"),
      new SampleJavaBeanWithMultipleCtors(2, "two"),
      new SampleJavaBeanWithMultipleCtors(3, "three")
    ))

    CassandraJavaUtil.javaFunctions(beansRdd)
      .writerBuilder(ks, "test_table", mapToRow(classOf[SampleJavaBeanWithMultipleCtors]))
      .saveToCassandra()

    val results = conn.withSessionDo(_.execute(s"SELECT * FROM $ks.test_table"))

    val rows = results.all()
    assert(rows.size() == 3)
    assert(rows.asScala.exists(row ⇒ row.getString("value") == "one" && row.getInt("key") == 1))
    assert(rows.asScala.exists(row ⇒ row.getString("value") == "two" && row.getInt("key") == 2))
    assert(rows.asScala.exists(row ⇒ row.getString("value") == "three" && row.getInt("key") == 3))
  }


  it should "allow to save beans to Cassandra" in {
    conn.withSessionDo(_.execute(s"TRUNCATE $ks.test_table"))

    val beansRdd = sc.parallelize(Seq(
      SampleJavaBean.newInstance(1, "one"),
      SampleJavaBean.newInstance(2, "two"),
      SampleJavaBean.newInstance(3, "three")
    ))

    CassandraJavaUtil.javaFunctions(beansRdd)
      .writerBuilder(ks, "test_table", mapToRow(classOf[SampleJavaBean]))
      .saveToCassandra()

    val results = conn.withSessionDo(_.execute(s"SELECT * FROM $ks.test_table"))

    val rows = results.all()
    assert(rows.size() == 3)
    assert(rows.asScala.exists(row ⇒ row.getString("value") == "one" && row.getInt("key") == 1))
    assert(rows.asScala.exists(row ⇒ row.getString("value") == "two" && row.getInt("key") == 2))
    assert(rows.asScala.exists(row ⇒ row.getString("value") == "three" && row.getInt("key") == 3))
  }

  it should "allow to save beans with transient fields to Cassandra" in {
    conn.withSessionDo(_.execute(s"TRUNCATE $ks.test_table"))

    val beansRdd = sc.parallelize(Seq(
      SampleJavaBeanWithTransientFields.newInstance(1, "one"),
      SampleJavaBeanWithTransientFields.newInstance(2, "two"),
      SampleJavaBeanWithTransientFields.newInstance(3, "three")
    ))

    CassandraJavaUtil.javaFunctions(beansRdd)
      .writerBuilder(ks, "test_table", mapToRow(classOf[SampleJavaBeanWithTransientFields]))
      .saveToCassandra()

    val results = conn.withSessionDo(_.execute(s"SELECT * FROM $ks.test_table"))

    val rows = results.all()
    assert(rows.size() == 3)
    assert(rows.asScala.exists(row ⇒ row.getString("value") == "one" && row.getInt("key") == 1))
    assert(rows.asScala.exists(row ⇒ row.getString("value") == "two" && row.getInt("key") == 2))
    assert(rows.asScala.exists(row ⇒ row.getString("value") == "three" && row.getInt("key") == 3))
  }

  it should "allow to save beans with inherited fields to Cassandra" in {
    conn.withSessionDo(_.execute(s"TRUNCATE $ks.test_table_2"))

    val beansRdd = sc.parallelize(Seq(
      SampleJavaBeanSubClass.newInstance(1, "one", "a"),
      SampleJavaBeanSubClass.newInstance(2, "two", "b"),
      SampleJavaBeanSubClass.newInstance(3, "three", "c")
    ))

    CassandraJavaUtil.javaFunctions(beansRdd)
      .writerBuilder(ks, "test_table_2", mapToRow(classOf[SampleJavaBeanSubClass]))
      .saveToCassandra()

    val results = conn.withSessionDo(_.execute(s"SELECT * FROM $ks.test_table_2"))
    val rows = results.all()

    rows should have size 3
    rows.asScala.map(row => (row.getString("value"), row.getInt("key"), row.getString("sub_class_field"))).toSet shouldBe Set(
      ("one", 1, "a"),
      ("two", 2, "b"),
      ("three", 3, "c")
    )
  }

  it should "allow to save nested beans to Cassandra" in {
    conn.withSessionDo(_.execute(s"TRUNCATE $ks.test_table"))

    val outer = new SampleWithNestedJavaBean

    val beansRdd = sc.parallelize(Seq(
      new outer.InnerClass(1, "one"),
      new outer.InnerClass(2, "two"),
      new outer.InnerClass(3, "three")
    ))

    CassandraJavaUtil.javaFunctions(beansRdd)
      .writerBuilder(ks, "test_table", mapToRow(classOf[outer.InnerClass]))
      .saveToCassandra()

    val results = conn.withSessionDo(_.execute(s"SELECT * FROM $ks.test_table"))

    val rows = results.all()
    assert(rows.size() == 3)
    assert(rows.asScala.exists(row ⇒ row.getString("value") == "one" && row.getInt("key") == 1))
    assert(rows.asScala.exists(row ⇒ row.getString("value") == "two" && row.getInt("key") == 2))
    assert(rows.asScala.exists(row ⇒ row.getString("value") == "three" && row.getInt("key") == 3))
  }

  it should "allow to read rows as Tuple1" in {
    val tuple = CassandraJavaUtil.javaFunctions(sc)
      .cassandraTable(ks, "test_table_3", mapRowToTuple(
      classOf[Integer]
    )).select(
        "c1"
      )
      .collect().asScala.head
    tuple shouldBe Tuple1(
      1: Integer
    )
  }


  it should "allow to read rows as Tuple2" in {
    val tuple = CassandraJavaUtil.javaFunctions(sc)
      .cassandraTable(ks, "test_table_3", mapRowToTuple(
      classOf[Integer],
      classOf[String]
    )).select(
        "c1", "c2"
      )
      .collect().asScala.head
    tuple shouldBe Tuple2(
      1: Integer,
      "2"
    )
  }


  it should "allow to read rows as Tuple3" in {
    val tuple = CassandraJavaUtil.javaFunctions(sc)
      .cassandraTable(ks, "test_table_3", mapRowToTuple(
      classOf[Integer],
      classOf[String],
      classOf[Integer]
    )).select(
        "c1", "c2", "c3"
      )
      .collect().asScala.head
    tuple shouldBe Tuple3(
      1: Integer,
      "2",
      3: Integer
    )
  }


  it should "allow to read rows as Tuple4" in {
    val tuple = CassandraJavaUtil.javaFunctions(sc)
      .cassandraTable(ks, "test_table_3", mapRowToTuple(
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String]
    )).select(
        "c1", "c2", "c3", "c4"
      )
      .collect().asScala.head
    tuple shouldBe Tuple4(
      1: Integer,
      "2",
      3: Integer,
      "4"
    )
  }


  it should "allow to read rows as Tuple5" in {
    val tuple = CassandraJavaUtil.javaFunctions(sc)
      .cassandraTable(ks, "test_table_3", mapRowToTuple(
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer]
    )).select(
        "c1", "c2", "c3", "c4", "c5"
      )
      .collect().asScala.head
    tuple shouldBe Tuple5(
      1: Integer,
      "2",
      3: Integer,
      "4",
      5: Integer
    )
  }


  it should "allow to read rows as Tuple6" in {
    val tuple = CassandraJavaUtil.javaFunctions(sc)
      .cassandraTable(ks, "test_table_3", mapRowToTuple(
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String]
    )).select(
        "c1", "c2", "c3", "c4", "c5", "c6"
      )
      .collect().asScala.head
    tuple shouldBe Tuple6(
      1: Integer,
      "2",
      3: Integer,
      "4",
      5: Integer,
      "6"
    )
  }


  it should "allow to read rows as Tuple7" in {
    val tuple = CassandraJavaUtil.javaFunctions(sc)
      .cassandraTable(ks, "test_table_3", mapRowToTuple(
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer]
    )).select(
        "c1", "c2", "c3", "c4", "c5", "c6", "c7"
      )
      .collect().asScala.head
    tuple shouldBe Tuple7(
      1: Integer,
      "2",
      3: Integer,
      "4",
      5: Integer,
      "6",
      7: Integer
    )
  }


  it should "allow to read rows as Tuple8" in {
    val tuple = CassandraJavaUtil.javaFunctions(sc)
      .cassandraTable(ks, "test_table_3", mapRowToTuple(
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String]
    )).select(
        "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8"
      )
      .collect().asScala.head
    tuple shouldBe Tuple8(
      1: Integer,
      "2",
      3: Integer,
      "4",
      5: Integer,
      "6",
      7: Integer,
      "8"
    )
  }


  it should "allow to read rows as Tuple9" in {
    val tuple = CassandraJavaUtil.javaFunctions(sc)
      .cassandraTable(ks, "test_table_3", mapRowToTuple(
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer]
    )).select(
        "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9"
      )
      .collect().asScala.head
    tuple shouldBe Tuple9(
      1: Integer,
      "2",
      3: Integer,
      "4",
      5: Integer,
      "6",
      7: Integer,
      "8",
      9: Integer
    )
  }


  it should "allow to read rows as Tuple10" in {
    val tuple = CassandraJavaUtil.javaFunctions(sc)
      .cassandraTable(ks, "test_table_3", mapRowToTuple(
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String]
    )).select(
        "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10"
      )
      .collect().asScala.head
    tuple shouldBe Tuple10(
      1: Integer,
      "2",
      3: Integer,
      "4",
      5: Integer,
      "6",
      7: Integer,
      "8",
      9: Integer,
      "10"
    )
  }


  it should "allow to read rows as Tuple11" in {
    val tuple = CassandraJavaUtil.javaFunctions(sc)
      .cassandraTable(ks, "test_table_3", mapRowToTuple(
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer]
    )).select(
        "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11"
      )
      .collect().asScala.head
    tuple shouldBe Tuple11(
      1: Integer,
      "2",
      3: Integer,
      "4",
      5: Integer,
      "6",
      7: Integer,
      "8",
      9: Integer,
      "10",
      11: Integer
    )
  }


  it should "allow to read rows as Tuple12" in {
    val tuple = CassandraJavaUtil.javaFunctions(sc)
      .cassandraTable(ks, "test_table_3", mapRowToTuple(
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String]
    )).select(
        "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12"
      )
      .collect().asScala.head
    tuple shouldBe Tuple12(
      1: Integer,
      "2",
      3: Integer,
      "4",
      5: Integer,
      "6",
      7: Integer,
      "8",
      9: Integer,
      "10",
      11: Integer,
      "12"
    )
  }


  it should "allow to read rows as Tuple13" in {
    val tuple = CassandraJavaUtil.javaFunctions(sc)
      .cassandraTable(ks, "test_table_3", mapRowToTuple(
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer]
    )).select(
        "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13"
      )
      .collect().asScala.head
    tuple shouldBe Tuple13(
      1: Integer,
      "2",
      3: Integer,
      "4",
      5: Integer,
      "6",
      7: Integer,
      "8",
      9: Integer,
      "10",
      11: Integer,
      "12",
      13: Integer
    )
  }


  it should "allow to read rows as Tuple14" in {
    val tuple = CassandraJavaUtil.javaFunctions(sc)
      .cassandraTable(ks, "test_table_3", mapRowToTuple(
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String]
    )).select(
        "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13", "c14"
      )
      .collect().asScala.head
    tuple shouldBe Tuple14(
      1: Integer,
      "2",
      3: Integer,
      "4",
      5: Integer,
      "6",
      7: Integer,
      "8",
      9: Integer,
      "10",
      11: Integer,
      "12",
      13: Integer,
      "14"
    )
  }


  it should "allow to read rows as Tuple15" in {
    val tuple = CassandraJavaUtil.javaFunctions(sc)
      .cassandraTable(ks, "test_table_3", mapRowToTuple(
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer]
    )).select(
        "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13", "c14", "c15"
      )
      .collect().asScala.head
    tuple shouldBe Tuple15(
      1: Integer,
      "2",
      3: Integer,
      "4",
      5: Integer,
      "6",
      7: Integer,
      "8",
      9: Integer,
      "10",
      11: Integer,
      "12",
      13: Integer,
      "14",
      15: Integer
    )
  }


  it should "allow to read rows as Tuple16" in {
    val tuple = CassandraJavaUtil.javaFunctions(sc)
      .cassandraTable(ks, "test_table_3", mapRowToTuple(
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String]
    )).select(
        "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13", "c14", "c15", "c16"
      )
      .collect().asScala.head
    tuple shouldBe Tuple16(
      1: Integer,
      "2",
      3: Integer,
      "4",
      5: Integer,
      "6",
      7: Integer,
      "8",
      9: Integer,
      "10",
      11: Integer,
      "12",
      13: Integer,
      "14",
      15: Integer,
      "16"
    )
  }


  it should "allow to read rows as Tuple17" in {
    val tuple = CassandraJavaUtil.javaFunctions(sc)
      .cassandraTable(ks, "test_table_3", mapRowToTuple(
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer]
    )).select(
        "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13", "c14", "c15", "c16", "c17"
      )
      .collect().asScala.head
    tuple shouldBe Tuple17(
      1: Integer,
      "2",
      3: Integer,
      "4",
      5: Integer,
      "6",
      7: Integer,
      "8",
      9: Integer,
      "10",
      11: Integer,
      "12",
      13: Integer,
      "14",
      15: Integer,
      "16",
      17: Integer
    )
  }


  it should "allow to read rows as Tuple18" in {
    val tuple = CassandraJavaUtil.javaFunctions(sc)
      .cassandraTable(ks, "test_table_3", mapRowToTuple(
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String]
    )).select(
        "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13", "c14", "c15", "c16", "c17", "c18"
      )
      .collect().asScala.head
    tuple shouldBe Tuple18(
      1: Integer,
      "2",
      3: Integer,
      "4",
      5: Integer,
      "6",
      7: Integer,
      "8",
      9: Integer,
      "10",
      11: Integer,
      "12",
      13: Integer,
      "14",
      15: Integer,
      "16",
      17: Integer,
      "18"
    )
  }


  it should "allow to read rows as Tuple19" in {
    val tuple = CassandraJavaUtil.javaFunctions(sc)
      .cassandraTable(ks, "test_table_3", mapRowToTuple(
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer]
    )).select(
        "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13", "c14", "c15", "c16", "c17", "c18", "c19"
      )
      .collect().asScala.head
    tuple shouldBe Tuple19(
      1: Integer,
      "2",
      3: Integer,
      "4",
      5: Integer,
      "6",
      7: Integer,
      "8",
      9: Integer,
      "10",
      11: Integer,
      "12",
      13: Integer,
      "14",
      15: Integer,
      "16",
      17: Integer,
      "18",
      19: Integer
    )
  }


  it should "allow to read rows as Tuple20" in {
    val tuple = CassandraJavaUtil.javaFunctions(sc)
      .cassandraTable(ks, "test_table_3", mapRowToTuple(
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String]
    )).select(
        "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13", "c14", "c15", "c16", "c17", "c18", "c19", "c20"
      )
      .collect().asScala.head
    tuple shouldBe Tuple20(
      1: Integer,
      "2",
      3: Integer,
      "4",
      5: Integer,
      "6",
      7: Integer,
      "8",
      9: Integer,
      "10",
      11: Integer,
      "12",
      13: Integer,
      "14",
      15: Integer,
      "16",
      17: Integer,
      "18",
      19: Integer,
      "20"
    )
  }


  it should "allow to read rows as Tuple21" in {
    val tuple = CassandraJavaUtil.javaFunctions(sc)
      .cassandraTable(ks, "test_table_3", mapRowToTuple(
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer]
    )).select(
        "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13", "c14", "c15", "c16", "c17", "c18", "c19", "c20", "c21"
      )
      .collect().asScala.head
    tuple shouldBe Tuple21(
      1: Integer,
      "2",
      3: Integer,
      "4",
      5: Integer,
      "6",
      7: Integer,
      "8",
      9: Integer,
      "10",
      11: Integer,
      "12",
      13: Integer,
      "14",
      15: Integer,
      "16",
      17: Integer,
      "18",
      19: Integer,
      "20",
      21: Integer
    )
  }


  it should "allow to read rows as Tuple22" in {
    val tuple = CassandraJavaUtil.javaFunctions(sc)
      .cassandraTable(ks, "test_table_3", mapRowToTuple(
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String]
    )).select(
        "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13", "c14", "c15", "c16", "c17", "c18", "c19", "c20", "c21", "c22"
      )
      .collect().asScala.head
    tuple shouldBe Tuple22(
      1: Integer,
      "2",
      3: Integer,
      "4",
      5: Integer,
      "6",
      7: Integer,
      "8",
      9: Integer,
      "10",
      11: Integer,
      "12",
      13: Integer,
      "14",
      15: Integer,
      "16",
      17: Integer,
      "18",
      19: Integer,
      "20",
      21: Integer,
      "22"
    )
  }


  it should "allow to write Tuple1 to Cassandra" in {
    conn.withSessionDo(_.execute(s"TRUNCATE $ks.test_table_4"))
    val tuple = Tuple1(
      1: Integer
    )
    CassandraJavaUtil.javaFunctions(sc.makeRDD(Seq(tuple)))
      .writerBuilder(ks, "test_table_4", mapTupleToRow(
      classOf[Integer]
    )).withColumnSelector(someColumns(
      "c1"
    )).saveToCassandra()

    val row = conn.withSessionDo(_.execute(s"SELECT * FROM $ks.test_table_4").one())
    row.getInt("c1") shouldBe (1: Integer)
  }



  it should "allow to write Tuple2 to Cassandra" in {
    conn.withSessionDo(_.execute(s"TRUNCATE $ks.test_table_4"))
    val tuple = Tuple2(
      1: Integer,
      "2"
    )
    CassandraJavaUtil.javaFunctions(sc.makeRDD(Seq(tuple)))
      .writerBuilder(ks, "test_table_4", mapTupleToRow(
      classOf[Integer],
      classOf[String]
    )).withColumnSelector(someColumns(
      "c1", "c2"
    )).saveToCassandra()

    val row = conn.withSessionDo(_.execute(s"SELECT * FROM $ks.test_table_4").one())
    row.getInt("c1") shouldBe (1: Integer)
    row.getString("c2") shouldBe "2"
  }



  it should "allow to write Tuple3 to Cassandra" in {
    conn.withSessionDo(_.execute(s"TRUNCATE $ks.test_table_4"))
    val tuple = Tuple3(
      1: Integer,
      "2",
      3: Integer
    )
    CassandraJavaUtil.javaFunctions(sc.makeRDD(Seq(tuple)))
      .writerBuilder(ks, "test_table_4", mapTupleToRow(
      classOf[Integer],
      classOf[String],
      classOf[Integer]
    )).withColumnSelector(someColumns(
      "c1", "c2", "c3"
    )).saveToCassandra()

    val row = conn.withSessionDo(_.execute(s"SELECT * FROM $ks.test_table_4").one())
    row.getInt("c1") shouldBe (1: Integer)
    row.getString("c2") shouldBe "2"
    row.getInt("c3") shouldBe (3: Integer)
  }



  it should "allow to write Tuple4 to Cassandra" in {
    conn.withSessionDo(_.execute(s"TRUNCATE $ks.test_table_4"))
    val tuple = Tuple4(
      1: Integer,
      "2",
      3: Integer,
      "4"
    )
    CassandraJavaUtil.javaFunctions(sc.makeRDD(Seq(tuple)))
      .writerBuilder(ks, "test_table_4", mapTupleToRow(
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String]
    )).withColumnSelector(someColumns(
      "c1", "c2", "c3", "c4"
    )).saveToCassandra()

    val row = conn.withSessionDo(_.execute(s"SELECT * FROM $ks.test_table_4").one())
    row.getInt("c1") shouldBe (1: Integer)
    row.getString("c2") shouldBe "2"
    row.getInt("c3") shouldBe (3: Integer)
    row.getString("c4") shouldBe "4"
  }



  it should "allow to write Tuple5 to Cassandra" in {
    conn.withSessionDo(_.execute(s"TRUNCATE $ks.test_table_4"))
    val tuple = Tuple5(
      1: Integer,
      "2",
      3: Integer,
      "4",
      5: Integer
    )
    CassandraJavaUtil.javaFunctions(sc.makeRDD(Seq(tuple)))
      .writerBuilder(ks, "test_table_4", mapTupleToRow(
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer]
    )).withColumnSelector(someColumns(
      "c1", "c2", "c3", "c4", "c5"
    )).saveToCassandra()

    val row = conn.withSessionDo(_.execute(s"SELECT * FROM $ks.test_table_4").one())
    row.getInt("c1") shouldBe (1: Integer)
    row.getString("c2") shouldBe "2"
    row.getInt("c3") shouldBe (3: Integer)
    row.getString("c4") shouldBe "4"
    row.getInt("c5") shouldBe (5: Integer)
  }



  it should "allow to write Tuple6 to Cassandra" in {
    conn.withSessionDo(_.execute(s"TRUNCATE $ks.test_table_4"))
    val tuple = Tuple6(
      1: Integer,
      "2",
      3: Integer,
      "4",
      5: Integer,
      "6"
    )
    CassandraJavaUtil.javaFunctions(sc.makeRDD(Seq(tuple)))
      .writerBuilder(ks, "test_table_4", mapTupleToRow(
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String]
    )).withColumnSelector(someColumns(
      "c1", "c2", "c3", "c4", "c5", "c6"
    )).saveToCassandra()

    val row = conn.withSessionDo(_.execute(s"SELECT * FROM $ks.test_table_4").one())
    row.getInt("c1") shouldBe (1: Integer)
    row.getString("c2") shouldBe "2"
    row.getInt("c3") shouldBe (3: Integer)
    row.getString("c4") shouldBe "4"
    row.getInt("c5") shouldBe (5: Integer)
    row.getString("c6") shouldBe "6"
  }



  it should "allow to write Tuple7 to Cassandra" in {
    conn.withSessionDo(_.execute(s"TRUNCATE $ks.test_table_4"))
    val tuple = Tuple7(
      1: Integer,
      "2",
      3: Integer,
      "4",
      5: Integer,
      "6",
      7: Integer
    )
    CassandraJavaUtil.javaFunctions(sc.makeRDD(Seq(tuple)))
      .writerBuilder(ks, "test_table_4", mapTupleToRow(
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer]
    )).withColumnSelector(someColumns(
      "c1", "c2", "c3", "c4", "c5", "c6", "c7"
    )).saveToCassandra()

    val row = conn.withSessionDo(_.execute(s"SELECT * FROM $ks.test_table_4").one())
    row.getInt("c1") shouldBe (1: Integer)
    row.getString("c2") shouldBe "2"
    row.getInt("c3") shouldBe (3: Integer)
    row.getString("c4") shouldBe "4"
    row.getInt("c5") shouldBe (5: Integer)
    row.getString("c6") shouldBe "6"
    row.getInt("c7") shouldBe (7: Integer)
  }



  it should "allow to write Tuple8 to Cassandra" in {
    conn.withSessionDo(_.execute(s"TRUNCATE $ks.test_table_4"))
    val tuple = Tuple8(
      1: Integer,
      "2",
      3: Integer,
      "4",
      5: Integer,
      "6",
      7: Integer,
      "8"
    )
    CassandraJavaUtil.javaFunctions(sc.makeRDD(Seq(tuple)))
      .writerBuilder(ks, "test_table_4", mapTupleToRow(
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String]
    )).withColumnSelector(someColumns(
      "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8"
    )).saveToCassandra()

    val row = conn.withSessionDo(_.execute(s"SELECT * FROM $ks.test_table_4").one())
    row.getInt("c1") shouldBe (1: Integer)
    row.getString("c2") shouldBe "2"
    row.getInt("c3") shouldBe (3: Integer)
    row.getString("c4") shouldBe "4"
    row.getInt("c5") shouldBe (5: Integer)
    row.getString("c6") shouldBe "6"
    row.getInt("c7") shouldBe (7: Integer)
    row.getString("c8") shouldBe "8"
  }



  it should "allow to write Tuple9 to Cassandra" in {
    conn.withSessionDo(_.execute(s"TRUNCATE $ks.test_table_4"))
    val tuple = Tuple9(
      1: Integer,
      "2",
      3: Integer,
      "4",
      5: Integer,
      "6",
      7: Integer,
      "8",
      9: Integer
    )
    CassandraJavaUtil.javaFunctions(sc.makeRDD(Seq(tuple)))
      .writerBuilder(ks, "test_table_4", mapTupleToRow(
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer]
    )).withColumnSelector(someColumns(
      "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9"
    )).saveToCassandra()

    val row = conn.withSessionDo(_.execute(s"SELECT * FROM $ks.test_table_4").one())
    row.getInt("c1") shouldBe (1: Integer)
    row.getString("c2") shouldBe "2"
    row.getInt("c3") shouldBe (3: Integer)
    row.getString("c4") shouldBe "4"
    row.getInt("c5") shouldBe (5: Integer)
    row.getString("c6") shouldBe "6"
    row.getInt("c7") shouldBe (7: Integer)
    row.getString("c8") shouldBe "8"
    row.getInt("c9") shouldBe (9: Integer)
  }



  it should "allow to write Tuple10 to Cassandra" in {
    conn.withSessionDo(_.execute(s"TRUNCATE $ks.test_table_4"))
    val tuple = Tuple10(
      1: Integer,
      "2",
      3: Integer,
      "4",
      5: Integer,
      "6",
      7: Integer,
      "8",
      9: Integer,
      "10"
    )
    CassandraJavaUtil.javaFunctions(sc.makeRDD(Seq(tuple)))
      .writerBuilder(ks, "test_table_4", mapTupleToRow(
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String]
    )).withColumnSelector(someColumns(
      "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10"
    )).saveToCassandra()

    val row = conn.withSessionDo(_.execute(s"SELECT * FROM $ks.test_table_4").one())
    row.getInt("c1") shouldBe (1: Integer)
    row.getString("c2") shouldBe "2"
    row.getInt("c3") shouldBe (3: Integer)
    row.getString("c4") shouldBe "4"
    row.getInt("c5") shouldBe (5: Integer)
    row.getString("c6") shouldBe "6"
    row.getInt("c7") shouldBe (7: Integer)
    row.getString("c8") shouldBe "8"
    row.getInt("c9") shouldBe (9: Integer)
    row.getString("c10") shouldBe "10"
  }



  it should "allow to write Tuple11 to Cassandra" in {
    conn.withSessionDo(_.execute(s"TRUNCATE $ks.test_table_4"))
    val tuple = Tuple11(
      1: Integer,
      "2",
      3: Integer,
      "4",
      5: Integer,
      "6",
      7: Integer,
      "8",
      9: Integer,
      "10",
      11: Integer
    )
    CassandraJavaUtil.javaFunctions(sc.makeRDD(Seq(tuple)))
      .writerBuilder(ks, "test_table_4", mapTupleToRow(
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer]
    )).withColumnSelector(someColumns(
      "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11"
    )).saveToCassandra()

    val row = conn.withSessionDo(_.execute(s"SELECT * FROM $ks.test_table_4").one())
    row.getInt("c1") shouldBe (1: Integer)
    row.getString("c2") shouldBe "2"
    row.getInt("c3") shouldBe (3: Integer)
    row.getString("c4") shouldBe "4"
    row.getInt("c5") shouldBe (5: Integer)
    row.getString("c6") shouldBe "6"
    row.getInt("c7") shouldBe (7: Integer)
    row.getString("c8") shouldBe "8"
    row.getInt("c9") shouldBe (9: Integer)
    row.getString("c10") shouldBe "10"
    row.getInt("c11") shouldBe (11: Integer)
  }



  it should "allow to write Tuple12 to Cassandra" in {
    conn.withSessionDo(_.execute(s"TRUNCATE $ks.test_table_4"))
    val tuple = Tuple12(
      1: Integer,
      "2",
      3: Integer,
      "4",
      5: Integer,
      "6",
      7: Integer,
      "8",
      9: Integer,
      "10",
      11: Integer,
      "12"
    )
    CassandraJavaUtil.javaFunctions(sc.makeRDD(Seq(tuple)))
      .writerBuilder(ks, "test_table_4", mapTupleToRow(
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String]
    )).withColumnSelector(someColumns(
      "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12"
    )).saveToCassandra()

    val row = conn.withSessionDo(_.execute(s"SELECT * FROM $ks.test_table_4").one())
    row.getInt("c1") shouldBe (1: Integer)
    row.getString("c2") shouldBe "2"
    row.getInt("c3") shouldBe (3: Integer)
    row.getString("c4") shouldBe "4"
    row.getInt("c5") shouldBe (5: Integer)
    row.getString("c6") shouldBe "6"
    row.getInt("c7") shouldBe (7: Integer)
    row.getString("c8") shouldBe "8"
    row.getInt("c9") shouldBe (9: Integer)
    row.getString("c10") shouldBe "10"
    row.getInt("c11") shouldBe (11: Integer)
    row.getString("c12") shouldBe "12"
  }



  it should "allow to write Tuple13 to Cassandra" in {
    conn.withSessionDo(_.execute(s"TRUNCATE $ks.test_table_4"))
    val tuple = Tuple13(
      1: Integer,
      "2",
      3: Integer,
      "4",
      5: Integer,
      "6",
      7: Integer,
      "8",
      9: Integer,
      "10",
      11: Integer,
      "12",
      13: Integer
    )
    CassandraJavaUtil.javaFunctions(sc.makeRDD(Seq(tuple)))
      .writerBuilder(ks, "test_table_4", mapTupleToRow(
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer]
    )).withColumnSelector(someColumns(
      "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13"
    )).saveToCassandra()

    val row = conn.withSessionDo(_.execute(s"SELECT * FROM $ks.test_table_4").one())
    row.getInt("c1") shouldBe (1: Integer)
    row.getString("c2") shouldBe "2"
    row.getInt("c3") shouldBe (3: Integer)
    row.getString("c4") shouldBe "4"
    row.getInt("c5") shouldBe (5: Integer)
    row.getString("c6") shouldBe "6"
    row.getInt("c7") shouldBe (7: Integer)
    row.getString("c8") shouldBe "8"
    row.getInt("c9") shouldBe (9: Integer)
    row.getString("c10") shouldBe "10"
    row.getInt("c11") shouldBe (11: Integer)
    row.getString("c12") shouldBe "12"
    row.getInt("c13") shouldBe (13: Integer)
  }



  it should "allow to write Tuple14 to Cassandra" in {
    conn.withSessionDo(_.execute(s"TRUNCATE $ks.test_table_4"))
    val tuple = Tuple14(
      1: Integer,
      "2",
      3: Integer,
      "4",
      5: Integer,
      "6",
      7: Integer,
      "8",
      9: Integer,
      "10",
      11: Integer,
      "12",
      13: Integer,
      "14"
    )
    CassandraJavaUtil.javaFunctions(sc.makeRDD(Seq(tuple)))
      .writerBuilder(ks, "test_table_4", mapTupleToRow(
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String]
    )).withColumnSelector(someColumns(
      "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13", "c14"
    )).saveToCassandra()

    val row = conn.withSessionDo(_.execute(s"SELECT * FROM $ks.test_table_4").one())
    row.getInt("c1") shouldBe (1: Integer)
    row.getString("c2") shouldBe "2"
    row.getInt("c3") shouldBe (3: Integer)
    row.getString("c4") shouldBe "4"
    row.getInt("c5") shouldBe (5: Integer)
    row.getString("c6") shouldBe "6"
    row.getInt("c7") shouldBe (7: Integer)
    row.getString("c8") shouldBe "8"
    row.getInt("c9") shouldBe (9: Integer)
    row.getString("c10") shouldBe "10"
    row.getInt("c11") shouldBe (11: Integer)
    row.getString("c12") shouldBe "12"
    row.getInt("c13") shouldBe (13: Integer)
    row.getString("c14") shouldBe "14"
  }



  it should "allow to write Tuple15 to Cassandra" in {
    conn.withSessionDo(_.execute(s"TRUNCATE $ks.test_table_4"))
    val tuple = Tuple15(
      1: Integer,
      "2",
      3: Integer,
      "4",
      5: Integer,
      "6",
      7: Integer,
      "8",
      9: Integer,
      "10",
      11: Integer,
      "12",
      13: Integer,
      "14",
      15: Integer
    )
    CassandraJavaUtil.javaFunctions(sc.makeRDD(Seq(tuple)))
      .writerBuilder(ks, "test_table_4", mapTupleToRow(
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer]
    )).withColumnSelector(someColumns(
      "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13", "c14", "c15"
    )).saveToCassandra()

    val row = conn.withSessionDo(_.execute(s"SELECT * FROM $ks.test_table_4").one())
    row.getInt("c1") shouldBe (1: Integer)
    row.getString("c2") shouldBe "2"
    row.getInt("c3") shouldBe (3: Integer)
    row.getString("c4") shouldBe "4"
    row.getInt("c5") shouldBe (5: Integer)
    row.getString("c6") shouldBe "6"
    row.getInt("c7") shouldBe (7: Integer)
    row.getString("c8") shouldBe "8"
    row.getInt("c9") shouldBe (9: Integer)
    row.getString("c10") shouldBe "10"
    row.getInt("c11") shouldBe (11: Integer)
    row.getString("c12") shouldBe "12"
    row.getInt("c13") shouldBe (13: Integer)
    row.getString("c14") shouldBe "14"
    row.getInt("c15") shouldBe (15: Integer)
  }



  it should "allow to write Tuple16 to Cassandra" in {
    conn.withSessionDo(_.execute(s"TRUNCATE $ks.test_table_4"))
    val tuple = Tuple16(
      1: Integer,
      "2",
      3: Integer,
      "4",
      5: Integer,
      "6",
      7: Integer,
      "8",
      9: Integer,
      "10",
      11: Integer,
      "12",
      13: Integer,
      "14",
      15: Integer,
      "16"
    )
    CassandraJavaUtil.javaFunctions(sc.makeRDD(Seq(tuple)))
      .writerBuilder(ks, "test_table_4", mapTupleToRow(
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String]
    )).withColumnSelector(someColumns(
      "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13", "c14", "c15", "c16"
    )).saveToCassandra()

    val row = conn.withSessionDo(_.execute(s"SELECT * FROM $ks.test_table_4").one())
    row.getInt("c1") shouldBe (1: Integer)
    row.getString("c2") shouldBe "2"
    row.getInt("c3") shouldBe (3: Integer)
    row.getString("c4") shouldBe "4"
    row.getInt("c5") shouldBe (5: Integer)
    row.getString("c6") shouldBe "6"
    row.getInt("c7") shouldBe (7: Integer)
    row.getString("c8") shouldBe "8"
    row.getInt("c9") shouldBe (9: Integer)
    row.getString("c10") shouldBe "10"
    row.getInt("c11") shouldBe (11: Integer)
    row.getString("c12") shouldBe "12"
    row.getInt("c13") shouldBe (13: Integer)
    row.getString("c14") shouldBe "14"
    row.getInt("c15") shouldBe (15: Integer)
    row.getString("c16") shouldBe "16"
  }



  it should "allow to write Tuple17 to Cassandra" in {
    conn.withSessionDo(_.execute(s"TRUNCATE $ks.test_table_4"))
    val tuple = Tuple17(
      1: Integer,
      "2",
      3: Integer,
      "4",
      5: Integer,
      "6",
      7: Integer,
      "8",
      9: Integer,
      "10",
      11: Integer,
      "12",
      13: Integer,
      "14",
      15: Integer,
      "16",
      17: Integer
    )
    CassandraJavaUtil.javaFunctions(sc.makeRDD(Seq(tuple)))
      .writerBuilder(ks, "test_table_4", mapTupleToRow(
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer]
    )).withColumnSelector(someColumns(
      "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13", "c14", "c15", "c16", "c17"
    )).saveToCassandra()

    val row = conn.withSessionDo(_.execute(s"SELECT * FROM $ks.test_table_4").one())
    row.getInt("c1") shouldBe (1: Integer)
    row.getString("c2") shouldBe "2"
    row.getInt("c3") shouldBe (3: Integer)
    row.getString("c4") shouldBe "4"
    row.getInt("c5") shouldBe (5: Integer)
    row.getString("c6") shouldBe "6"
    row.getInt("c7") shouldBe (7: Integer)
    row.getString("c8") shouldBe "8"
    row.getInt("c9") shouldBe (9: Integer)
    row.getString("c10") shouldBe "10"
    row.getInt("c11") shouldBe (11: Integer)
    row.getString("c12") shouldBe "12"
    row.getInt("c13") shouldBe (13: Integer)
    row.getString("c14") shouldBe "14"
    row.getInt("c15") shouldBe (15: Integer)
    row.getString("c16") shouldBe "16"
    row.getInt("c17") shouldBe (17: Integer)
  }



  it should "allow to write Tuple18 to Cassandra" in {
    conn.withSessionDo(_.execute(s"TRUNCATE $ks.test_table_4"))
    val tuple = Tuple18(
      1: Integer,
      "2",
      3: Integer,
      "4",
      5: Integer,
      "6",
      7: Integer,
      "8",
      9: Integer,
      "10",
      11: Integer,
      "12",
      13: Integer,
      "14",
      15: Integer,
      "16",
      17: Integer,
      "18"
    )
    CassandraJavaUtil.javaFunctions(sc.makeRDD(Seq(tuple)))
      .writerBuilder(ks, "test_table_4", mapTupleToRow(
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String]
    )).withColumnSelector(someColumns(
      "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13", "c14", "c15", "c16", "c17", "c18"
    )).saveToCassandra()

    val row = conn.withSessionDo(_.execute(s"SELECT * FROM $ks.test_table_4").one())
    row.getInt("c1") shouldBe (1: Integer)
    row.getString("c2") shouldBe "2"
    row.getInt("c3") shouldBe (3: Integer)
    row.getString("c4") shouldBe "4"
    row.getInt("c5") shouldBe (5: Integer)
    row.getString("c6") shouldBe "6"
    row.getInt("c7") shouldBe (7: Integer)
    row.getString("c8") shouldBe "8"
    row.getInt("c9") shouldBe (9: Integer)
    row.getString("c10") shouldBe "10"
    row.getInt("c11") shouldBe (11: Integer)
    row.getString("c12") shouldBe "12"
    row.getInt("c13") shouldBe (13: Integer)
    row.getString("c14") shouldBe "14"
    row.getInt("c15") shouldBe (15: Integer)
    row.getString("c16") shouldBe "16"
    row.getInt("c17") shouldBe (17: Integer)
    row.getString("c18") shouldBe "18"
  }



  it should "allow to write Tuple19 to Cassandra" in {
    conn.withSessionDo(_.execute(s"TRUNCATE $ks.test_table_4"))
    val tuple = Tuple19(
      1: Integer,
      "2",
      3: Integer,
      "4",
      5: Integer,
      "6",
      7: Integer,
      "8",
      9: Integer,
      "10",
      11: Integer,
      "12",
      13: Integer,
      "14",
      15: Integer,
      "16",
      17: Integer,
      "18",
      19: Integer
    )
    CassandraJavaUtil.javaFunctions(sc.makeRDD(Seq(tuple)))
      .writerBuilder(ks, "test_table_4", mapTupleToRow(
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer]
    )).withColumnSelector(someColumns(
      "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13", "c14", "c15", "c16", "c17", "c18", "c19"
    )).saveToCassandra()

    val row = conn.withSessionDo(_.execute(s"SELECT * FROM $ks.test_table_4").one())
    row.getInt("c1") shouldBe (1: Integer)
    row.getString("c2") shouldBe "2"
    row.getInt("c3") shouldBe (3: Integer)
    row.getString("c4") shouldBe "4"
    row.getInt("c5") shouldBe (5: Integer)
    row.getString("c6") shouldBe "6"
    row.getInt("c7") shouldBe (7: Integer)
    row.getString("c8") shouldBe "8"
    row.getInt("c9") shouldBe (9: Integer)
    row.getString("c10") shouldBe "10"
    row.getInt("c11") shouldBe (11: Integer)
    row.getString("c12") shouldBe "12"
    row.getInt("c13") shouldBe (13: Integer)
    row.getString("c14") shouldBe "14"
    row.getInt("c15") shouldBe (15: Integer)
    row.getString("c16") shouldBe "16"
    row.getInt("c17") shouldBe (17: Integer)
    row.getString("c18") shouldBe "18"
    row.getInt("c19") shouldBe (19: Integer)
  }



  it should "allow to write Tuple20 to Cassandra" in {
    conn.withSessionDo(_.execute(s"TRUNCATE $ks.test_table_4"))
    val tuple = Tuple20(
      1: Integer,
      "2",
      3: Integer,
      "4",
      5: Integer,
      "6",
      7: Integer,
      "8",
      9: Integer,
      "10",
      11: Integer,
      "12",
      13: Integer,
      "14",
      15: Integer,
      "16",
      17: Integer,
      "18",
      19: Integer,
      "20"
    )
    CassandraJavaUtil.javaFunctions(sc.makeRDD(Seq(tuple)))
      .writerBuilder(ks, "test_table_4", mapTupleToRow(
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String]
    )).withColumnSelector(someColumns(
      "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13", "c14", "c15", "c16", "c17", "c18", "c19", "c20"
    )).saveToCassandra()

    val row = conn.withSessionDo(_.execute(s"SELECT * FROM $ks.test_table_4").one())
    row.getInt("c1") shouldBe (1: Integer)
    row.getString("c2") shouldBe "2"
    row.getInt("c3") shouldBe (3: Integer)
    row.getString("c4") shouldBe "4"
    row.getInt("c5") shouldBe (5: Integer)
    row.getString("c6") shouldBe "6"
    row.getInt("c7") shouldBe (7: Integer)
    row.getString("c8") shouldBe "8"
    row.getInt("c9") shouldBe (9: Integer)
    row.getString("c10") shouldBe "10"
    row.getInt("c11") shouldBe (11: Integer)
    row.getString("c12") shouldBe "12"
    row.getInt("c13") shouldBe (13: Integer)
    row.getString("c14") shouldBe "14"
    row.getInt("c15") shouldBe (15: Integer)
    row.getString("c16") shouldBe "16"
    row.getInt("c17") shouldBe (17: Integer)
    row.getString("c18") shouldBe "18"
    row.getInt("c19") shouldBe (19: Integer)
    row.getString("c20") shouldBe "20"
  }



  it should "allow to write Tuple21 to Cassandra" in {
    conn.withSessionDo(_.execute(s"TRUNCATE $ks.test_table_4"))
    val tuple = Tuple21(
      1: Integer,
      "2",
      3: Integer,
      "4",
      5: Integer,
      "6",
      7: Integer,
      "8",
      9: Integer,
      "10",
      11: Integer,
      "12",
      13: Integer,
      "14",
      15: Integer,
      "16",
      17: Integer,
      "18",
      19: Integer,
      "20",
      21: Integer
    )
    CassandraJavaUtil.javaFunctions(sc.makeRDD(Seq(tuple)))
      .writerBuilder(ks, "test_table_4", mapTupleToRow(
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer]
    )).withColumnSelector(someColumns(
      "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13", "c14", "c15", "c16", "c17", "c18", "c19", "c20", "c21"
    )).saveToCassandra()

    val row = conn.withSessionDo(_.execute(s"SELECT * FROM $ks.test_table_4").one())
    row.getInt("c1") shouldBe (1: Integer)
    row.getString("c2") shouldBe "2"
    row.getInt("c3") shouldBe (3: Integer)
    row.getString("c4") shouldBe "4"
    row.getInt("c5") shouldBe (5: Integer)
    row.getString("c6") shouldBe "6"
    row.getInt("c7") shouldBe (7: Integer)
    row.getString("c8") shouldBe "8"
    row.getInt("c9") shouldBe (9: Integer)
    row.getString("c10") shouldBe "10"
    row.getInt("c11") shouldBe (11: Integer)
    row.getString("c12") shouldBe "12"
    row.getInt("c13") shouldBe (13: Integer)
    row.getString("c14") shouldBe "14"
    row.getInt("c15") shouldBe (15: Integer)
    row.getString("c16") shouldBe "16"
    row.getInt("c17") shouldBe (17: Integer)
    row.getString("c18") shouldBe "18"
    row.getInt("c19") shouldBe (19: Integer)
    row.getString("c20") shouldBe "20"
    row.getInt("c21") shouldBe (21: Integer)
  }



  it should "allow to write Tuple22 to Cassandra" in {
    conn.withSessionDo(_.execute(s"TRUNCATE $ks.test_table_4"))
    val tuple = Tuple22(
      1: Integer,
      "2",
      3: Integer,
      "4",
      5: Integer,
      "6",
      7: Integer,
      "8",
      9: Integer,
      "10",
      11: Integer,
      "12",
      13: Integer,
      "14",
      15: Integer,
      "16",
      17: Integer,
      "18",
      19: Integer,
      "20",
      21: Integer,
      "22"
    )
    CassandraJavaUtil.javaFunctions(sc.makeRDD(Seq(tuple)))
      .writerBuilder(ks, "test_table_4", mapTupleToRow(
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String],
      classOf[Integer],
      classOf[String]
    )).withColumnSelector(someColumns(
      "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13", "c14", "c15", "c16", "c17", "c18", "c19", "c20", "c21", "c22"
    )).saveToCassandra()

    val row = conn.withSessionDo(_.execute(s"SELECT * FROM $ks.test_table_4").one())
    row.getInt("c1") shouldBe (1: Integer)
    row.getString("c2") shouldBe "2"
    row.getInt("c3") shouldBe (3: Integer)
    row.getString("c4") shouldBe "4"
    row.getInt("c5") shouldBe (5: Integer)
    row.getString("c6") shouldBe "6"
    row.getInt("c7") shouldBe (7: Integer)
    row.getString("c8") shouldBe "8"
    row.getInt("c9") shouldBe (9: Integer)
    row.getString("c10") shouldBe "10"
    row.getInt("c11") shouldBe (11: Integer)
    row.getString("c12") shouldBe "12"
    row.getInt("c13") shouldBe (13: Integer)
    row.getString("c14") shouldBe "14"
    row.getInt("c15") shouldBe (15: Integer)
    row.getString("c16") shouldBe "16"
    row.getInt("c17") shouldBe (17: Integer)
    row.getString("c18") shouldBe "18"
    row.getInt("c19") shouldBe (19: Integer)
    row.getString("c20") shouldBe "20"
    row.getInt("c21") shouldBe (21: Integer)
    row.getString("c22") shouldBe "22"
  }

}
