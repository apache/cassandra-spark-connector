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

import com.datastax.oss.driver.api.core.Version
import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.ccm.CcmConfig.V3_6_0
import com.datastax.spark.connector.cluster.DefaultCluster
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.types.UserDefinedType
import com.datastax.spark.connector.util.schemaFromCassandra
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.functions._
import org.scalatest.Matchers
import org.scalatest.concurrent.Eventually

import scala.concurrent.Future

class CassandraDataFrameMetadataSpec extends SparkCassandraITFlatSpecBase with DefaultCluster with Eventually with Matchers {
  override lazy val conn = CassandraConnector(defaultConf)


  conn.withSessionDo { session =>
    createKeyspace(session)
    val executor = getExecutor(session)

    def typesToCheck = {
      val ord = implicitly[Ordering[Version]]
      import ord._
      if (cluster.getCassandraVersion > V3_6_0) {
        session.execute(
          s"""
             |CREATE TABLE $ks.test_reading_types (
             |    id bigint PRIMARY KEY,
             |    list_val list<int>,
             |    list_val_frozen frozen<list<int>>,
             |    map_val map<text, int>,
             |    map_val_frozen frozen<map<text, int>>,
             |    set_val set<int>,
             |    set_val_frozen frozen<set<int>>,
             |    simple_val int,
             |    udt_frozen_val frozen<fullname>,
             |    udt_val fullname,
             |    tuple_val tuple <int, int>,
             |    tuple_val_frozen frozen<tuple<int, int>>
             |)
        """.stripMargin)
        session.execute(
          s"""
             |insert into $ks.test_reading_types (id, simple_val, list_val, list_val_frozen,
             |map_val, map_val_frozen, set_val, set_val_frozen, udt_val, udt_frozen_val, tuple_val,
             |tuple_val_frozen) values
             |(0, 1,
             |[2, 3], [2, 3],
             |{'four': 4, 'five': 5}, {'four': 4, 'five': 5},
             |{6, 7}, {6, 7},
             |{firstname: 'Joe', lastname: 'Smith'}, {firstname: 'Bredo', lastname: 'Morstoel'},
             |(1, 1), (1, 1)) USING
             |timestamp 1000
           """.stripMargin)
      } else {
        session.execute(
          s"""
             |CREATE TABLE $ks.test_reading_types (
             |    id bigint PRIMARY KEY,
             |    list_val_frozen frozen<list<int>>,
             |    map_val_frozen frozen<map<text, int>>,
             |    set_val_frozen frozen<set<int>>,
             |    simple_val int,
             |    udt_frozen_val frozen<fullname>,
             |    tuple_val_frozen frozen<tuple<int, int>>
             |)
        """.stripMargin)
        session.execute(
          s"""
             |insert into $ks.test_reading_types (id, simple_val, list_val_frozen,
             |map_val_frozen, set_val_frozen, udt_frozen_val,
             |tuple_val_frozen) values
             |(0, 1,
             |[2, 3],
             |{'four': 4, 'five': 5},
             |{6, 7},
             |{firstname: 'Joe', lastname: 'Smith'},
             |(1, 1)) USING
             |timestamp 1000
           """.stripMargin)
      }
    }


    awaitAll(
      Future {
        session.execute(
          s"""
             |CREATE TABLE $ks.basic (k INT, c INT, v INT, v2 INT, PRIMARY KEY (k,c))
             |""".stripMargin)

        val prepared = session.prepare(
          s"""INSERT INTO $ks.basic (k, c, v, v2) VALUES (?, ?, ?, ?)
             |USING TTL ? AND TIMESTAMP ?""".stripMargin)

        awaitAll(for (x <- 1 to 100) yield {
          executor.executeAsync(prepared.bind(
            x: java.lang.Integer,
            x: java.lang.Integer,
            x: java.lang.Integer,
            x: java.lang.Integer,
            ((x * 10000): java.lang.Integer),
            x.toLong: java.lang.Long))
        })
      },
      Future {
        session.execute(
          s"""
             |CREATE TYPE $ks.fullname (
             |    firstname text,
             |    lastname text
             |)
         """.stripMargin)
        typesToCheck

      },
      Future {
        session.execute(
          s"""
             |CREATE TABLE $ks."caseNames" ("Key" INT, "Value" INT, "Dot.Value" INT, PRIMARY KEY ("Key"))
             |""".stripMargin)

        session.execute(
          s"""INSERT INTO $ks."caseNames" ("Key", "Value", "Dot.Value") VALUES (1, 2, 3)
             |USING TTL 10000 AND TIMESTAMP 10000""".stripMargin)
      }
    )
    executor.waitForCurrentlyExecutingTasks()
  }

  setupCassandraCatalog


  val dseVersion = cluster.getDseVersion.getOrElse(Version.parse("6.0.0"))

  val columnsToCheck = schemaFromCassandra(conn, Some(ks), Some("test_reading_types"))
    .tables
    .head
    .regularColumns
    .filter( columnDef =>
      (dseVersion.getMajor >= 6 && dseVersion.getMinor >= 7)
        || (!(columnDef.isCollection || columnDef.columnType.isInstanceOf[UserDefinedType]))
      )

  "A DataFrame" should "be able to read TTL" in {
    val df = spark.sql(s"SELECT * FROM $ks.basic")

    val result = df.select(sum(ttl("v")).as("sum"))
      .collect()
      .head
      .getAs[Long]("sum")
    result should be > 0L
  }

  it should "handle various attribute orderings" in {
    val df =  spark.sql(s"SELECT * from $ks.test_reading_types")
      .withColumn("simple_val_TTL", ttl("simple_val"))

    val a = df.select("simple_val_TTL", "id", "map_val_frozen").collect.head.toSeq
    val b = df.select("id", "map_val_frozen", "simple_val_TTL").collect.head.toSeq
    val c = df.select("map_val_frozen", "simple_val_TTL", "id").collect.head.toSeq

    a should contain theSameElementsAs (b)
    b should contain theSameElementsAs (c)
  }

  for (col <- columnsToCheck) {
    val name = col.columnName
    it should s" handle ttl on $name ${col.columnType} with function" in {
      val df =  spark.sql(s"SELECT * from $ks.test_reading_types")
        .withColumn("ttlResult", ttl(col.columnName))

      val result = df.collect().head
      if (col.isMultiCell) {
        result.getList[Int](result.fieldIndex("ttlResult")) should contain theSameElementsAs Seq(null, null)
      } else {
        result.get(result.fieldIndex("ttlResult")).asInstanceOf[AnyRef] should be (null)
      }
    }

    it should s" handle writeTime on $name ${col.columnType} with function" in {
      val df =  spark.sql(s"SELECT * from $ks.test_reading_types")
        .withColumn("writeTimeResult", writeTime(col.columnName))

      val result = df.collect().head
      if (col.isMultiCell) {
        result.getList[Long](result.fieldIndex("writeTimeResult")) should contain theSameElementsAs Seq(1000, 1000)
      } else {
        result.get(result.fieldIndex("writeTimeResult")).asInstanceOf[AnyRef] should be (1000)
      }
    }
  }

  it should "be able to read multiple TTLs" in {
    val df = spark.sql(s"SELECT * FROM $ks.basic")
      .select(ttl("v") as "ttlOfV", ttl("v2") as "ttlOfV2")

    val result = df.select(sum("ttlOfV").as("sum"))
      .collect()
      .head
      .getAs[Long]("sum")
    result should be > 0L

    val result2 = df.select(sum("ttlOfV2").as("sum"))
      .collect()
      .head
      .getAs[Long]("sum")
  }

  it should "be able to read TTL using the function api" in {
    val df = spark.sql(s"SELECT * FROM $ks.basic")
      .select(ttl("v"))

    val result = df.select(sum("TTL(V)").as("sum"))
      .collect()
      .head
      .getAs[Long]("sum")
    result should be > 0L
  }

  it should "be able to read TTL using the function api and get the column" in {
    val df = spark.sql(s"SELECT * FROM $ks.basic")
      .select(col("v"), ttl("v"))

    val result = df.select(sum("TTL(V)").as("sum"))
      .collect()
      .head
      .getAs[Long]("sum")
    result should be > 0L
  }

  it should "be able to read TTL using the function api and get the column from subsequent call" in {
    val df = spark.sql(s"SELECT * FROM $ks.basic")
      .select(col("v"))
      .select(ttl("v"))

    val result = df.select(sum("TTL(V)").as("sum"))
      .collect()
      .head
      .getAs[Long]("sum")
    result should be > 0L
  }

  it should "return null TTL for lit(null) column" in {
    val df = spark.sql(s"SELECT * FROM $ks.basic")
      .select("v").limit(1)

    import spark.implicits._
    val nullDf = (Seq((1))).toDF("n").select (lit(null) as "v")


    val unionDf = (df union nullDf).select(ttl("v") as ("ttl"))
    val result = unionDf.collect()

    result(0).getAs[Int](0) should be > 0
    result(1).getAs[AnyRef](0) should be (null)
  }

  it should "return null TTL for lit(null) reversed column" in {
    val df = spark.sql(s"SELECT * FROM $ks.basic")
      .select("v").limit(1)

    import spark.implicits._
    val nullDf = (Seq((1))).toDF("n").select (lit(null) as "v")

    val unionDf = (nullDf union df).select(ttl("v") as ("ttl"))
    val result = unionDf.collect()

    result(1).getAs[Int](0) should be > 0
    result(0).getAs[AnyRef](0) should be (null)
  }

  it should "fail trying to read TTL from non-regular columns" in intercept[AnalysisException]{
    val df = spark.sql(s"SELECT * FROM $ks.basic")
      .withColumn("ttlOfV", ttl("v"))
      .withColumn("ttlOfC", ttl("c"))
      .collect()
  }

  it should "throw an exception when reading writetime from non-regular columns" in intercept[AnalysisException]{
    val df = spark.sql(s"SELECT * FROM $ks.basic")
      .withColumn("writeTimeOfV", writeTime("v"))
      .withColumn("writeTimeOfC", writeTime("c"))
      .collect()
  }

  it should "be able to write ttl" in {
    spark
      .createDataFrame(Seq((-1,-1,-1,10000)))
      .toDF("k","c","v","ttlCol")
      .write
      .cassandraFormat("basic", ks)
      .option("ttl", "ttlCol")
      .mode("APPEND")
      .save()

    val result = conn.withSessionDo(_.execute(s"SELECT TTL(v) as t FROM $ks.basic WHERE k = -1"))
    result.one().getInt("t") shouldBe (9000 +- 1000)
  }

  it should "be able to write ttl as first column" in {
    spark
      .createDataFrame(Seq((10000,-1,-1,-2)))
      .toDF("ttlCol","k","c","v")
      .write
      .cassandraFormat("basic", ks)
      .option("ttl", "ttlCol")
      .mode("APPEND")
      .save()

    val result = conn.withSessionDo(_.execute(s"SELECT TTL(v) as t FROM $ks.basic WHERE k = -1"))
    result.one().getInt("t") shouldBe (9000 +- 1000)
  }

  it should "be able to write ttl literals" in {
     spark
      .createDataFrame(Seq((-500,-1,-1)))
      .toDF("k","c","v")
      .write
      .cassandraFormat("basic", ks)
      .withTTL(50000)
      .mode("APPEND")
      .save()

    val result = conn.withSessionDo(_.execute(s"SELECT TTL(v) as t FROM $ks.basic WHERE k = -500"))
    result.one().getInt("t") shouldBe (50000 +- 1000)
  }

  it should "be able to write ttl withTTL" in {
    spark
      .createDataFrame(Seq((-50,-1,-1,10000)))
      .toDF("k","c","v","ttlCol")
      .write
      .cassandraFormat("basic", ks)
      .withTTL("ttlCol")
      .mode("APPEND")
      .save()

    val result = conn.withSessionDo(_.execute(s"SELECT TTL(v) as t FROM $ks.basic WHERE k = -50"))
    result.one().getInt("t") shouldBe (9000 +- 1000)
  }

   it should "be able to write withWritetime" in {
    spark
      .createDataFrame(Seq((-501,-2,-2)))
      .toDF("k","c","v")
      .write
      .cassandraFormat("basic", ks)
      .withWriteTime(5000)
      .mode("APPEND")
      .save()

    val result = conn.withSessionDo(_.execute(s"SELECT WRITETIME(v) as t FROM $ks.basic WHERE k = -501"))
    result.one().getLong("t") shouldBe (5000L)
  }

  it should "be able to write writeTime literals" in {
    spark
      .createDataFrame(Seq((-51,-2,-2,10000)))
      .toDF("k","c","v","writetimeCol")
      .write
      .cassandraFormat("basic", ks)
      .withWriteTime("writetimeCol")
      .mode("APPEND")
      .save()

    val result = conn.withSessionDo(_.execute(s"SELECT WRITETIME(v) as t FROM $ks.basic WHERE k = -51"))
    result.one().getLong("t") shouldBe (10000L)
  }

  it should "throw an exception when attempting to use withWriteTime or withTTL on non-Cassandra sources" in {
     intercept[IllegalArgumentException] {
       spark
         .createDataFrame(Seq((-51,-2,-2,10000)))
         .toDF("k","c","v","writetimeCol")
         .write
         .withWriteTime("writetimeCol")
         .mode("APPEND")
         .save()
     }
    intercept[IllegalArgumentException] {
       spark
         .createDataFrame(Seq((-51,-2,-2,10000)))
         .toDF("k","c","v","writetimeCol")
         .write
         .withWriteTime("writetimeCol")
         .mode("APPEND")
         .save()
     }
  }


  it should "be able to write writetime" in {
    spark
      .createDataFrame(Seq((-2,-2,-2,10000)))
      .toDF("k","c","v","writetimeCol")
      .write
      .cassandraFormat("basic", ks)
      .option("writetime", "writetimeCol")
      .mode("APPEND")
      .save()

    val result = conn.withSessionDo(_.execute(s"SELECT WRITETIME(v) as t FROM $ks.basic WHERE k = -2"))
    result.one().getLong("t") shouldBe (10000L)
  }

  "Spark SQL" should "be able to read TTL" in {
    spark.sql(s"SELECT sum(ttl(v)) FROM $ks.basic")
      .collect()
      .head.getLong(0) should be > 1000L
  }

  it should "be able to read WRITETIME" in {
    spark.sql(s"SELECT sum(writetime(v)) FROM $ks.basic")
      .collect()
      .head.getLong(0) should be > 1000L
  }

  it should "be able to read TTL from case sensitive column" in {

    val request = spark.sql(s"SELECT ttl(Value) FROM $ks.caseNames")
    request.explain(true)
    request
      .collect()
      .head.getInt(0) should be > 1000
  }

  it should "be able to read WRITETIME from case sensitive column" in {
    spark.sql(s"SELECT writetime(`Dot.Value`) FROM $ks.caseNames")
      .collect()
      .head.getLong(0) should be (10000L)
  }


  it should "throw an exception when calling writetime on more than one column" in intercept[AnalysisException] {
    spark.sql(s"SELECT sum(writetime(v, k)) FROM $ks.basic")
  }

  it should "throw an exception when calling ttl on more than one column" in intercept[AnalysisException] {
    spark.sql(s"SELECT sum(ttl(v, k)) FROM $ks.basic")
  }

}

