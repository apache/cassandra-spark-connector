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

import java.io.IOException
import com.datastax.oss.driver.api.core.{CqlIdentifier, CqlSession, DefaultProtocolVersion}
import com.datastax.oss.driver.api.core.`type`.DataTypes
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder
import com.datastax.spark.connector.cluster.DefaultCluster
import com.datastax.spark.connector.{SparkCassandraITFlatSpecBase, _}
import com.datastax.spark.connector.cql.{AuthConf, AuthConfFactory, CassandraConnectionFactory, CassandraConnector, CassandraConnectorConf, ClusteringColumn, DefaultConnectionFactory, NoAuthConf}
import com.datastax.spark.connector.util.DriverUtil.toName
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.joda.time.LocalDate
import org.scalatest.concurrent.Eventually

import scala.jdk.CollectionConverters._
import scala.concurrent.Future

case class RowWithV4Types(key: Int, a: Byte, b: Short, c: java.sql.Date)
case class TestData(id: String, col1: Int, col2: Int)

class CassandraDataFrameSpec extends SparkCassandraITFlatSpecBase with DefaultCluster with Eventually{

  override lazy val conn = CassandraConnector(defaultConf)

  def pushDown: Boolean = true

  override def beforeClass {
    conn.withSessionDo { session =>
      createKeyspace(session)
      val executor = getExecutor(session)

      awaitAll(
        Future {
          session.execute(
            s"""
               |CREATE TABLE $ks.kv_copy (k INT, v TEXT, PRIMARY KEY (k))
               |""".stripMargin)
        },

        Future {
          session.execute(
            s"""
               |CREATE TABLE $ks.hardtoremembernamedtable (k INT, v TEXT, PRIMARY KEY (k))
               |""".stripMargin)
        },

        Future {
          session.execute(
            s"""
               |CREATE TABLE IF NOT EXISTS $ks.kv (k INT, v TEXT, PRIMARY KEY (k))
               |""".stripMargin)

          val prepared = session.prepare( s"""INSERT INTO $ks.kv (k, v) VALUES (?, ?)""")

          awaitAll(for (x <- 1 to 1000) yield {
            executor.executeAsync(prepared.bind(x: java.lang.Integer, x.toString))
          })
        },

        Future {
          session.execute(s"CREATE TABLE $ks.tuple_test1 (id int, t Tuple<text, int>, PRIMARY KEY (id))")
          session.execute(s"CREATE TABLE $ks.tuple_test2 (id int, t Tuple<text, int>, PRIMARY KEY (id))")
          session.execute(s"INSERT INTO $ks.tuple_test1 (id, t) VALUES (1, ('xyz', 3))")
        },

        Future {
          info("Setting up Date Tables")
          skipIfProtocolVersionLT(DefaultProtocolVersion.V4) {
            session.execute(s"create table $ks.date_test (key int primary key, dd date)")
            session.execute(s"create table $ks.date_test2 (key int primary key, dd date)")
            session.execute(s"insert into $ks.date_test (key, dd) values (1, '1930-05-31')")
          }
        },

        Future {
          session.execute(
            s"""
               |CREATE TABLE $ks.timeuuidtable (k INT, v TIMEUUID, PRIMARY KEY (k))
               |""".stripMargin)
        },

        Future {
          session.execute(
            s"""
               |CREATE TABLE $ks.camelcasecolumns (k INT, \"camelCase\" INT, PRIMARY KEY (k))
               |""".stripMargin)
        }
      )
      executor.waitForCurrentlyExecutingTasks()
    }
  }

  "A DataFrame" should "be able to be created programmatically" in {
    val df = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(
        Map(
          "table" -> "kv",
          "keyspace" -> ks
        )
      )
      .load()

    df.count() should be(1000)
  }

  it should "be able to be saved programmatically" in {
    val df = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(
        Map(
          "table" -> "kv",
          "keyspace" -> ks
        )
      )
      .load()

    df.write
      .format("org.apache.spark.sql.cassandra")
      .options(
        Map(
          "table" -> "kv_copy",
          "keyspace" -> ks,
          "spark.cassandra.output.ttl" -> "300",
          "spark.cassandra.output.timestamp" -> "1470009600000000"
        )
      )
      .mode("append")
      .save()

    val dfCopy = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(
        Map(
          "table" -> "kv_copy",
          "keyspace" -> ks
        )
      )
      .load()

    dfCopy.count() should be (1000)

    val ttl = conn.withSessionDo { session =>
      val rs = session.execute(s"""SELECT TTL(v) from $ks.kv_copy""")
      rs.one().getInt(0)
    }

    ttl should be > 0
    ttl should be <= 300

    val writeTime = conn.withSessionDo { session =>
      val rs = session.execute(s"""SELECT WRITETIME(v) from $ks.kv_copy""")
      rs.one().getLong(0)
    }

    writeTime shouldEqual 1470009600000000L
  }

  it should " be able to create a C* schema from a table" in {
     val df = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(
        Map(
          "table" -> "kv",
          "keyspace" -> ks
        )
      )
      .load()

    df.createCassandraTable(ks, "kv_auto", Some(Seq("v")), Some(Seq("k")))

    val meta = conn.withSessionDo(_.getMetadata)
    val autoTableMeta = meta.getKeyspace(ks).get().getTable("kv_auto").get()
    autoTableMeta.getPartitionKey.asScala.map(k => toName(k.getName)) should contain ("v")
    autoTableMeta.getClusteringColumns.asScala.map(c => toName(c._1.getName)) should contain ("k")

  }

  it should " be able to create a customized C* schema from a table" in {
    val df = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(
        Map(
          "table" -> "kv",
          "keyspace" -> ks
        )
      )
      .load()

    df.createCassandraTableEx(ks, "kv_auto2", Seq("v"), Seq(("k", ClusteringColumn.Descending)),
      tableOptions = Map("gc_grace_seconds" -> "1000"))

    val meta = conn.withSessionDo(_.getMetadata)
    val autoTableMeta = meta.getKeyspace(ks).get().getTable("kv_auto2").get()
    autoTableMeta.getPartitionKey.asScala.map(k => toName(k.getName)) should contain ("v")
    autoTableMeta.getClusteringColumns.asScala.map(c => toName(c._1.getName)) should contain ("k")
    autoTableMeta.getClusteringColumns.asScala.map(_._2) should contain (ClusteringOrder.DESC)
    autoTableMeta.getOptions.getOrDefault(CqlIdentifier.fromCql("gc_grace_seconds"), "0").toString should equal ("1000")
  }

  it should " provide useful messages when creating a table with columnName mismatches" in {
    val df = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(
        Map(
          "table" -> "kv",
          "keyspace" -> ks
        )
      )
      .load()

    val pkError = intercept[IllegalArgumentException] {
      df.createCassandraTable(ks, "kv_auto", Some(Seq("cara")))
    }
    pkError.getMessage should include ("\"cara\" not Found.")

    val ccError = intercept[IllegalArgumentException] {
      df.createCassandraTable(ks, "kv_auto", Some(Seq("k")), Some(Seq("sundance")))
    }
    ccError.getMessage should include ("\"sundance\" not Found.")

  }

  it should " provide error out with a sensible message when a table can't be found" in {
    val exception = intercept[NoSuchTableException] {
      val df = spark
        .read
        .format("org.apache.spark.sql.cassandra")
        .options(
          Map(
            "table" -> "randomtable",
            "keyspace" -> ks
          )
        )
        .load()
    }
    exception.getMessage should include("Couldn't find")
  }

  it should " provide useful suggestions if a table can't be found but a close match exists" in {
    val exception = intercept[NoSuchTableException] {
      val df = spark
        .read
        .format("org.apache.spark.sql.cassandra")
        .options(
          Map(
            "table" -> "hardertoremembertablename",
            "keyspace" -> ks
          )
        )
        .load
    }
    exception.getMessage should include("Couldn't find")
    exception.getMessage should include("hardtoremembernamedtable")
  }

  it should "read and write C* Tuple columns" in {
    val df = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "tuple_test1", "keyspace" -> ks, "cluster" -> "ClusterOne"))
      .load

    df.count should be (1)
    df.first.getStruct(1).getString(0) should be ("xyz")
    df.first.getStruct(1).getInt(1) should be (3)

    df.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "tuple_test2", "keyspace" -> ks, "cluster" -> "ClusterOne"))
      .mode("append")
      .save

    conn.withSessionDo { session =>
      session.execute(s"select count(1) from $ks.tuple_test2").one().getLong(0) should be (1)
    }
  }

  it should "read and write C* LocalDate columns" in skipIfProtocolVersionLT(DefaultProtocolVersion.V4){
    val df = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "date_test", "keyspace" -> ks, "cluster" -> "ClusterOne"))
      .load

    df.count should be (1)
    df.first.getDate(1) should be (new LocalDate(1930, 5, 31).toDate)

    df.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "date_test2", "keyspace" -> ks, "cluster" -> "ClusterOne"))
      .mode("append")
      .save

    conn.withSessionDo { session =>
      session.execute(s"select count(1) from $ks.date_test2").one().getLong(0) should be (1)
    }
  }

  it should "be able to write to ProtocolVersion 3 Tables correctly with V4 Types" in skipIfProtocolVersionGTE(DefaultProtocolVersion.V4){

    val table = "newtypetable"

    val rdd = sc.parallelize(1 to 100).map( x =>
      RowWithV4Types(x, Byte.MinValue, Short.MinValue, java.sql.Date.valueOf("2016-08-03")))

    val df = spark.createDataFrame(rdd)
    df.createCassandraTable(ks, table)

    val tableColumns = eventually(
      conn.withSessionDo(_.getMetadata.getKeyspace(ks).get().getTable(table)).get().getColumns.asScala.map(_._2.getType))

    tableColumns should contain theSameElementsAs (
      Seq(DataTypes.INT, DataTypes.INT, DataTypes.INT, DataTypes.TIMESTAMP)
      )

    df.write.cassandraFormat(table, ks).mode("append").save()

    val  rows = spark
      .read
      .cassandraFormat(table, ks)
      .load()
      .collect
      .map(row => (row.getInt(1), row.getInt(2), row.getTimestamp(3).toString))

    val firstRow = rows(0)
    firstRow should be((Byte.MinValue.toInt, Short.MinValue.toInt, "2016-08-03 00:00:00.0"))
  }

  //Test whether Pruned Source Reused Exchange is Broken
  it should "aggregate and union correctly" in {
    val table = "sparkc429"

    val data = List(TestData("A", 1, 7))
    val frame = spark
      .sqlContext
      .createDataFrame(spark.sparkContext.parallelize(data))

    frame.createCassandraTable(
      ks,
      table,
      partitionKeyColumns = Some(Seq("id")))

    frame
      .write
      .format("org.apache.spark.sql.cassandra")
      .mode(SaveMode.Append)
      .options(Map("table" -> table, "keyspace" -> ks))
      .save()

    val loaded = spark.sqlContext
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> table, "keyspace" -> ks))
      .load()
      .select("id", "col1", "col2")

    val min1 = loaded.groupBy("id").agg(min("col1").as("min"))
    val min2 = loaded.groupBy("id").agg(min("col2").as("min"))
    val m1 = min1.union(min2).collect
    val m2 = min2.union(min1).collect
    m1 should contain theSameElementsAs m2

  }

  it should "complain when non-equality predicate on timeuuid is detected" in cassandraOnly {
    val df = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(
        Map(
          "table" -> "timeuuidtable",
          "keyspace" -> ks
        )
      )
      .load()
      .where(col("v") < "61129592-FBE4-11E3-A3AC-0800200C9A66")


    intercept[IllegalArgumentException] {
      df.count()
    }
  }

  it should "allow to specify custom per-cluster settings" in {
    // active session is used for consolidating confs
    SparkSession.setActiveSession(spark)

    spark.conf.set("myCluster/spark.cassandra.connection.factory",
      TestConnectionFactory.getClass.getName.split("\\$").last)
    spark.conf.set("myCluster/spark.cassandra.auth.conf.factory",
      TestAuthFactory.getClass.getName.split("\\$").last)
    spark.conf.set("myCluster/spark.cassandra.test.custom.property", "specialValue")
    spark.conf.set("myCluster/spark.cassandra.connection.keepAliveMS", "0")

    spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "tuple_test1", "keyspace" -> ks, "cluster" -> "myCluster"))
      .load
      .count()

    withClue("Test auth factory was not used during the test") {
      TestAuthFactory.used shouldBe true
    }
  }

  it should "properly join tables with mixed case column names" in {
    val df1 = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "camelcasecolumns", "keyspace" -> ks))
      .option("directJoinSetting", "on")
      .load

    val df2 = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "camelcasecolumns", "keyspace" -> ks))
      .option("directJoinSetting", "on")
      .load

    val other = df1.join(df2, df1.col("camelCase").equalTo(df2.col("camelCase")))
    other.count should be >= 0L
  }
}

object TestConnectionFactory extends CassandraConnectionFactory {
  override def createSession(conf: CassandraConnectorConf): CqlSession =
    DefaultConnectionFactory.createSession(conf)

  override def properties: Set[String] = Set("spark.cassandra.test.custom.property")
}

object TestAuthFactory extends AuthConfFactory {
  var used: Boolean = false

  override def authConf(conf: SparkConf): AuthConf = {
    used = true
    assert(conf.get("spark.cassandra.test.custom.property") == "specialValue")
    NoAuthConf
  }
}