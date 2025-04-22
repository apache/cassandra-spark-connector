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

package com.datastax.spark.connector.types

import com.datastax.dse.driver.api.core.`type`.DseDataTypes
import com.datastax.dse.driver.api.core.data.geometry.{LineString, Point, Polygon}
import com.datastax.dse.driver.internal.core.`type`.codec.geometry.PointCodec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.spark.connector._
import com.datastax.spark.connector.cluster.DefaultCluster
import com.datastax.spark.connector.cql.CassandraConnector

class GeometricTypeSpec extends SparkCassandraITFlatSpecBase with DefaultCluster {

  override lazy val conn = CassandraConnector(sparkConf)

  def makeGeometricTables(session: CqlSession): Unit = {
    session.execute(
      s"""CREATE TABLE $ks.geom
          |(k INT PRIMARY KEY,
          | pnt 'PointType',
          | line 'LineStringType',
          | poly 'PolygonType') """.stripMargin)

    session.execute(
      s"""
         |Insert INTO $ks.geom (k, pnt, line, poly) VALUES
         |(1,
         |'POINT (1.1 2.2)',
         |'LINESTRING (30 10, 10 30, 40 40)',
         |'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))')
       """.stripMargin
    )
  }

  override def beforeClass {
    dseOnly
    {
      conn.withSessionDo { session =>
        session.execute(
          s"""CREATE KEYSPACE IF NOT EXISTS $ks
             |WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }"""
            .stripMargin)
        makeGeometricTables(session)
      }
    }
  }

  "The Spark Cassandra Connector" should "find a converter for Point types" in dseOnly {
    ColumnType.fromDriverType(DseDataTypes.POINT) should be(PointType)
  }

  it should "find a converter for Polygon types" in dseOnly {
    ColumnType.fromDriverType(DseDataTypes.POLYGON) should be(PolygonType)
  }

  it should "find a converter for LineString types" in dseOnly {
    ColumnType.fromDriverType(DseDataTypes.LINE_STRING) should be(LineStringType)
  }

  it should "read point types" in dseOnly {
    val result = sc.cassandraTable(ks, "geom").select("pnt").collect
    val resultCC = sc.cassandraTable[(Point)](ks, "geom").select("pnt").collect
    val expected = Point.fromCoordinates(1.1, 2.2)
    result(0).get[Point](0) should be(expected)
    resultCC(0) should be(expected)
  }

  it should "read polygon types" in dseOnly {
    val result = sc.cassandraTable(ks, "geom").select("poly").collect
    val resultCC = sc.cassandraTable[(Polygon)](ks, "geom").select("poly").collect
    val expected = Polygon.fromPoints(
      Point.fromCoordinates(30, 10),
      Point.fromCoordinates(40, 40),
      Point.fromCoordinates(20, 40),
      Point.fromCoordinates(10, 20),
      Point.fromCoordinates(30, 10))

    result(0).get[Polygon](0) should be(expected)
    resultCC(0) should be(expected)
  }

  it should "read linestring types" in dseOnly {
    val result = sc.cassandraTable(ks, "geom").select("line").collect
    val resultCC = sc.cassandraTable[(LineString)](ks, "geom").select("line").collect
    val expected = LineString.fromPoints(
      Point.fromCoordinates(30, 10),
      Point.fromCoordinates(10, 30),
      Point.fromCoordinates(40, 40))

    result(0).get[LineString](0) should be(expected)
    resultCC(0) should be(expected)
  }

  it should "write point types" in dseOnly {
    val expected = Point.fromCoordinates(1.1, 2.2)
    sc.parallelize(Seq((2, expected))).saveToCassandra(ks, "geom", SomeColumns("k", "pnt"))
    val result = conn.withSessionDo(_.execute(s"SELECT pnt FROM $ks.geom where k = 2").one())
    result.get[Point](0, classOf[Point]) should be(expected)
  }

  it should "write polygon types" in dseOnly {
    val expected = Polygon.fromPoints(
      Point.fromCoordinates(30, 10),
      Point.fromCoordinates(40, 40),
      Point.fromCoordinates(20, 40),
      Point.fromCoordinates(10, 20),
      Point.fromCoordinates(30, 10))

    sc.parallelize(Seq((2, expected))).saveToCassandra(ks, "geom", SomeColumns("k", "poly"))
    val result = conn.withSessionDo(_.execute(s"SELECT poly FROM $ks.geom where k = 2").one())
    result.get[Polygon](0, classOf[Polygon]) should be(expected)
  }

  it should "write linestring types" in dseOnly {
    val expected = LineString.fromPoints(
      Point.fromCoordinates(30, 10),
      Point.fromCoordinates(10, 30),
      Point.fromCoordinates(40, 40))

    sc.parallelize(Seq((2, expected))).saveToCassandra(ks, "geom", SomeColumns("k", "line"))
    val result = conn.withSessionDo(_.execute(s"SELECT line FROM $ks.geom where k = 2").one())
    result.get[LineString](0, classOf[LineString]) should be(expected)
  }

  def readGeoDF: DataFrame = spark.read.format("org.apache.spark.sql.cassandra").option("table", "geom").option("keyspace", ks).load()

  "SparkSQL" should "read point types" in dseOnly {
    val result = readGeoDF.select("pnt").collect
    val expected = Point.fromCoordinates(1.1, 2.2)
    Point.fromWellKnownText(result(0).getString(0)) should be(expected)
  }

  it should "read polygon types" in dseOnly {
    val result = readGeoDF.select("poly").collect
    val expected = Polygon.fromPoints(
      Point.fromCoordinates(30, 10),
      Point.fromCoordinates(40, 40),
      Point.fromCoordinates(20, 40),
      Point.fromCoordinates(10, 20),
      Point.fromCoordinates(30, 10))
    Polygon.fromWellKnownText(result(0).getString(0)) should be(expected)
  }

  it should "read linestring types" in dseOnly {
    val result = readGeoDF.select("line").collect
    val expected = LineString.fromPoints(
      Point.fromCoordinates(30, 10),
      Point.fromCoordinates(10, 30),
      Point.fromCoordinates(40, 40))
    LineString.fromWellKnownText(result(0).getString(0)) should be(expected)
  }

  def writeGeoDF(df: DataFrame): Unit = df.write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra").options(Map("table" -> "geom", "keyspace" -> ks)).save

  it should "write point types" in dseOnly {
    val expected = Point.fromCoordinates(1.1, 2.2)
    val df = spark.createDataFrame(Seq((3, expected.toString))).select(col("_1") as "k", col("_2") as "pnt")
    writeGeoDF(df)
    val result = conn.withSessionDo(_.execute(s"SELECT pnt FROM $ks.geom where k = 3").one())
    result.get[Point](0, classOf[Point]) should be(expected)
  }

  it should "write polygon types" in dseOnly {
    val expected = Polygon.fromPoints(
      Point.fromCoordinates(30, 10),
      Point.fromCoordinates(40, 40),
      Point.fromCoordinates(20, 40),
      Point.fromCoordinates(10, 20),
      Point.fromCoordinates(30, 10))
    val df = spark.createDataFrame(Seq((3, expected.toString))).select(col("_1") as "k", col("_2") as "poly")
    writeGeoDF(df)
    val result = conn.withSessionDo(_.execute(s"SELECT poly FROM $ks.geom where k = 3").one())
    result.get[Polygon](0, classOf[Polygon]) should be(expected)
  }

  it should "write linestring types" in dseOnly {
    val expected = LineString.fromPoints(
      Point.fromCoordinates(30, 10),
      Point.fromCoordinates(10, 30),
      Point.fromCoordinates(40, 40))
    val df = spark.createDataFrame(Seq((3, expected.toString))).select(col("_1") as "k", col("_2") as "line")
    writeGeoDF(df)
    val result = conn.withSessionDo(_.execute(s"SELECT line FROM $ks.geom where k = 3").one())
    result.get[LineString](0, classOf[LineString]) should be(expected)
  }
}
