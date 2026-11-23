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

import java.sql.Date
import java.time.LocalDate
import java.util.TimeZone

import com.datastax.oss.driver.api.core.DefaultProtocolVersion
import com.datastax.spark.connector.SparkCassandraITSpecBase
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.scalatest.FlatSpec

trait CassandraDataFrameDateBehaviors extends SparkCassandraITSpecBase {
  this: FlatSpec =>

  override lazy val conn = CassandraConnector(defaultConf)

  def dataFrame(timeZone: TimeZone): Unit = skipIfProtocolVersionLT(DefaultProtocolVersion.V4){

    TimeZone.setDefault(timeZone)

    val readTable = s"date_test_${timeZone.getID.toLowerCase}_read"
    val writeTable = s"date_test_${timeZone.getID.toLowerCase}_write"

    conn.withSessionDo { session =>
      createKeyspace(session)
      session.execute(s"create table $ks.$readTable (key int primary key, dd date)")
      session.execute(s"insert into $ks.$readTable (key, dd) values (1, '1930-05-31')")
      session.execute(s"create table $ks.$writeTable (key int primary key, d0 date)")
    }

    it should s"read C* LocalDate columns in ${timeZone.getID} timezone" in {
      val df = spark
        .read
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> readTable, "keyspace" -> ks, "cluster" -> "ClusterOne"))
        .load

      df.count should be(1)

      val foundDate = df.first.getDate(1)
      val foundLocalDate = foundDate.toLocalDate
      val foundTuple = (foundLocalDate.getYear, foundLocalDate.getMonthValue, foundLocalDate.getDayOfMonth)

      val expectedTuple = (1930, 5, 31)

      foundTuple should be(expectedTuple)
    }

    it should s"write java.sql.date to C* date columns in ${timeZone.getID} timezone" in {
      val schema = StructType(Seq(
        StructField("key", DataTypes.IntegerType),
        StructField("d0", DataTypes.DateType)
      ))

      val rows = sc.parallelize(Seq(
        Row(0, Date.valueOf("1986-01-02")),
        Row(1, Date.valueOf("1987-01-02"))
      ))

      val dataFrame = spark.createDataFrame(rows, schema)

      dataFrame.write
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> writeTable, "keyspace" -> ks, "cluster" -> "ClusterOne"))
        .mode("append")
        .save

      conn.withSessionDo { session =>
        val count = session.execute(s"select count(1) from $ks.$writeTable").one().getLong(0)
        count should be(2)

        val date = session.execute(s"select d0 from $ks.$writeTable where key = 0").one().getLocalDate(0)
        date should be(LocalDate.of(1986, 1, 2))
      }
    }
  }
}
