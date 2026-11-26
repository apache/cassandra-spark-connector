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

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cluster.DefaultCluster
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._

// UDTs
case class File(data: Array[Byte])

case class Profile(name: String, picture: File)

// Tables
case class Files(id: Int, file: File)

case class Profiles(id: Int, profile: Profile)

class UserDefinedTypeSpec extends SparkCassandraITFlatSpecBase with DefaultCluster {

  override lazy val conn = CassandraConnector(sparkConf)

  val FilesTable = "files"
  val ProfilesTable = "profiles"

  def makeUdtTables(session: CqlSession): Unit = {
    session.execute(s"""CREATE TYPE IF NOT EXISTS $ks.file (data blob);""")
    session.execute(
      s"""CREATE TABLE IF NOT EXISTS $ks.$FilesTable
         |(id int PRIMARY KEY, file frozen<file>);""".stripMargin)

    session.execute(s"""CREATE TYPE IF NOT EXISTS $ks.profile (name text, picture frozen<file>)""")
    session.execute(
      s"""CREATE TABLE IF NOT EXISTS $ks.$ProfilesTable
         |(id int PRIMARY KEY, profile frozen<profile>)""".stripMargin)
  }

  override def beforeClass {
    conn.withSessionDo { session =>
      session.execute(
        s"""CREATE KEYSPACE IF NOT EXISTS $ks
           |WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }"""
          .stripMargin)
      makeUdtTables(session)
    }
  }

  "SparkSql" should "write UDTs with BLOB fields" in {
    val expected = File(":)".getBytes)
    spark.createDataFrame(Seq(Files(1, expected)))
      .write
      .cassandraFormat(FilesTable, ks)
      .mode("append")
      .save()
    val row = spark.sparkContext
      .cassandraTable[Files](ks, FilesTable)
      .collect()
      .head
    row.file.data shouldEqual expected.data
  }

  it should "write nested UDTs" in {
    val expected = Profile("John Smith", File(":)".getBytes))
    spark.createDataFrame(Seq(Profiles(1, expected)))
      .write
      .cassandraFormat(ProfilesTable, ks)
      .mode("append")
      .save()
    val row = spark.sparkContext
      .cassandraTable[Profiles](ks, ProfilesTable)
      .collect()
      .head
    row.profile.name shouldEqual expected.name
    row.profile.picture.data shouldEqual expected.picture.data
  }

}