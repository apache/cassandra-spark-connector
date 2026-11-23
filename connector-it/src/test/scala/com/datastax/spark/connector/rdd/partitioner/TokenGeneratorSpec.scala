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

package com.datastax.spark.connector.rdd.partitioner

import java.util.concurrent.CompletableFuture

import com.datastax.oss.driver.api.core.metadata.token.Token
import com.datastax.spark.connector.cluster.DefaultCluster
import com.datastax.spark.connector.cql.{CassandraConnector, Schema}
import com.datastax.spark.connector.util.schemaFromCassandra
import com.datastax.spark.connector.writer.RowWriterFactory
import com.datastax.spark.connector.{PartitionKeyColumns, SparkCassandraITFlatSpecBase}

import scala.jdk.CollectionConverters._
import scala.concurrent.Future

class TokenGeneratorSpec extends SparkCassandraITFlatSpecBase with DefaultCluster {

  case class SimpleKey(key: Int)
  case class ComplexKey(key1: Int, key2: Int, key3: String)

  override lazy val conn = CassandraConnector(defaultConf)

  val simpleKeys = Seq(1, -500, 2801, 4000000, -95500).map(SimpleKey(_))
  val complexKey = simpleKeys.map{ case SimpleKey(x) => ComplexKey(x, x, x.toString)}

  conn.withSessionDo { session =>
    createKeyspace(session)
    val executor = getExecutor(session)
    awaitAll(
      Future{session.execute(s"CREATE TABLE $ks.simple(key INT PRIMARY KEY)")},
      Future{session.execute(s"CREATE TABLE $ks.complex(key1 INT, key2 INT, key3 text, PRIMARY KEY ((key1, key2, key3)))")}
    )
    val simplePS = session.prepare(s"INSERT INTO $ks.simple (key) VALUES (?)")
    val complexPS = session.prepare(s"INSERT INTO $ks.complex (key1, key2, key3) VALUES (?, ?, ?)")
    awaitAll {
      simpleKeys.map(simpleKey =>
        executor.executeAsync(simplePS.bind(simpleKey.key: java.lang.Integer))) ++
      complexKey.map(complexKey =>
        executor.executeAsync(complexPS.bind(
          complexKey.key1: java.lang.Integer,
          complexKey.key2: java.lang.Integer,
          complexKey.key3: java.lang.String)))
    }
    executor.waitForCurrentlyExecutingTasks()
  }

  val simpleTokenMap: Map[SimpleKey, Token] = conn.withSessionDo { session =>
    val resultSet = session.execute(s"SELECT key, TOKEN(key) FROM $ks.simple").all().asScala
    resultSet.map(row => SimpleKey(row.getInt("key")) -> row.getToken(1)).toMap
  }

  val complexTokenMap: Map[ComplexKey, Token] = conn.withSessionDo { session =>
    val resultSet = session
      .execute(s"SELECT key1, key2, key3, TOKEN(key1, key2, key3) FROM $ks.complex")
      .all()
      .asScala

    resultSet.map(row =>
      ComplexKey(row.getInt("key1"), row.getInt("key2"), row.getString("key3")) ->
        row.getToken(3)
    ).toMap
  }

  val simpleTableDef = schemaFromCassandra(conn, Some(ks), Some("simple")).tables.head
  val complexTableDef = schemaFromCassandra(conn, Some(ks), Some("complex")).tables.head

  val simpleRW = implicitly[RowWriterFactory[SimpleKey]]
    .rowWriter(simpleTableDef, PartitionKeyColumns.selectFrom(simpleTableDef))

  val complexRW = implicitly[RowWriterFactory[ComplexKey]]
    .rowWriter(complexTableDef, PartitionKeyColumns.selectFrom(complexTableDef))

  "TokenGenerators" should "be able to determine simple partition keys" in {

    val tokenGenerator = new TokenGenerator[SimpleKey](conn, simpleTableDef, simpleRW)
    for ((key, token) <- simpleTokenMap) {
      tokenGenerator.getTokenFor(key) should be (token)
    }
  }

  it should "be able to determine composite keys" in {
    val tokenGenerator = new TokenGenerator[ComplexKey](conn, complexTableDef, complexRW)
    for ((key, token) <- complexTokenMap) {
      tokenGenerator.getTokenFor(key) should be (token)
    }
  }

}
