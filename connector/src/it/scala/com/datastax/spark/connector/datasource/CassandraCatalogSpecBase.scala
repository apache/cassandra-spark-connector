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

package com.datastax.spark.connector.datasource

import com.datastax.oss.driver.api.core.CqlIdentifier.fromInternal
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata
import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cluster.DefaultCluster
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.rdd.partitioner.CassandraPartition
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._
import org.scalatest.BeforeAndAfterAll

class CassandraCatalogSpecBase
  extends SparkCassandraITFlatSpecBase
    with DefaultCluster
    with BeforeAndAfterAll {

  override def conn: CassandraConnector = CassandraConnector(sparkConf)

  val defaultKs = "catalogtestks"
  val defaultCatalog = "cassandra"

  val EmptyInputPartition = CassandraPartition(0, Array.empty, Iterable.empty, 0)

  def getMetadata() = {
    conn.withSessionDo(_.getMetadata)
  }

  def dropKeyspace(name: String) = {
    conn.withSessionDo(_.execute(s"DROP KEYSPACE IF EXISTS $name"))
  }

  def waitForKeyspaceToExist(keyspace: String, exist: Boolean = true) = {
    eventually(getMetadata().getKeyspace(keyspace).isPresent shouldBe exist)
  }

  def getTable(keyspace: String, table: String): TableMetadata = {
    getMetadata()
      .getKeyspace(fromInternal(keyspace)).get
      .getTable(fromInternal(table)).get
  }

  def createDefaultKs(rf: Int = 5) = {
    dropKeyspace(defaultKs)
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $defaultKs WITH DBPROPERTIES (class='SimpleStrategy',replication_factor='$rf')")
    waitForKeyspaceToExist(defaultKs, true)
  }

  implicit val patienceConfig = PatienceConfig(scaled(5 seconds), scaled(200 millis))

  override def beforeClass: Unit = {
    super.beforeClass
    spark.conf.set(s"spark.sql.catalog.$defaultCatalog", classOf[CassandraCatalog].getCanonicalName)
    spark.conf.set(SQLConf.DEFAULT_CATALOG.key, "cassandra")
    // Spark 4 no longer treats spark.sql.defaultCatalog as the *current* catalog; select it explicitly.
    // Catalog plugin loading resolves against the active session's conf, so make `spark` active first.
    SparkSession.setActiveSession(spark)
    spark.sessionState.catalogManager.setCurrentCatalog(defaultCatalog)
  }



}
