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

import org.apache.spark.sql.functions._

class CassandraCatalogTableWriteSpec extends CassandraCatalogSpecBase {

  val testTable = "testTable"

  def setupBasicTable(): Unit = {
    createDefaultKs(1)
    spark.sql(s"CREATE TABLE $defaultKs.$testTable (key Int, value STRING) USING cassandra PARTITIONED BY (key)")
    val ps = conn.withSessionDo(_.prepare(s"""INSERT INTO $defaultKs."$testTable" (key, value) VALUES (?, ?)"""))
    awaitAll {
      for (i <- 0 to 100) yield {
        executor.executeAsync(ps.bind(i : java.lang.Integer, i.toString))
      }
    }
  }

  "A Cassandra Catalog Table Write Support" should "initialize successfully" in {
    spark.sessionState.catalogManager.currentCatalog.name() should be(defaultCatalog)
  }

  it should "support CTAS" in {
    val outputTable = "output_empty"
    setupBasicTable()
    spark.sql(
      s"""CREATE TABLE $defaultKs.$outputTable USING cassandra PARTITIONED BY (key)
         |AS SELECT * FROM $defaultKs.$testTable""".stripMargin)
    val results = spark.sql(s"""SELECT * FROM $defaultKs.$outputTable""").collect()
    results.length shouldBe (101)
  }

  it should "support a vanilla insert" in {
    createDefaultKs(1)
    spark.sql(
      s"""CREATE TABLE $defaultKs.vanilla (key Int, value String) USING cassandra PARTITIONED BY (key)""".stripMargin)

    spark.range(0, 100)
      .withColumnRenamed("id", "key")
      .withColumn("value", col("key").cast("string"))
      .writeTo(s"$defaultKs.vanilla")
      .append()
    val results = spark.sql(s"""SELECT * FROM $defaultKs.vanilla""").collect()
    results.length shouldBe (100)
  }

  it should "report missing primary key columns" in intercept[CassandraCatalogException]{
    createDefaultKs(1)
    spark.sql(
      s"""CREATE TABLE $defaultKs.vanilla (key Int, value String) USING cassandra PARTITIONED BY (key)""".stripMargin)

    spark.range(0, 100)
      .withColumn("value", col("id").cast("string"))
      .writeTo(s"$defaultKs.vanilla")
      .append()
  }

}
