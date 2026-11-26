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

package org.apache.spark.sql

import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cluster.DefaultCluster
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.rdd.CqlWhereClause
import com.datastax.spark.connector.util.CatalystUtil._

import scala.concurrent.Future

class CassandraPrunedFilteredScanSpec extends SparkCassandraITFlatSpecBase with DefaultCluster {

  override lazy val conn = CassandraConnector(defaultConf)

  val cassandraFormat = "org.apache.spark.sql.cassandra"

  override def beforeClass {
    conn.withSessionDo { session =>
      createKeyspace(session)

      awaitAll(
        Future {
          session.execute(
            s"""CREATE TABLE IF NOT EXISTS $ks.colors
                |(name TEXT, color TEXT, priority INT, PRIMARY KEY (name, priority)) """
                .stripMargin)
        },
        Future {
          session.execute(
            s"""CREATE TABLE IF NOT EXISTS $ks.fields
                |(k INT, a TEXT, b TEXT, c TEXT, d TEXT, e TEXT, PRIMARY KEY (k)) """
                .stripMargin)
        }
      )
    }
  }

  val colorOptions = Map("keyspace" -> ks, "table" -> "colors")
  val fieldsOptions = Map("keyspace" -> ks, "table" -> "fields")
  val withPushdown = Map("pushdown" -> "true")
  val withoutPushdown = Map("pushdown" -> "false")

  "CassandraPrunedFilteredScan" should "pushdown predicates for clustering keys" in {
    val colorDF = spark.read.format(cassandraFormat).options(colorOptions ++ withPushdown).load()
    val executionPlan = colorDF.filter("priority > 5").queryExecution.executedPlan
    val cts = findCassandraScan(executionPlan)
    cts.isDefined shouldBe true
    cts.get.cqlQueryParts.whereClause shouldBe CqlWhereClause(Seq(""""priority" > ?"""), List(5))
  }

  it should "not pushdown predicates for clustering keys if filterPushdown is disabled" in {
    val colorDF = spark.read.format(cassandraFormat).options(colorOptions ++ withoutPushdown).load()
    val executionPlan = colorDF.filter("priority > 5").queryExecution.executedPlan
    val cts = findCassandraScan(executionPlan)
    cts.isDefined shouldBe true
    cts.get.cqlQueryParts.whereClause shouldBe CqlWhereClause(Seq(), List())
  }

  it should "prune data columns" in {
    val fieldsDF = spark.read.format(cassandraFormat).options(fieldsOptions ++ withPushdown).load()
    val executionPlan = fieldsDF.select("b","c","d").queryExecution.executedPlan
    val cts = findCassandraScan(executionPlan)
    cts.isDefined shouldBe true
    cts.get.cqlQueryParts.selectedColumnRefs.map(_.columnName) should contain theSameElementsAs Seq("b", "c", "d")
  }

  it should "prune data columns if filterPushdown is disabled" in {
    val fieldsDF = spark.read.format(cassandraFormat).options(fieldsOptions ++ withoutPushdown).load()
    val executionPlan = fieldsDF.select("b","c","d").queryExecution.executedPlan
    val cts = findCassandraScan(executionPlan)
    cts.isDefined shouldBe true
    cts.get.cqlQueryParts.selectedColumnRefs.map(_.columnName) should contain theSameElementsAs Seq("b", "c", "d")
  }


}
