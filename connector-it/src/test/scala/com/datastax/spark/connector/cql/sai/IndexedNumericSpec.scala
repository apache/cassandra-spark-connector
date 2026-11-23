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

package com.datastax.spark.connector.cql.sai

import java.sql.{Date, Timestamp}
import java.time.{LocalDate, LocalTime}

import com.datastax.spark.connector.SparkCassandraITWordSpecBase
import com.datastax.spark.connector.ccm.CcmConfig.DSE_V6_8_3
import com.datastax.spark.connector.cluster.DefaultCluster
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.sources._


class IndexedNumericSpec extends SparkCassandraITWordSpecBase with DefaultCluster with SaiBaseSpec {

  private val numericTypes = Seq("int", "date", "inet", "time", "timestamp", "timeuuid") // and others

  override def beforeClass {
    dseFrom(DSE_V6_8_3) {
      conn.withSessionDo { session =>

        createKeyspace(session)
        createTableWithIndexes(session, "numeric_types_test", numericTypes)

        for (i <- 0 to 9) {
          session.execute(
            s"""insert into $ks.numeric_types_test
               | (pk, int_col, date_col, inet_col, time_col, timestamp_col, timeuuid_col)
               | values ($i, $i, '2020-12-1$i', '127.0.0.1$i', '13:57:1$i', 129903870000$i, 50554d6e-29bb-11e5-b345-feff819cdc9$i)""".stripMargin)
        }
      }
    }
  }

  "Index on a numeric column" should {

    "allow for equality predicate push down" in dseFrom(DSE_V6_8_3) {
      val data = df("numeric_types_test").filter(col("int_col") === 7)
      assertPushedPredicate(data, pushedPredicate = EqualTo("int_col", 7))
      data.count() shouldBe 1
    }

    "allow for gte predicate push down" in dseFrom(DSE_V6_8_3) {
      val data = df("numeric_types_test").filter(col("int_col") >= 7)
      assertPushedPredicate(data, pushedPredicate = GreaterThanOrEqual("int_col", 7))
      data.count() shouldBe 3
    }

    "allow for gt predicate push down" in dseFrom(DSE_V6_8_3) {
      val data = df("numeric_types_test").filter(col("int_col") > 7)
      assertPushedPredicate(data, pushedPredicate = GreaterThan("int_col", 7))
      data.count() shouldBe 2
    }

    "allow for lt predicate push down" in dseFrom(DSE_V6_8_3) {
      val data = df("numeric_types_test").filter(col("int_col") < 7)
      assertPushedPredicate(data, pushedPredicate = LessThan("int_col", 7))
      data.count() shouldBe 7
    }

    "allow for lte predicate push down" in dseFrom(DSE_V6_8_3) {
      val data = df("numeric_types_test").filter(col("int_col") <= 7)
      assertPushedPredicate(data, pushedPredicate = LessThanOrEqual("int_col", 7))
      data.count() shouldBe 8
    }

    "allow for more than one range predicate push down" in dseFrom(DSE_V6_8_3) {
      val data = df("numeric_types_test").filter(col("int_col") >= 7 and col("int_col") < 9)
      assertPushedPredicate(
        data,
        pushedPredicate = GreaterThanOrEqual("int_col", 7), LessThan("int_col", 9))
      data.count() shouldBe 2
    }

    "allow only for equality push down if range and equality predicates are defined" in dseFrom(DSE_V6_8_3) {
      val data = df("numeric_types_test").filter(col("int_col") === 2 and col("int_col") < 4)

      assertPushedPredicate(data, pushedPredicate = EqualTo("int_col", 2))
      data.count() shouldBe 1
    }

    "allow only for a single equality push down if there are more than one" in dseFrom(DSE_V6_8_3) {
      val data = df("numeric_types_test").filter(col("int_col") === 5 and col("int_col") === 8)
      assertPushedPredicate(data, pushedPredicate = EqualTo("int_col", 5))
      data.count() shouldBe 0
    }

    "not allow for a predicate push down if OR clause is used" in dseFrom(DSE_V6_8_3) {
      assertNoPushDown(df("numeric_types_test").filter(col("int_col") >= 7 or col("int_col") === 4))
    }
  }

  /** Minimal check to verify if non-obvious numeric types are supported */
  private def indexOnNumericColumn(columnName: String, value: Any): Unit = {
    "allow for equality predicate push down" in dseFrom(DSE_V6_8_3) {
      val data = df("numeric_types_test").filter(col(columnName) === value)
      assertPushedPredicate(data, pushedPredicate = EqualTo(columnName, value))
      data.count() shouldBe 1
    }

    "allow for gte predicate push down" in dseFrom(DSE_V6_8_3) {
      val data = df("numeric_types_test").filter(col(columnName) >= value)
      assertPushedPredicate(data, pushedPredicate = GreaterThanOrEqual(columnName, value))
      data.count() shouldBe 3
    }
  }

  "Index on a date column" should {
    indexOnNumericColumn("date_col", Date.valueOf(LocalDate.of(2020, 12, 17)))
  }

  "Index on a inet column" should {
    indexOnNumericColumn("inet_col", "127.0.0.17")
  }

  "Index on a time column" should {
    indexOnNumericColumn("time_col", LocalTime.of(13,57,17).toNanoOfDay)
  }

  "Index on a timestamp column" should {
    indexOnNumericColumn("timestamp_col",  new Timestamp(1299038700007L))
  }

  "Index on a timeuuid column" should {
    indexOnNumericColumn("timeuuid_col", "50554d6e-29bb-11e5-b345-feff819cdc97")
  }

}
