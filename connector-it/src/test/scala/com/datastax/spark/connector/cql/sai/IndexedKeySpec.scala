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

import com.datastax.spark.connector.SparkCassandraITWordSpecBase
import com.datastax.spark.connector.ccm.CcmConfig.DSE_V6_8_3
import com.datastax.spark.connector.cluster.DefaultCluster
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.sources._

class IndexedKeySpec extends SparkCassandraITWordSpecBase with DefaultCluster with SaiBaseSpec {

  override def beforeClass {
    dseFrom(DSE_V6_8_3) {
      conn.withSessionDo { session =>
        createKeyspace(session, ks)
        session.execute(
          s"""CREATE TABLE IF NOT EXISTS $ks.pk_test (
             |  pk_1 int,
             |  pk_2 int,
             |  pk_3 int,
             |  ck_1 int,
             |  ck_2 int,
             |  ck_3 int,
             |  v_1 int,
             |
             |  PRIMARY KEY ((pk_1, pk_2, pk_3), ck_1, ck_2));""".stripMargin)

        session.execute(
          s"""CREATE CUSTOM INDEX pk_1_sai_idx ON $ks.pk_test (pk_1) USING 'StorageAttachedIndex';""".stripMargin)
        session.execute(
          s"""CREATE CUSTOM INDEX pk_2_sai_idx ON $ks.pk_test (pk_2) USING 'StorageAttachedIndex';""".stripMargin)
        session.execute(
          s"""CREATE CUSTOM INDEX pk_3_sai_idx ON $ks.pk_test (pk_3) USING 'StorageAttachedIndex';""".stripMargin)

        session.execute(
          s"""CREATE CUSTOM INDEX ck_2_sai_idx ON $ks.pk_test (ck_2) USING 'StorageAttachedIndex';""".stripMargin)
        session.execute(
          s"""CREATE CUSTOM INDEX v_1_sai_idx ON $ks.pk_test (v_1) USING 'StorageAttachedIndex';""".stripMargin)

        for (i <- (0 to 9)) {
          session.execute(s"insert into $ks.pk_test (pk_1, pk_2, pk_3, ck_1, ck_2, ck_3, v_1) values ($i, $i, $i, 0, 0, 0, 0)")
          session.execute(s"insert into $ks.pk_test (pk_1, pk_2, pk_3, ck_1, ck_2, ck_3, v_1) values ($i, $i, $i, 1, 1, 1, 1)")
        }

      }
    }
  }

  "Index on partition key columns" should {
    "allow for predicate push down for indexed parts of the partition key" in dseFrom(DSE_V6_8_3) {
      assertPushedPredicate(
        df("pk_test").filter(col("pk_1") === 1),
        pushedPredicate = EqualTo("pk_1", 1))
      assertPushedPredicate(
        df("pk_test").filter(col("pk_3") === 1),
        pushedPredicate = EqualTo("pk_3", 1))
      assertPushedPredicate(
        df("pk_test").filter(col("pk_1") < 1),
        pushedPredicate = LessThan("pk_1", 1))
      assertPushedPredicate(
        df("pk_test").filter(col("pk_3") > 1),
        pushedPredicate = GreaterThan("pk_3", 1))
      assertPushedPredicate(
        df("pk_test").filter(col("pk_2") >= 1),
        pushedPredicate = GreaterThanOrEqual("pk_2", 1))
    }

    "allow for multiple predicate push down for the same indexed part of the partition key" in dseFrom(DSE_V6_8_3) {
      assertPushedPredicate(
        df("pk_test").filter(col("pk_3") < 10 and col("pk_3") > 0),
        pushedPredicate = LessThan("pk_3", 10), GreaterThan("pk_3", 0))
    }

    "allow for multiple range predicate push down for different indexed parts of the partition key" in dseFrom(DSE_V6_8_3) {
      assertPushedPredicate(
        df("pk_test").filter(col("pk_3") < 10 and col("pk_1") > 0),
        pushedPredicate = LessThan("pk_3", 10), GreaterThan("pk_1", 0))
      assertPushedPredicate(
        df("pk_test").filter(col("pk_3") === 10 and col("pk_1") === 0),
        pushedPredicate = EqualTo("pk_3", 10), EqualTo("pk_1", 0))
      assertPushedPredicate(
        df("pk_test").filter(col("pk_3") === 10 and col("v_1") < 1),
        pushedPredicate = EqualTo("pk_3", 10), LessThan("v_1", 1))
    }

    "allow for range predicate push down for the partition key" in dseFrom(DSE_V6_8_3) {
      assertPushedPredicate(
        df("pk_test").filter(col("pk_3") < 10 and col("pk_1") > 0 and col("pk_2") >= 0),
        pushedPredicate = LessThan("pk_3", 10), GreaterThan("pk_1", 0), GreaterThanOrEqual("pk_2", 0))
      assertPushedPredicate(
        df("pk_test").filter(col("pk_3") === 10 and col("pk_1") < 6 and col("pk_2") === 1),
        pushedPredicate = EqualTo("pk_3", 10), LessThan("pk_1", 6), EqualTo("pk_2", 1))
    }

    "not allow for regular column predicate push down if any part of the partition key has an IN clause" in dseFrom(DSE_V6_8_3) {
      assertNonPushedColumns(
        df("pk_test").filter("pk_1 = 1 and pk_2 = 2 and pk_3 in(1, 3) and v_1 < 5"),
        nonPushedColumns = "v_1")
      assertNonPushedColumns(
        df("pk_test").filter("pk_1 = 1 and pk_2 in(1, 3) and pk_3 = 3 and v_1 < 5"),
        nonPushedColumns = "v_1")
      assertNonPushedColumns(
        df("pk_test").filter("pk_1 in (1,3) and pk_2 = 2 and pk_3 = 3 and v_1 < 5"),
        nonPushedColumns = "v_1")
    }

    "allow for regular column predicate push down if a part of the clustering key has an IN clause" in dseFrom(DSE_V6_8_3) {
      assertPushedPredicate(
        df("pk_test").filter("pk_1 = 1 and pk_2 = 2 and pk_3 = 3 and ck_1 in (1,2) and v_1 < 5"),
        pushedPredicate = EqualTo("pk_1", 1), EqualTo("pk_2", 2), EqualTo("pk_3", 3), In("ck_1", Array(1, 2)), LessThan("v_1", 5))
    }

    "not allow for push down if more than one equality predicate is defined" in dseFrom(DSE_V6_8_3) {
      val data = df("pk_test").filter(col("pk_1") === 7 and col("pk_1") === 10)
      assertPushedPredicate(data, pushedPredicate = EqualTo("pk_1", 7))
    }

    "allow only for equality push down if equality and range predicates are defined for the same pk column" in dseFrom(DSE_V6_8_3) {
      val data = df("pk_test").filter(col("pk_1") === 7 and col("pk_1") < 10)
      assertPushedPredicate(data, pushedPredicate = EqualTo("pk_1", 7))
      data.count() shouldBe 2
    }
  }

  "Index on clustering key columns" should {
    "allow for predicate push down for indexed parts of the clustering key" in dseFrom(DSE_V6_8_3) {
      assertPushedPredicate(
        df("pk_test").filter(col("ck_2") === 1),
        pushedPredicate = EqualTo("ck_2", 1))
    }

    "not allow for predicate push down for non-indexed parts of the clustering key" in dseFrom(DSE_V6_8_3) {
      assertNoPushDown(df("pk_test").filter(col("ck_3") === 1))
    }
  }
}
