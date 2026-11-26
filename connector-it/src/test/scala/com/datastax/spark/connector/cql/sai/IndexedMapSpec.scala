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
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sources.EqualTo

class IndexedMapSpec extends SparkCassandraITWordSpecBase with DefaultCluster with SaiBaseSpec {

  override def beforeClass {
    dseFrom(DSE_V6_8_3) {
      conn.withSessionDo { session =>
        createKeyspace(session)
        session.execute(
          s"""CREATE TABLE IF NOT EXISTS $ks.map_test (
             |  pk int,
             |
             |  map_col_1 map<int,int>,
             |  map_col_2 map<int,int>,
             |  map_col_3 map<int,int>,
             |  map_col_4 frozen<map<int,int>>,
             |
             |  PRIMARY KEY (pk));""".stripMargin)

        session.execute(
          s"""CREATE CUSTOM INDEX keys_map_sai_idx ON $ks.map_test (keys(map_col_1))
             |USING 'StorageAttachedIndex';""".stripMargin)
        session.execute(
          s"""CREATE CUSTOM INDEX values_map_sai_idx ON $ks.map_test (values(map_col_2))
             |USING 'StorageAttachedIndex';""".stripMargin)
        session.execute(
          s"""CREATE CUSTOM INDEX entries_map_sai_idx ON $ks.map_test (entries(map_col_3))
             |USING 'StorageAttachedIndex';""".stripMargin)
        session.execute(
          s"""CREATE CUSTOM INDEX full_map_sai_idx ON $ks.map_test (full(map_col_4))
             |USING 'StorageAttachedIndex';""".stripMargin)
      }
    }
  }

  // TODO: SPARKC-630
  "Index on map keys" ignore {
    "allow for contains predicate push down on keys" in dseFrom(DSE_V6_8_3) {
      // SELECT * from test_storage_attached_index_spec.map_test WHERE map_col_1 CONTAINS KEY 2;
      assertPushDown(df("map_test").filter(array_contains(map_keys(col("map_col_1")), 2)))
    }

    "not allow for contains predicate push down on values" in dseFrom(DSE_V6_8_3) {
      assertNoPushDown(df("map_test").filter(array_contains(map_values(col("map_col_1")), 2)))
    }

    "not allow for equality predicate push down" in dseFrom(DSE_V6_8_3) {
      assertNoPushDown(df("map_test").filter(col("map_col_1").getItem(5) === 4))
    }
  }

  // TODO: SPARKC-630
  "Index on map values" ignore {
    "allow for contains predicate push down on values" in dseFrom(DSE_V6_8_3) {
      // SELECT * from test_storage_attached_index_spec.map_test WHERE map_col_2 CONTAINS 2;
      assertPushDown(df("map_test").filter(array_contains(map_values(col("map_col_2")), 2)))
    }

    "not allow for contains predicate push down on keys" in dseFrom(DSE_V6_8_3) {
      assertNoPushDown(df("map_test").filter(array_contains(map_keys(col("map_col_2")), 2)))
    }

    "not allow for equality predicate push down" in dseFrom(DSE_V6_8_3) {
      assertNoPushDown(df("map_test").filter(col("map_col_2").getItem(5) === 4))
    }
  }

  // TODO: SPARKC-630
  "Index on map entries" ignore {
    "allow for equality predicate push down" in dseFrom(DSE_V6_8_3) {
      // SELECT * from test_storage_attached_index_spec.map_test WHERE map_col_3[5] = 4;
      assertPushDown(df("map_test").filter(col("map_col_3").getItem(5) === 4))
    }

    "not allow for predicate push down different than equality" in dseFrom(DSE_V6_8_3) {
      assertNoPushDown(df("map_test").filter(col("map_col_3").getItem(5) > 3))
      assertNoPushDown(df("map_test").filter(col("map_col_3").getItem(5) >= 3))
    }

    "not allow for contains predicate push down on keys" in dseFrom(DSE_V6_8_3) {
      assertNoPushDown(df("map_test").filter(array_contains(map_keys(col("map_col_3")), 2)))
    }

    "not allow for contains predicate push down on values" in dseFrom(DSE_V6_8_3) {
      assertNoPushDown(df("map_test").filter(array_contains(map_values(col("map_col_3")), 2)))
    }
  }

  // TODO: SPARKC-630: Unsupported literal type class scala.collection.immutable.Map$Map1 Map(102 -> 112)
  "Index on full map" ignore {
    "allow for equality predicate push down" in dseFrom(DSE_V6_8_3) {
      val data = df("map_test").filter(col("map_col_4") === Map(102 -> 112))
      // spark changes array to wrapped array
      assertPushedPredicate(data, pushedPredicate = EqualTo("map_col_4", Map(102 -> 112)))
    }

    "allow for only one equality predicate push down when more than one is provided" in dseFrom(DSE_V6_8_3) {
      val data = df("map_test").filter(col("map_col_4") === Map(102 -> 112) and col("map_col_4") === Map(107 -> 117))
      assertPushedPredicate(data, pushedPredicate = EqualTo("map_col_4", Map(102 -> 112)))
    }
  }
}
