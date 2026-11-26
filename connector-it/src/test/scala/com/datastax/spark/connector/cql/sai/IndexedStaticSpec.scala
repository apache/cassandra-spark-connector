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

class IndexedStaticSpec extends SparkCassandraITWordSpecBase with DefaultCluster with SaiBaseSpec {

  override def beforeClass {
    dseFrom(DSE_V6_8_3) {
      conn.withSessionDo { session =>
        createKeyspace(session, ks)
        session.execute(
          s"""CREATE TABLE IF NOT EXISTS $ks.static_test (
             |  pk int,
             |  ck int,
             |  static_col_1 int STATIC,
             |  static_col_2 int STATIC,
             |
             |  PRIMARY KEY (pk, ck));""".stripMargin)

        session.execute(
          s"CREATE CUSTOM INDEX static_sai_idx ON $ks.static_test (static_col_1) USING 'StorageAttachedIndex';")

      }
    }
  }
  "Index on static columns" should {
    "allow for predicate push down" in dseFrom(DSE_V6_8_3) {
      assertPushDown(df("static_test").filter(col("static_col_1") === 1))
    }

    "not cause predicate push down for non-indexed static columns" in dseFrom(DSE_V6_8_3) {
      assertNoPushDown(df("static_test").filter(col("static_col_2") === 1))
    }
  }
}
