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


class IndexedSetSpec extends SparkCassandraITWordSpecBase with DefaultCluster with SaiCollectionBaseSpec {

  override def beforeClass {
    dseFrom(DSE_V6_8_3) {
      conn.withSessionDo { session =>
        createKeyspace(session, ks)
        session.execute(
          s"""CREATE TABLE IF NOT EXISTS $ks.set_test (
             |  pk_1 frozen<set<int>>,
             |  pk_2 int,
             |  set_col set<int>,
             |  frozen_set_col frozen<set<int>>,
             |  PRIMARY KEY ((pk_1, pk_2)));""".stripMargin)

        session.execute(
          s"""CREATE CUSTOM INDEX pk_set_test_sai_idx ON $ks.set_test (full(pk_1)) USING 'StorageAttachedIndex';""".stripMargin)

        session.execute(
          s"""CREATE CUSTOM INDEX frozen_set_test_sai_idx ON $ks.set_test (set_col) USING 'StorageAttachedIndex';""".stripMargin)

        session.execute(
          s"""CREATE CUSTOM INDEX full_set_test_sai_idx ON $ks.set_test (full(frozen_set_col)) USING 'StorageAttachedIndex';""".stripMargin)

        for (i <- (0 to 9)) {
          session.execute(s"insert into $ks.set_test " +
            s"(pk_1, pk_2, set_col, frozen_set_col) values " +
            s"({10$i, 11$i}, $i, {10$i, 11$i}, {10$i, 11$i})")
        }
      }
    }
  }

  // TODO: SPARKC-630
  "Index on a non-frozen set column" ignore {
    indexOnANonFrozenCollection("set_test", "set_col")
  }

  "Index on a frozen set column" should {
    indexOnAFrozenCollection("set_test", "frozen_set_col")
  }
}
