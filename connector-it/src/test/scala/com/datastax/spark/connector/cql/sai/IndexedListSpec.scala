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


class IndexedListSpec extends SparkCassandraITWordSpecBase with DefaultCluster with SaiCollectionBaseSpec {

  override def beforeClass {
    dseFrom(DSE_V6_8_3) {
      conn.withSessionDo { session =>
        createKeyspace(session, ks)
        session.execute(
          s"""CREATE TABLE IF NOT EXISTS $ks.list_test (
             |  pk_1 frozen<list<int>>,
             |  pk_2 int,
             |  list_col list<int>,
             |  frozen_list_col frozen<list<int>>,
             |  PRIMARY KEY ((pk_1, pk_2)));""".stripMargin)

        session.execute(
          s"""CREATE CUSTOM INDEX pk_list_test_sai_idx ON $ks.list_test (full(pk_1)) USING 'StorageAttachedIndex';""".stripMargin)

        session.execute(
          s"""CREATE CUSTOM INDEX frozen_list_test_sai_idx ON $ks.list_test (list_col) USING 'StorageAttachedIndex';""".stripMargin)

        session.execute(
          s"""CREATE CUSTOM INDEX full_list_test_sai_idx ON $ks.list_test (full(frozen_list_col)) USING 'StorageAttachedIndex';""".stripMargin)

        for (i <- (0 to 9)) {
          session.execute(s"insert into $ks.list_test " +
            s"(pk_1, pk_2, list_col, frozen_list_col) values " +
            s"([10$i, 11$i], $i, [10$i, 11$i], [10$i, 11$i])")
        }
      }
    }

  }

  // TODO: SPARKC-630
  "Index on a non-frozen list column" ignore {
    indexOnANonFrozenCollection("list_test", "list_col")
  }

  "Index on a frozen list column" should {
    indexOnAFrozenCollection("list_test", "frozen_list_col")
  }

}
