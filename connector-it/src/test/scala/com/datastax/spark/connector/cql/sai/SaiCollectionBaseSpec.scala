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

import com.datastax.spark.connector.ccm.CcmConfig.DSE_V6_8_3
import org.apache.spark.sql.functions.{array_contains, col}
import org.apache.spark.sql.sources.EqualTo
import org.scalatest.WordSpec

import scala.collection.mutable


trait SaiCollectionBaseSpec extends SaiBaseSpec {
  this: WordSpec =>

  def indexOnANonFrozenCollection(table: String, column: String): Unit = {
    "allow for contains predicate push down" in dseFrom(DSE_V6_8_3) {
      val data = df(table).filter(array_contains(col(column), 107))
      // TODO: SPARKC-630
      assertPushDown(data)
      data.count shouldBe 1
    }

    "allow for multiple contains predicate push down" in dseFrom(DSE_V6_8_3) {
      val data = df(table).filter(array_contains(col(column), 100) and array_contains(col(column), 110))
      // TODO: SPARKC-630
      assertPushDown(data)
      data.count() shouldBe 1
    }

    "not allow for equal predicate push down" in dseFrom(DSE_V6_8_3) {
      val data = df(table).filter(col(column) === Array(100, 110))
      assertNoPushDown(data)
      data.count() shouldBe 1
    }
  }

  def indexOnAFrozenCollection(table: String, column: String): Unit = {
    "allow for equality predicate push down" in dseFrom(DSE_V6_8_3) {
      val data = df(table).filter(col(column) === Array(102, 112))
      // spark changes array to wrapped array
      assertPushedPredicate(data, pushedPredicate = EqualTo(column, mutable.WrappedArray.make(Array(102, 112))))
      data.count() shouldBe 1
    }

    "allow for equality predicate push down for pk column" in dseFrom(DSE_V6_8_3) {
      val data = df(table).filter(col("pk_1") === Array(102, 112))
      assertPushedPredicate(data, pushedPredicate = EqualTo("pk_1", mutable.WrappedArray.make(Array(102, 112))))
      data.count() shouldBe 1
    }

    "allow for only one equality predicate push down when more than one is provided" in dseFrom(DSE_V6_8_3) {
      val data = df(table).filter(col(column) === Array(102, 112) and col(column) === Array(107, 117))
      assertPushedPredicate(data,
        pushedPredicate = EqualTo(column, mutable.WrappedArray.make(Array(102, 112))))
      data.count() shouldBe 0
    }

    "not allow for contains predicate push down" in dseFrom(DSE_V6_8_3) {
      val data = df(table).filter(array_contains(col(column), 107))
      assertNoPushDown(data)
      data.count() shouldBe 1
    }
  }

}
