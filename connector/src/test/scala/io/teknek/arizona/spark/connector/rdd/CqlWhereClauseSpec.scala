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

package io.teknek.arizona.spark.connector.rdd

import org.scalatest.{FlatSpec, Matchers}

class CqlWhereClauseSpec extends FlatSpec with Matchers {

  it should "produce a string for each predicate" in {
    val clause = CqlWhereClause(Seq("x < ?", "y = ?"), Seq(1, "aaa"))

    clause.toString shouldBe "[[x < ?, 1],[y = ?, aaa]]"
  }

  it should "produce empty predicate string for empty predicate list" in {
    val clause = CqlWhereClause(Seq(), Seq())

    clause.toString shouldBe "[]"
  }

  it should "produce valid string for IN clause predicate" in {
    val clause = CqlWhereClause(Seq("x < ?", "z IN (?, ?)", "y IN (?, ?, ?)", "a = ?"), Seq(1, 2, 3, 4, 5, 6, 7))

    clause.toString shouldBe "[[x < ?, 1],[z IN (?, ?), (2, 3)],[y IN (?, ?, ?), (4, 5, 6)],[a = ?, 7]]"
  }

  it should "complain when the number of values doesn't match the number of placeholders '?'" in {
    intercept[AssertionError] {
      CqlWhereClause(Seq("x < ?"), Seq())
    }
  }
}
