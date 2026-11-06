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

package org.apache.spark.sql.cassandra

import _root_.io.teknek.arizona.spark.connector.cql.TableDef
import org.apache.spark.SparkConf
import org.apache.spark.sql.sources._
import org.scalatest.{FlatSpec, Matchers}

class DsePredicateRulesSpec extends FlatSpec with Matchers {

  val fakeTableDef = TableDef(
    "fake",
    "fake",
    Seq.empty,
    Seq.empty,
    Seq.empty
  )

  "DSE Predicate Rules" should "un-push all other predicates if solr_query exists" in {
    val solrquery: Set[Filter] = Set(EqualTo("solr_query", "*:*"))
    val otherPreds: Set[Filter] = Set(GreaterThan("x", "4"), LessThan("y", "3"))
    val preds = AnalyzedPredicates(
      solrquery ++ otherPreds,
      Set.empty)

    val results = DsePredicateRules.apply(preds, fakeTableDef, new SparkConf())
    results should be (AnalyzedPredicates(solrquery, otherPreds))
  }

  "it" should "do nothing if solr_query is not present in a query" in {
    val otherPreds: Set[Filter] = Set(GreaterThan("x", "4"), LessThan("y", "3"))
    val preds = AnalyzedPredicates(
      otherPreds,
      Set.empty)

    val results = DsePredicateRules.apply(preds, fakeTableDef, new SparkConf())
    results should be (AnalyzedPredicates(otherPreds, Set.empty))
  }
}

