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

import _root_.io.teknek.arizona.spark.connector.cql._
import _root_.io.teknek.arizona.spark.connector.types._
import org.apache.spark.SparkConf
import _root_.org.apache.spark.sql.cassandra.SolrConstants._
import org.apache.spark.sql.sources._
import org.scalatest.{FlatSpec, Matchers}

class SolrPredicateRulesSpec extends FlatSpec with Matchers {

  val fullyIndexedTableDef = TableDef(
    "fake",
    "fake",
    Seq(ColumnDef("a", PartitionKeyColumn, IntType)),
    Seq(ColumnDef("b", ClusteringColumn(0), IntType)),
    Seq(ColumnDef("c", RegularColumn, IntType), ColumnDef("d", RegularColumn, TextType)),
    Seq(
      IndexDef(Some(DseSolrIndexClassName), "a", "some_a", Map.empty),
      IndexDef(Some(DseSolrIndexClassName), "b", "some_a", Map.empty),
      IndexDef(Some(DseSolrIndexClassName), "c", "some_a", Map.empty),
      IndexDef(Some(DseSolrIndexClassName), "d", "some_a", Map.empty)
    )
  )

  def testPredicates(
    test: AnalyzedPredicates,
    expected: AnalyzedPredicates): Unit = {

    val conf = new SparkConf().set(CassandraSourceRelation.SearchPredicateOptimizationParam.name, "on")

    val results = new SolrPredicateRules(On)
      .convertToSolrQuery(
        test,
        fullyIndexedTableDef,
        fullyIndexedTableDef.columnByName.keys.toSet,
        On,
        conf)
    results should be (expected)
  }

  "Solr Predicate Rules" should "convert easy predicates to a solr query" in {
    val test = AnalyzedPredicates(Set(LessThan("b", "3")), Set(GreaterThan("a", "4")))

    val expected = AnalyzedPredicates(
      Set(EqualTo(SolrQuery, """{"q":"*:*", "fq":["b:[* TO 3}", "a:{4 TO *]"]}""")),
      Set.empty)

    testPredicates(test, expected)
  }

  it should "convert an OR filter " in {
    val test = AnalyzedPredicates(
      Set.empty,
      Set(Or(GreaterThan("a", "4"), LessThan("b", "3"))))

    val expected = AnalyzedPredicates(
      Set(EqualTo(SolrQuery, """{"q":"*:*", "fq":["(a:{4 TO *] OR b:[* TO 3})"]}""")),
      Set.empty)

    testPredicates(test, expected)
  }

  it should "convert a large conjugated tree" in {
    val test = AnalyzedPredicates(
      Set.empty,
      Set(
        And(
          Or(GreaterThan("a", "4"), LessThan("b", "3")),
          Or(EqualTo("c", "1"),
            Not(
              Or(EqualTo("a", "1"), EqualTo("b", "1")))))))

    val expected = AnalyzedPredicates(
      Set(EqualTo(SolrQuery, """{"q":"*:*", "fq":["((a:{4 TO *] OR b:[* TO 3}) AND (c:1 OR -((a:1 OR b:1))))"]}""")),
      Set.empty)

    testPredicates(test, expected)
  }

  it should "not convert conjunctions with non solr indexed attributes" in {
    val test = AnalyzedPredicates(
      Set(Or(GreaterThan("a", "4"), LessThan("nonsolr", "3"))),
      Set.empty)

    val expected = test

    testPredicates(test, expected)
  }

  it should "be able to convert only some filters if some are non indexed" in {
     val test = AnalyzedPredicates(
       Set.empty,
       Set(GreaterThan("a", "4"), LessThan("nonsolr", "3")))

    val expected = AnalyzedPredicates(
      Set(EqualTo(SolrQuery, """{"q":"*:*", "fq":["a:{4 TO *]"]}""")),
      Set(LessThan("nonsolr", "3")))

    testPredicates(test, expected)
  }

  it should "be able to convert string like filters" in {
    val test = AnalyzedPredicates(
      Set.empty,
      Set(StringStartsWith("d", "bob"), StringEndsWith("d", "end")))

    val expected = AnalyzedPredicates(
      Set(EqualTo(SolrQuery, """{"q":"*:*", "fq":["d:bob*", "d:*end"]}""")),
      Set.empty)

    testPredicates(test, expected)
  }

  it should "be able to convert string contains filters" in {
    val test = AnalyzedPredicates(
      Set.empty,
      Set(StringContains("d", "bob")))

    val expected = AnalyzedPredicates(
      Set(EqualTo(SolrQuery, """{"q":"*:*", "fq":["d:*bob*"]}""")),
      Set.empty)

    testPredicates(test, expected)
  }

  it should "be able to convert compound NOT IN clauses" in {
    val test = AnalyzedPredicates(
      Set.empty,
      Set(Not(In("a", Seq(1,2,3,4,5).toArray))))

    val expected = AnalyzedPredicates(
      Set(EqualTo(SolrQuery, """{"q":"*:*", "fq":["-(a:(1 2 3 4 5))"]}""")),
      Set.empty)

    testPredicates(test, expected)
  }

  it should "be able to convert Null predicates" in {
    val test = AnalyzedPredicates(
      Set.empty,
      Set(EqualNullSafe("a", 5), IsNull("b"), IsNotNull("c")))

    val expected = AnalyzedPredicates(
      Set(EqualTo(SolrQuery, """{"q":"*:*", "fq":["a:5", "-b:[* TO *]", "c:*"]}""")),
      Set.empty)

    testPredicates(test, expected)
  }

  it should "be able to convert GreaterThanEquals and LessThanEquals" in {
    val test = AnalyzedPredicates(
      Set.empty,
      Set(GreaterThanOrEqual("a", 5), LessThanOrEqual("c", 5)))

    val expected = AnalyzedPredicates(
      Set(EqualTo(SolrQuery, """{"q":"*:*", "fq":["a:[5 TO *]", "c:[* TO 5]"]}""")),
      Set.empty)

    testPredicates(test, expected)
  }

}

