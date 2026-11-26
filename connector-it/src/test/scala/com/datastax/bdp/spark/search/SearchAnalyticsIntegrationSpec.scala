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

package com.datastax.bdp.spark.search

import java.lang.{Integer => JInt}

import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cluster.DseSearchAnalyticsCluster
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.datasource.{CassandraCatalog, CassandraScan}
import com.datastax.spark.connector.rdd.{CqlWhereClause}
import org.apache.spark.SparkConf
import org.apache.spark.sql.cassandra.CassandraSourceRelation.SearchPredicateOptimizationParam
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.{DataFrame, _}

import scala.concurrent.Future


class SearchAnalyticsIntegrationSpec extends SparkCassandraITFlatSpecBase with DseSearchAnalyticsCluster with SearchSupport {

  val table = "test"
  val typesTable = "types"
  val weirdTable = "weirdtable"
  val tableNoSolr = "test_nosolr"
  val tableAutoSolr = "test_autosolr"

  val (_key, _a, _b, _c) = (0, 1, 2, 3)
  val generatedRows = for (i <- 1 to 1000; a <- 1 to 5) yield {
    Row(i, a, i + a, s"words-$i-$a-${i + a}")
  }

  val conf: SparkConf = sparkConf.set(SearchPredicateOptimizationParam.name, "on")

  override lazy val spark = SparkSession.builder()
    .config(conf)
    .getOrCreate()
    .newSession()

  override lazy val conn = CassandraConnector(conf)

  override def beforeClass {
    val index: String = s"$ks.$table"
    val typeIndex: String = s"$ks.$typesTable"
    val weirdIndex: String = s"$ks.$weirdTable"

    dseOnly {
      conn.withSessionDo { session =>

        val executor = getExecutor(session)
        session.execute(
          s"""CREATE KEYSPACE IF NOT EXISTS $ks
             |WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }
            """
          .stripMargin)
      awaitAll(
        Future(
          session.execute(
            s"""CREATE TABLE IF NOT EXISTS $ks.$table (key int, a int, b int, c text, PRIMARY KEY (key, a))"""
          )),
        Future {
          session.execute(s"""CREATE TABLE IF NOT EXISTS $ks.$weirdTable (key int, "~~funcolumn" text, "MixEdCol" text, PRIMARY KEY (key))""")
          session.execute(s"""INSERT INTO $ks.$weirdTable (key, "~~funcolumn", "MixEdCol") VALUES (1, 'hello', 'world')""")
        },
        Future {
          session.execute(
            s"""CREATE TABLE IF NOT EXISTS $ks.$typesTable (
              key int,
              a ascii ,
              b bigint ,
              c blob ,
              d date ,
              e decimal ,
              f double ,
              g inet ,
              h int,
              i text ,
              j timestamp ,
              k varchar ,
              l varint,
              m uuid,
              PRIMARY KEY (key))""")
          session.execute(
            s"""INSERT INTO $ks.$typesTable
               |(key, a , b , c, d, e, f, g, h, i, j, k, l, m) VALUES
               |(1, 'one:\".', 1, 0xFF, '2017-2-7', 1.5, 2.0, '8.8.8.8' , 1, 'one', 1, 'one', 1, 10000000-0000-0000-0000-000000000000)"""
              .stripMargin)
          session.execute(
            s"""INSERT INTO $ks.$typesTable
               |(key, a , i) VALUES
               |(2, 'OR', 'OR')"""
              .stripMargin)
        }
      )

      awaitAll(
        Future(createCore(session, 1, index)),
        Future(createCore(session, 1, typeIndex)),
        Future(createCore(session, 1, weirdIndex, true, false, true, true))
      )
      val ps = session.prepare(s"INSERT INTO $ks.$table (key, a, b, c) VALUES (?,?,?,?)")
      val groupedRows = generatedRows.grouped(100)
      awaitAll(
        for (group <- groupedRows; row <- group) yield {
          executor.executeAsync(ps.bind(row.getInt(0): JInt, row.getInt(1): JInt, row.getInt(2): JInt, row.getString(3)))
        }
      )

      executor.waitForCurrentlyExecutingTasks()
    }

    registerTableWithOptions(typesTable, tableName = typesTable)
    registerTableWithOptions(table)
    registerTableWithOptions(tableNoSolr, Map(SearchPredicateOptimizationParam.name -> "off"))
    registerTableWithOptions(tableAutoSolr, Map(SearchPredicateOptimizationParam.name -> "auto"), table)

    awaitAll(
      Future(getSolrClient(cluster.addresses.head.getAddress, index).commit(true, true, true)),
      Future(getSolrClient(cluster.addresses.head.getAddress, typeIndex).commit(true, true, true)),
      Future(getSolrClient(cluster.addresses.head.getAddress, weirdIndex).commit(true, true, true))
    )
    }
  }

  def registerTableWithOptions(
    registerName: String,
    options: Map[String, String] = Map.empty,
    tableName: String = table): Unit = {

    spark.conf.set(s"spark.sql.catalog.$registerName", classOf[CassandraCatalog].getCanonicalName)
    options.foreach { case (key, value) => spark.conf.set(s"spark.sql.catalog.$registerName.$key", value) }

  }

  implicit class DFFunctions(df: DataFrame) {
    /**
      * Attempt to walk back dependency tree for the RDD and find
      * the CassandraTableScanRDD. This should only work correctly if
      * we are in a series of 1 to 1 mappings back to the source.
      */
    def getUnderlyingCqlWhereClause(): CqlWhereClause = {
      getCassandraScan(df.queryExecution.sparkPlan).cqlQueryParts.whereClause
    }
  }


  "SearchAnalytics" should "be able to use solr_query" in dseOnly {
    val df = spark.sql(s"""SELECT key,a,b,c FROM $table.$ks.$table WHERE solr_query = '{"q": "*:*", "fq":["key:[500 TO *]", "a:1"]}' """)

    //Check that the correct data got through
    val expected = generatedRows
      .filter(_.getInt(_key) >= 500)
      .filter(_.getInt(_a) == 1)

    val results = df.collect
    expected.toSet -- results.toSet shouldBe empty
    results should contain theSameElementsAs (expected)

    results.size should be(expected.size)
  }

  it should "be able to use solr optimizations " in dseOnly {
    val df = spark.sql(s"SELECT key,a,b,c FROM $table.$ks.$table WHERE key >= 500 AND a == 1")

    //Check that pushdown happened
    val whereClause = df.getUnderlyingCqlWhereClause()
    whereClause.predicates.head should include("solr_query")
    val solrClause = whereClause.values.head.toString
    solrClause should include("""key:[500 TO *]""")
    solrClause should include("""a:1""")

    //Check that the correct data got through
    val expected = generatedRows
      .filter(_.getInt(_key) >= 500)
      .filter(_.getInt(_a) == 1)

    val results = df.collect
    expected.toSet -- results.toSet shouldBe empty
    results should contain theSameElementsAs (expected)

    results.size should be(expected.size)
  }

  it should "not use solr optimizations if there is a manual optimization" in dseOnly {
    val df = spark.sql(s"SELECT key,a,b,c FROM $table.$ks.$table WHERE solr_query='key:[500 TO *]' AND a == 1")
    val whereClause = df.getUnderlyingCqlWhereClause()
    whereClause.predicates.head should include("solr_query")
    val solrClause = whereClause.values.head.toString
    solrClause should include("""key:[500 TO *]""")
    solrClause should not include ("a")

    //Check that the correct data got through
    val expected = generatedRows
      .filter(_.getInt(_key) >= 500)
      .filter(_.getInt(_a) == 1)

    val results = df.collect
    expected.toSet -- results.toSet shouldBe empty
    results should contain theSameElementsAs (expected)

    results.size should be(expected.size)

  }

  it should "should choose solr clauses over clustering key clauses " in dseOnly {
    val df = spark.sql(s"SELECT key,a,b,c FROM $table.$ks.$table WHERE a > 2")

    //Check that pushdown happened
    val whereClause = df.getUnderlyingCqlWhereClause()
    whereClause.predicates.head should include("solr_query")
    val solrClause = whereClause.values.head.toString
    solrClause should include("""a:{2 TO *]""")

    //Check that the correct data got through
    val expected = generatedRows
      .filter(_.getInt(_a) > 2)

    val results = df.collect
    results should contain theSameElementsAs (expected)

    results.size should be(expected.size)
  }

  it should "should be able to turn off solr optimization " in dseOnly {
    val df = spark.sql(s"SELECT key,a,b,c FROM $tableNoSolr.$ks.$table WHERE a > 2")

    //Check that solr pushdown didn't happened
    val whereClause = df.getUnderlyingCqlWhereClause()
    whereClause.predicates.head should not include ("solr_query")

    val results = df.collect
    results.size should be(3000)
  }

  it should "should correctly do negation filters " in dseOnly {
    val df = spark.sql(
      s"""SELECT key,a,b,c FROM $table.$ks.$table WHERE
         |key > 998 AND
         |c NOT LIKE '%999%'""".stripMargin)

    //Check that pushdown happened
    val whereClause = df.getUnderlyingCqlWhereClause()
    whereClause.predicates.head should include("solr_query")
    val solrClause = whereClause.values.head.toString
    solrClause should include(s"""key:{998 TO *]""")
    solrClause should include(s"""-(c:*999*)""")

    //Check that the correct data got through
    val expected = generatedRows
      .filter(_.getInt(_key) > 998)
      .filter(row => !(row.getString(_c).contains(s"999")))

    val results = df.collect
    expected.toSet -- results.toSet shouldBe empty
    results should contain theSameElementsAs (expected)
  }

  it should "handle OR conjunctions when possible" in dseOnly {
    val df = spark.sql(
      s"""SELECT key,a,b,c FROM $table.$ks.$table
         |WHERE key < 10
         |OR key > 990""".stripMargin)

    //Check that pushdown happened
    val whereClause = df.getUnderlyingCqlWhereClause()
    whereClause.predicates.head should include("solr_query")
    val solrClause = whereClause.values.head.toString
    solrClause should include(s"""key:{990 TO *]""")
    solrClause should include(s"""key:[* TO 10}""")

    //Check that the correct data got through
    val expected = generatedRows
      .filter { row =>
        val key = row.getInt(_key)
        key < 10 || key > 990
      }

    val results = df.collect
    expected.toSet -- results.toSet shouldBe empty
    results should contain theSameElementsAs (expected)
  }

  it should "handle conjunctions with LIKE clauses" in dseOnly {
    val df = spark.sql(
      s"""SELECT key,a,b,c FROM $table.$ks.$table
         |WHERE c LIKE '%100%'
         |AND c LIKE '%101'
         |AND c LIKE '%1%'""".stripMargin)

    //Check that pushdown happened
    val whereClause = df.getUnderlyingCqlWhereClause()
    whereClause.predicates.head should include("solr_query")
    val solrClause = whereClause.values.head.toString
    solrClause should include(s"""c:*100*""")
    solrClause should include(s"""c:*101""")
    solrClause should include(s"""c:*1*""")

    //Check that the correct data got through
    val expected = generatedRows
      .filter { row =>
        val c = row.getString(_c)
        c.contains("100") && c.endsWith("101") & c.contains("1")
      }

    val results = df.collect
    expected.toSet -- results.toSet shouldBe empty
    results should contain theSameElementsAs (expected)
  }

  it should "handle IN clauses" in dseOnly {
    val df = spark.sql(
      s"""SELECT key,a,b,c FROM $table.$ks.$table
         |WHERE key IN (4, 8, 15, 16, 23, 42)""".stripMargin)

    //Check that pushdown happened
    val whereClause = df.getUnderlyingCqlWhereClause()
    whereClause.predicates.head should include("solr_query")
    val solrClause = whereClause.values.head.toString
    solrClause should include(s"""key:(4 8 15 16 23 42)""")

    //Check that the correct data got through
    val lostNum = Seq(4, 8, 15, 16, 23, 42)
    val expected = generatedRows.filter { row => lostNum.contains(row.getInt(_key)) }

    val results = df.collect
    expected.toSet -- results.toSet shouldBe empty
    results should contain theSameElementsAs (expected)
  }

  it should "handle IsNotNull on a partition key" in dseOnly {
    val df = spark.sql(
      s"""SELECT key,a,b,c FROM $table.$ks.$table WHERE key IS NOT NULL""")

    //Check that pushdown happened
    val whereClause = df.getUnderlyingCqlWhereClause()
    whereClause.predicates.headOption should not contain (""""solr_query" = ?""")

    //Check that the correct data got through
    val expected = generatedRows.filter { row => row.getInt(_key) != null }

    val results = df.collect
    expected.toSet -- results.toSet shouldBe empty
    results should contain theSameElementsAs (expected)
  }

  it should "handle mixed partition key and solr restrictions" in dseOnly {
    val df = spark.sql(
      s"""SELECT key,a,b,c FROM $table.$ks.$table WHERE key = 4 AND b = 5""")

    //Check that pushdown happened
    val whereClause = df.getUnderlyingCqlWhereClause()
    whereClause.predicates should contain theSameElementsAs (Seq(""""solr_query" = ?""", """"key" = ?"""))
    whereClause.values should contain theSameElementsAs (Seq("""{"q":"*:*", "fq":["b:5"]}""", 4))

    //Check that the correct data got through
    val expected = generatedRows.filter { row => row.getInt(_key) == 4 && row.getInt(_b) == 5 }

    val results = df.collect
    expected.toSet -- results.toSet shouldBe empty
    results should contain theSameElementsAs (expected)
  }

  it should "handle isNotNull all by itself" in dseOnly {
    val df = spark.sql(
      s"""SELECT key,a,b,c FROM $table.$ks.$table WHERE c IS NOT NULL""")

    //Check that the correct data got through
    val expected = generatedRows.filter { row => row.getString(_c) != null }
    val results = df.collect
    expected.toSet -- results.toSet shouldBe empty
    results should contain theSameElementsAs (expected)
    df.collect()
  }

  it should "handle only partition key restrictions" in dseOnly {
    val df = spark.sql(
      s"""SELECT key,a,b,c FROM $table.$ks.$table WHERE key = 4""")

    //Check that pushdown happened
    val whereClause = df.getUnderlyingCqlWhereClause()
    whereClause.predicates.headOption should contain(""""key" = ?""")
    whereClause.values should contain theSameElementsAs (Seq(4))

    //Check that the correct data got through
    val expected = generatedRows.filter { row => row.getInt(_key) == 4 }

    val results = df.collect
    expected.toSet -- results.toSet shouldBe empty
    results should contain theSameElementsAs (expected)
  }

  it should "handle primary key restrictions" in dseOnly {
    val df = spark.sql(
      s"""SELECT key,a,b,c FROM $table.$ks.$table WHERE key = 4 AND a > 2""")

    //Check that pushdown happened
    val whereClause = df.getUnderlyingCqlWhereClause()
    val predicates = whereClause.predicates.mkString(" AND ")
    predicates should include(""""key" = ?""")
    predicates should include(""""a" > ?""")
    whereClause.values should contain theSameElementsAs (Seq(4, 2))

    //Check that the correct data got through
    val expected = generatedRows.filter { row => row.getInt(_key) == 4 && row.getInt(_a) > 2 }
    val results = df.collect
    expected.toSet -- results.toSet shouldBe empty
    results should contain theSameElementsAs (expected)
  }

  it should "handle Primary key restrictions and solr queries at the same time" in dseOnly {
    val df = spark.sql(
      s"""SELECT key,a,b,c FROM $table.$ks.$table WHERE key = 4 AND a > 2 AND b < 25""")
    //Check that pushdown happened
    val whereClause = df.getUnderlyingCqlWhereClause()
    val predicates = whereClause.predicates.mkString(" AND ")
    predicates should include(""""solr_query" = ?""")
    predicates should include(""""key" = ?""")
    whereClause.values should contain theSameElementsAs
      Seq("""{"q":"*:*", "fq":["a:{2 TO *]", "b:[* TO 25}"]}""", 4)

    //Check that the correct data got through
    val expected = generatedRows.filter { row => row.getInt(_key) == 4 && row.getInt(_a) > 2 && row.getInt(_b) < 25 }
    val results = df.collect
    expected.toSet -- results.toSet shouldBe empty
    results should contain theSameElementsAs (expected)
  }

  it should "optimize a count(*) without any predicates in " in dseOnly {
    val df = spark.sql(s"SELECT COUNT(*) from $table.$ks.$table")
    val whereClause = df.getUnderlyingCqlWhereClause()
    val predicates = whereClause.predicates.head
    predicates should include("""solr_query = '*:*'""")
    val results = df.collect()
    results should contain(Row(5000))
  }

  def testType(queryPredicate: String, expectedPredicate: String) = {
    val df = spark.sql(s"""SELECT * FROM $typesTable.$ks.$typesTable WHERE $queryPredicate""")
    val whereClause = df.getUnderlyingCqlWhereClause()
    whereClause.predicates.headOption should contain(""""solr_query" = ?""")
    val generatedWhereQuery: String = whereClause.values.head.toString
    generatedWhereQuery should include(expectedPredicate)
    df.collect() should have size 1
  }

  it should "work with ascii" in dseOnly {
    testType("""a = 'one:\".'""", "a:one")
  }

  it should "work with solr reserved words in ascii" in dseOnly {
    testType("a = 'OR'", "a:\\\\OR")
  }

  it should "work with bigint" in dseOnly {
    testType("b = 1", "b:1")
  }

  it should "work with date" in dseOnly {
    testType("d = cast('2017-2-7' as date)", """d:2017\\-02\\-07""")
  }

  it should "work with decimal" in dseOnly {
    testType("e > 1.0", "1.000000000000000000 TO *")
  }

  it should "work with double" in dseOnly {
    testType("f = 2.0", "f:2.0")
  }

  it should "work with inet" in dseOnly {
    testType("g = '8.8.8.8'", "g:8.8.8.8")
  }

  it should "work with int" in dseOnly {
    testType("h = 1", "h:1")
  }

  it should "work with text" in dseOnly {
    testType("i = 'one'", "i:one")
  }

  it should "work with solr reserved words in text" in dseOnly {
    testType("i = 'OR'", "i:\\\\OR")
  }

  it should "work with timestamp" in dseOnly {
    testType("j < cast( \"2000-01-01T00:08:20.000Z\" as timestamp)", """{"q":"*:*", "fq":["j:[* TO 2000\\-01\\-01T00\\:08\\:20Z}"]}""")
  }

  it should "work with varchar" in dseOnly {
    testType("k = 'one'", "k:one")
  }

  it should "work with varint" in dseOnly {
    testType("l = 1", "l:1")
  }

  it should "work with uuid" in dseOnly {
    testType("m = '10000000-0000-0000-0000-000000000000'", """"m:10000000\\-0000\\-0000\\-0000\\-000000000000"""")
  }

  it should "work with weird column names" in dseOnly {
    val df: DataFrame = spark
      .read
      .cassandraFormat(weirdTable, ks)
      .load()

    val filtered = df.filter(df("key") === 1)
      .filter(df("~~funcolumn") === "hello")
      .filter(df("MixEdCol") === "world")

    val whereClause = filtered.getUnderlyingCqlWhereClause()
    val predicates = whereClause.predicates.head
    val values: String = whereClause.values.head.toString
    predicates should include("""solr_query""")
    values should include("funcolumn")
    values should include("MixEdCol")
    val rows = filtered.collect
    rows.head.getAs[String]("~~funcolumn") should be("hello")
    rows.head.getAs[String]("MixEdCol") should be("world")
  }

  "Automatic Solr Optimization" should "occur when the selected amount of data is less than the threshold" in dseOnly {
    val df = spark.sql(s"SELECT COUNT(*) from $tableAutoSolr.$ks.$table where key < 5")
    val whereClause = df.getUnderlyingCqlWhereClause()
    val predicates = whereClause.predicates.head
    predicates should include("""solr_query""")
  }

  it should "not occur if the selected amount of data is greater than the threshold" in dseOnly {
    val df = spark.sql(s"SELECT COUNT(*) from $tableAutoSolr.$ks.$table where key < 500")
    val whereClause = df.getUnderlyingCqlWhereClause()
    val predicates = whereClause.predicates
    predicates shouldBe empty
  }
}
