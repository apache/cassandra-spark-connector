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

package com.datastax.spark.connector.sql

import com.datastax.spark.connector.cluster.DefaultCluster
import com.datastax.spark.connector.cql.{CassandraConnector, TableDef}
import com.datastax.spark.connector.datasource.CassandraInJoin
import com.datastax.spark.connector.{SparkCassandraITFlatSpecBase, _}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.cassandra.CassandraSourceRelation.InClauseToJoinWithTableConversionThreshold
import org.apache.spark.sql.cassandra.{AnalyzedPredicates, CassandraPredicateRules, CassandraSourceRelation}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.sources.{EqualTo, Filter}
import org.scalatest.BeforeAndAfterEach

import scala.concurrent.Future

//noinspection ScalaStyle
class CassandraDataSourceSpec extends SparkCassandraITFlatSpecBase with DefaultCluster with BeforeAndAfterEach {

  override lazy val conn = CassandraConnector(defaultConf)

  def pushDown: Boolean = true

  override def beforeClass {
    conn.withSessionDo { session =>
      createKeyspace(session)

      awaitAll(
        Future {
          session.execute(s"""CREATE TABLE $ks.test1 (a INT, b INT, c INT, d INT, e INT, f INT, g INT, h INT, PRIMARY KEY ((a, b, c), d , e, f))""")
          session.execute(s"""CREATE MATERIALIZED VIEW $ks.test1_by_g as select g, a, b, c, d, e, f, h from $ks.test1  where g is not null and a is not null and b is not null and c is not null and d is not null and e is not null and f is not null PRIMARY KEY (g, a, b, c, d, e, f)""")
          session.execute(s"""INSERT INTO $ks.test1 (a, b, c, d, e, f, g, h) VALUES (1, 1, 1, 1, 1, 1, 1, 1)""")
          session.execute(s"""INSERT INTO $ks.test1 (a, b, c, d, e, f, g, h) VALUES (1, 1, 1, 1, 2, 1, 1, 2)""")
          session.execute(s"""INSERT INTO $ks.test1 (a, b, c, d, e, f, g, h) VALUES (1, 1, 1, 2, 1, 1, 2, 1)""")
          session.execute(s"""INSERT INTO $ks.test1 (a, b, c, d, e, f, g, h) VALUES (1, 1, 1, 2, 2, 1, 2, 2)""")
          session.execute(s"""INSERT INTO $ks.test1 (a, b, c, d, e, f, g, h) VALUES (1, 2, 1, 1, 1, 2, 1, 1)""")
          session.execute(s"""INSERT INTO $ks.test1 (a, b, c, d, e, f, g, h) VALUES (1, 2, 1, 1, 2, 2, 1, 2)""")
          session.execute(s"""INSERT INTO $ks.test1 (a, b, c, d, e, f, g, h) VALUES (1, 2, 1, 2, 1, 2, 2, 1)""")
          session.execute(s"""INSERT INTO $ks.test1 (a, b, c, d, e, f, g, h) VALUES (1, 2, 1, 2, 2, 2, 2, 2)""")
        },

        Future {
          session.execute(s"CREATE TABLE $ks.test_rowwriter (a INT PRIMARY KEY, b INT)")
        },

        Future {
          session.execute(s"CREATE TABLE $ks.test_insert (a INT PRIMARY KEY, b INT)")
        },

        Future {
          session.execute(s"CREATE TABLE $ks.test_insert1 (a INT PRIMARY KEY, b INT)")
        },

        Future {
          session.execute(s"CREATE TABLE $ks.test_insert2 (a INT PRIMARY KEY, b INT)")
          session.execute(s"INSERT INTO $ks.test_insert2 (a, b) VALUES (3,4)")
          session.execute(s"INSERT INTO $ks.test_insert2 (a, b) VALUES (5,6)")
        },

        Future {
          session.execute(
            s"""
               |CREATE TABLE $ks.df_test(
               |  customer_id int,
               |  uri text,
               |  browser text,
               |  epoch bigint,
               |  PRIMARY KEY (customer_id, epoch, uri)
               |)""".stripMargin.replaceAll("\n", " "))
        },

        Future {
          session.execute(
            s"""
               |CREATE TABLE $ks.df_test2(
               |  customer_id int,
               |  uri text,
               |  browser text,
               |  epoch bigint,
               |  PRIMARY KEY (customer_id, epoch)
               |)""".stripMargin.replaceAll("\n", " "))
        }
      )
    }

    setupCassandraCatalog
    spark.conf.set("pushdown", pushDown)
  }

  def cassandraTable(tableRef: TableRef) : DataFrame = {
    spark.sql(s"SELECT * From ${tableRef.keyspace}.${tableRef.table}")
  }

  "Cassandra Source Relation" should "allow to select all rows" in {
    val result = cassandraTable(TableRef("test1", ks)).select("a").collect()
    result should have length 8
    result.head should have length 1
  }

  it should "allow to insert data into a cassandra table" in {
    spark.sql("SET confirm.truncate = true")
    spark.sql(s"SELECT * FROM $ks.test_insert").collect() should have length 0
    spark.sql(s"INSERT OVERWRITE TABLE $ks.test_insert SELECT a, b FROM $ks.test1")
    spark.sql(s"SELECT * FROM $ks.test_insert").collect() should have length 1
  }

  it should "allow to save data to a cassandra table" in {
    spark.sql(s"SELECT a, b from $ks.test1")
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "test_insert1", "keyspace" -> ks))
      .mode("append")
      .save()

    cassandraTable(TableRef("test_insert1", ks)).collect() should have length 1
  }

  it should "allow to overwrite a cassandra table" in {
    spark.sql(s"SELECT a, b from $ks.test1")
      .write
      .format("org.apache.spark.sql.cassandra")
      .mode(Overwrite)
      .options(Map("table" -> "test_insert2", "keyspace" -> ks, "confirm.truncate" -> "true"))
      .save()
    spark.sql(s"SELECT * FROM $ks.test_insert2").collect() should have length 1
  }

  // This test is just to make sure at runtime the implicit for RDD[Row] can be found
  it should "implicitly generate a rowWriter from its RDD form" in {
    spark.sql(s"SELECT a, b from $ks.test1").rdd.saveToCassandra(ks, "test_rowwriter")
  }

  it should "allow to filter a table" in {
    spark.sql(s"SELECT a, b FROM $ks.test1 WHERE a=1 and b=2 and c=1 and e=1").collect() should have length 2
  }

  it should "allow to filter a table with a function for a column alias" in {
    spark.sql(
      s"""SELECT * FROM (SELECT (a + b + c) AS x, d FROM $ks.test1)
         |WHERE x= 3""".stripMargin).collect() should have length 4
  }

  it should "allow to filter a table with alias" in {
    spark.sql(
      s"""SELECT * FROM (SELECT a AS a1, b AS b1, c AS c1, d AS d1, e AS e1 FROM $ks.test1)
         |WHERE  a1=1 and b1=2 and c1=1 and e1=1 """.stripMargin).collect() should have length 2
  }

  it should "be able to save DF with reversed order columns to a Cassandra table" in {
    val test_df = Test(1400820884, "http://foobar", "Firefox", 123242)

    val ss = spark
    import ss.implicits._
    val df = sc.parallelize(Seq(test_df)).toDF

    df.write
      .format("org.apache.spark.sql.cassandra")
      .mode(Overwrite)
      .options(Map("table" -> "df_test", "keyspace" -> ks, "confirm.truncate" -> "true"))
      .save()
    cassandraTable(TableRef("df_test", ks)).collect() should have length 1
  }

  it should "be able to save DF with partial columns to a Cassandra table" in {
    val test_df = TestPartialColumns(1400820884, "Firefox", 123242)

    val ss = spark
    import ss.implicits._
    val df = sc.parallelize(Seq(test_df)).toDF

    df.write
      .format("org.apache.spark.sql.cassandra")
      .mode(Overwrite)
      .options(Map("table" -> "df_test2", "keyspace" -> ks, "confirm.truncate" -> "true"))
      .save()
    cassandraTable(TableRef("df_test2", ks)).collect() should have length 1
  }

  it should "throws exception during overwriting a table when confirm.truncate is false" in {
    val test_df = TestPartialColumns(1400820884, "Firefox", 123242)
    val ss = spark
    import ss.implicits._

    val df = sc.parallelize(Seq(test_df)).toDF

    val message = intercept[UnsupportedOperationException] {
      df.write
        .format("org.apache.spark.sql.cassandra")
        .mode(Overwrite)
        .options(Map("table" -> "df_test2", "keyspace" -> ks, "confirm.truncate" -> "false"))
        .save()
    }.getMessage

    assert(
      message.contains("You are attempting to use overwrite mode"),
      "Exception should be thrown when  attempting to overwrite a table if confirm.truncate is false")

  }

  it should "apply user custom predicates which erase basic pushdowns" in withConfig(
      CassandraSourceRelation.AdditionalCassandraPushDownRulesParam.name,
      "com.datastax.spark.connector.sql.PushdownNothing") {

    val df = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> ks, "table" -> "test1", "pushdown" -> "true"))
      .load().filter("a=1 and b=2 and c=1 and e=1")

    val qp = df.queryExecution.executedPlan.toString
    qp should include ("Filter (") // Should have a Spark Filter Step
  }

  it should "apply user custom predicate rules in the order they are specified" in withConfig(
      CassandraSourceRelation.AdditionalCassandraPushDownRulesParam.name,
      "com.datastax.spark.connector.sql.PushdownNothing,com.datastax.spark.connector.sql.PushdownEverything,com.datastax.spark.connector.sql.PushdownEqualsOnly") {

    val df = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map(
        "keyspace" -> ks,
        "table" -> "test1",
        "pushdown" -> "true",
        CassandraSourceRelation.AdditionalCassandraPushDownRulesParam.name -> "com.datastax.spark.connector.sql.PushdownNothing,com.datastax.spark.connector.sql.PushdownEverything,com.datastax.spark.connector.sql.PushdownEqualsOnly"))
      .load()
      .filter("a=1 and b=2 and c=1 and e=1")
      .select("a", "b", "c", "e")

    val cassandraScan = getCassandraScan(df.queryExecution.executedPlan)

    val pushedWhere = cassandraScan.cqlQueryParts.whereClause
    val predicates = pushedWhere.predicates.map(_.trim)
    val values = pushedWhere.values
    val pushedPredicates = predicates.zip(values)
    pushedPredicates should contain allOf(
      ("\"a\" = ?", 1),
      ("\"b\" = ?", 2),
      ("\"c\" = ?", 1),
      ("\"e\" = ?", 1))
  }

  it should "pass through local conf properties" in withConfig(
      CassandraSourceRelation.AdditionalCassandraPushDownRulesParam.name,
      "com.datastax.spark.connector.sql.PushdownUsesConf") {

    SparkSession.setActiveSession(spark)

    val df = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> ks, "table" -> "test1", PushdownUsesConf.testKey -> "Don't Remove"))
      .load().filter("g=1 and h=1")

    df.queryExecution.executedPlan // Will throw an exception if local key is not set

    val df2 = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> ks, "table" -> "test1", "pushdown" -> "true"))
      .load().filter("g=1 and h=1")

    intercept[IllegalAccessException] {
      df2.explain() //
    }
  }

  private def joinConversionLowThreshold(testFun: => Unit): Unit =
    withConfig(InClauseToJoinWithTableConversionThreshold.name, 1) { testFun }

  private def assertOnCassandraInJoinPresence(df: DataFrame): Unit = {
    if (pushDown)
      withClue(s"Given Dataframe plan does not contain CassandraInJoin in its predecessors.\n${df.queryExecution.sparkPlan.toString()}") {
        df.queryExecution.executedPlan.collectLeaves().collectFirst{
          case a@BatchScanExec(_, _: CassandraInJoin, _, _, _, _) => a
          case b@AdaptiveSparkPlanExec(_, _, _, _, _) =>
            b.executedPlan.collectLeaves().collectFirst{
              case a@BatchScanExec(_, _: CassandraInJoin, _, _, _, _) => a
            }
        } shouldBe defined
     }
    else
      assertOnAbsenceOfCassandraInJoin(df)
  }

  private def assertOnAbsenceOfCassandraInJoin(df: DataFrame): Unit =
    withClue(s"Given Dataframe plan contains CassandraInJoin in its predecessors.\n${df.queryExecution.sparkPlan.toString()}") {
      df.queryExecution.executedPlan.collectLeaves().collectFirst{
        case a@BatchScanExec(_, _: CassandraInJoin, _, _, _, _) => a
      } shouldBe empty
    }

  it should "convert to joinWithCassandra for 'IN' clause spanned on simple partition key " in joinConversionLowThreshold {
    val df = spark.sql(s"SELECT * FROM $ks.df_test2 WHERE customer_id IN (1,2)")
    assertOnCassandraInJoinPresence(df)
  }

  it should "convert to joinWithCassandra for 'IN' clause for composite partition key " in joinConversionLowThreshold {
    val df = spark.sql(s"SELECT a,b FROM $ks.test1 WHERE a IN (1,2) AND b IN (1,2) AND c IN (1,2)")
    assertOnCassandraInJoinPresence(df)
    df.collect() should have size 8
  }

  it should "convert to joinWithCassandra for 'IN' clause for some columns of composite partition key " in joinConversionLowThreshold {
    val df = spark.sql(s"SELECT * FROM $ks.test1 WHERE a IN (1,2) AND b = 2 AND c IN (1,2,3)")
    assertOnCassandraInJoinPresence(df)
    df.collect() should have size 4
  }

  it should "convert to joinWithCassandra for 'IN' clause for partition key and some clustering columns" in joinConversionLowThreshold {
    val df = spark.sql(s"SELECT * FROM $ks.test1 WHERE a IN (1,2) AND b IN (1,2) AND c IN (1,2) AND d IN (1) AND e IN (1)")
    assertOnCassandraInJoinPresence(df)
    df.collect() should have size 2
  }

  it should "convert to joinWithCassandra for 'IN' clause for partition key and all clustering columns" in joinConversionLowThreshold {
    val df = spark.sql(s"SELECT * FROM $ks.test1 WHERE a IN (1,2) AND b IN (1,2) AND c IN (1,2) AND d IN (1) AND e IN (1) AND f IN (1,3)")
    assertOnCassandraInJoinPresence(df)
    df.collect() should have size 1
  }

  it should "convert to joinWithCassandra for 'IN' clause for partition and some clustering columns and range" in joinConversionLowThreshold {
    val df = spark.sql(s"SELECT * FROM $ks.test1 WHERE a IN (1,2) AND b IN (1,2) AND c IN (1,2) AND d IN (1) AND e IN (1) AND f < 2")
    assertOnCassandraInJoinPresence(df)
    val rows = df.collect()
    rows should have size 1
    rows.head.size should be(8)
  }

  it should "convert to joinWithCassandra for 'IN' clause for partition columns and respect required columns" in joinConversionLowThreshold {
    val df = spark.sql(s"SELECT a,b,c FROM $ks.test1 WHERE a IN (1,2) AND b IN (1,2) AND c IN (1,2)")
    assertOnCassandraInJoinPresence(df)
    df.collect().head.size should be(3)
  }

  it should "convert to joinWithCassandra for 'IN' clause for partition columns and retrieve all columns" in joinConversionLowThreshold {
    val df = spark.sql(s"SELECT * FROM $ks.test1 WHERE a IN (1,2) AND b IN (1,2) AND c IN (1,2)")
    assertOnCassandraInJoinPresence(df)
    df.collect().head.size should be(8)
  }

  it should "convert to joinWithCassandra for 'IN' clause for partition key and clustering columns regardless the predicate order" in joinConversionLowThreshold {
    val df = spark.sql(s"SELECT * FROM $ks.test1 WHERE e IN (2) AND b IN (1,2) AND a IN (1,2) AND d IN (2) AND f IN (2) AND c IN (1)")
    assertOnCassandraInJoinPresence(df)
    val rows = df.collect()
    rows should have size 1
    rows.head.mkString(",") should be("1,2,1,2,2,2,2,2")
  }

  it should "convert to joinWithCassandra for 'IN' clause for partition key and clustering columns with equal predicates" in joinConversionLowThreshold {
    val df = spark.sql(s"SELECT * FROM $ks.test1 WHERE e = 2 AND b IN (1,2) AND a IN (1,2) AND d = 2 AND f IN (2) AND c = 1")
    assertOnCassandraInJoinPresence(df)
    val rows = df.collect()
    rows should have size 1
    rows.head.mkString(",") should be("1,2,1,2,2,2,2,2")
  }

  it should "convert to joinWithCassandra for 'IN' clause for partition columns and respect required columns order" in joinConversionLowThreshold {
    val df = spark.sql(s"SELECT b,a FROM $ks.test1 WHERE a IN (1) AND b IN (2) AND c IN (1,9)")
    assertOnCassandraInJoinPresence(df)
    val rows = df.collect()
    rows should have size 4
    rows.foreach(row => row.mkString(",") should be("2,1"))
  }

  it should "convert to joinWithCassandra for 'IN' clause for partition columns and allow count" in joinConversionLowThreshold {
    val df = spark.sql(s"SELECT count(1) FROM $ks.test1 WHERE a IN (1) AND b IN (2) AND c IN (1,9)")
    assertOnCassandraInJoinPresence(df)
    val rows = df.collect()
    rows should have size 1
    rows.head.getLong(0) should be(4)
  }

  it should "not convert to joinWithCassandra when some partition columns have no predicate" in joinConversionLowThreshold {
    val df = spark.sql(s"SELECT * FROM $ks.test1 WHERE a IN (1) AND b IN (2,3,4,5)")
    assertOnAbsenceOfCassandraInJoin(df)
  }

  it should "not convert to joinWithCassandra if 'IN' value sets cumulative size does not exceed configurable threshold" in
      withConfig(InClauseToJoinWithTableConversionThreshold.name, 9) {
    val df = spark.sql(s"SELECT * FROM $ks.test1 WHERE a IN (1,2) AND b IN (2,1) AND c IN (3,1)")
    assertOnAbsenceOfCassandraInJoin(df)
  }

  it should "not convert to joinWithCassandra for 'IN' clause if threshold is 0" in
      withConfig((InClauseToJoinWithTableConversionThreshold.name, 0)) {
        val df = spark.sql(s"SELECT * FROM $ks.test1 WHERE a IN (1) AND b IN (2) AND c IN (1,9)")
        assertOnAbsenceOfCassandraInJoin(df)
  }

  it should "keep default parallelism when converting IN clause to joinWithCassandra" in joinConversionLowThreshold {
    if (pushDown) {
      spark.sparkContext.defaultParallelism
      val df = spark.sql(s"SELECT * FROM $ks.test1 WHERE a IN (1,2) AND b IN (1,2) AND c IN (1,2)")
      assertOnCassandraInJoinPresence(df)
      df.rdd.partitions.length should be (df.sparkSession.sparkContext.defaultParallelism)
    }
  }

  it should "read from materialized view by new partition key" in  {
    val df = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> ks, "table" -> "test1_by_g"))
      .load().filter("g=1")

    df.collect() should have size 4
  }

  it should "have predicates pushed down when reading from materialized view" in  {
    val df = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> ks, "table" -> "test1_by_g", "pushdown" -> "true"))
      .load().filter("g=1 and a=1")

    val cassandraScan = getCassandraScan(df.queryExecution.executedPlan)

    val pushedWhere = cassandraScan.cqlQueryParts.whereClause
    val predicates = pushedWhere.predicates.map(_.trim)
    val values = pushedWhere.values
    val pushedPredicates = predicates.zip(values)

    pushedPredicates should contain allOf (("\"g\" = ?", 1), ("\"a\" = ?", 1))

  }

  it should "throw UnsupportedOperationException when trying to write to materialized view" in  {
    val test_df = TestMvWrite(3, 3, 3, 3, 3, 3, 3, 3)

    val ss = spark
    import ss.implicits._
    val df = sc.parallelize(Seq(test_df)).toDF

    val thrown = the [UnsupportedOperationException] thrownBy df.write
      .format("org.apache.spark.sql.cassandra")
      .mode(Append)
      .options(Map("table" -> "test1_by_g", "keyspace" -> ks))
      .save()

    thrown.getMessage should be ("Writing to a materialized view is not supported")
  }
}

case class Test(epoch: Long, uri: String, browser: String, customer_id: Int)

case class TestPartialColumns(epoch: Long, browser: String, customer_id: Int)

case class TestMvWrite(a: Long, b: Long, c: Long, d: Long, e: Long, f: Long, g: Long, h: Long)

object PushdownEverything extends CassandraPredicateRules {
  override def apply(
    predicates: AnalyzedPredicates,
    tableDef: TableDef,
    sparkConf: SparkConf): AnalyzedPredicates = {

    AnalyzedPredicates(predicates.handledByCassandra ++ predicates.handledBySpark, Set.empty)
  }
}

object PushdownNothing extends CassandraPredicateRules {
  override def apply(
    predicates: AnalyzedPredicates,
    tableDef: TableDef,
    sparkConf: SparkConf): AnalyzedPredicates = {

    AnalyzedPredicates(Set.empty, predicates.handledByCassandra ++ predicates.handledBySpark)
  }
}

object PushdownEqualsOnly extends CassandraPredicateRules {
  override def apply(
    predicates: AnalyzedPredicates,
    tableDef: TableDef,
    sparkConf: SparkConf): AnalyzedPredicates = {

    val eqFilters = (predicates.handledByCassandra ++ predicates.handledBySpark).collect {
      case x: EqualTo => x: Filter
    }
    AnalyzedPredicates(
      eqFilters,
      (predicates.handledBySpark ++ predicates.handledByCassandra) -- eqFilters)
  }
}

/**
  * Throws an exception if test key is not set
  */
object PushdownUsesConf extends CassandraPredicateRules {
  val testKey = "testkey"
  val notSet = "notset"
  override def apply(
                      predicates: AnalyzedPredicates,
                      tableDef: TableDef,
                      conf: SparkConf): AnalyzedPredicates = {
    if (conf.contains(testKey)){
      predicates
    } else {
      throw new IllegalAccessException(s"Conf did not contain $testKey")
    }
  }
}
