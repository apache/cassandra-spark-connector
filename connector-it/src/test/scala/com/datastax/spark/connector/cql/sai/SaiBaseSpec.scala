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

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.spark.connector.SparkCassandraITSpecBase
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.datasource.{CassandraScan, CassandraScanBuilder}
import com.datastax.spark.connector.rdd.CqlWhereClause
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.{FilterExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.sources.{EqualTo, Filter, GreaterThan, GreaterThanOrEqual, In, LessThan, LessThanOrEqual}
import org.scalatest.Matchers

trait SaiBaseSpec extends Matchers with SparkCassandraITSpecBase {
  override lazy val conn = CassandraConnector(defaultConf)

  def df(table: String): DataFrame = spark.read.cassandraFormat(table, ks).load()

  def createTableWithIndexes(session: CqlSession, tableName: String, columns: Seq[String]): Unit = {
    val typesDeclaration = columns.map { t => s"${t}_col ${t}" }.mkString("", ",", ",")

    session.execute(
      s"""CREATE TABLE IF NOT EXISTS $ks.$tableName (
         |  pk int,
         |  $typesDeclaration
         |  PRIMARY KEY (pk));""".stripMargin)

    columns.foreach { t =>
      session.execute(
        s"CREATE CUSTOM INDEX ${t}_sai_idx ON $ks.$tableName (${t}_col) USING 'StorageAttachedIndex';")
    }
  }

  def findFilterOption(plan: SparkPlan): Option[FilterExec] = {
    plan match {
      case filter: FilterExec => Option(filter)
      case project: ProjectExec => findFilterOption(project.child)
      case _ => None
    }
  }

  def findFilter(plan: SparkPlan): FilterExec = {
    findFilterOption(plan).getOrElse(throw new NoSuchElementException("Filter was not found in the given plan"))
  }

  def findCassandraScan(plan: SparkPlan): CassandraScan = {
    plan match {
      case BatchScanExec(_, scan: CassandraScan, _, _, _, _) => scan
      case filter: FilterExec => findCassandraScan(filter.child)
      case project: ProjectExec => findCassandraScan(project.child)
      case _ => throw new NoSuchElementException("RowDataSourceScanExec was not found in the given plan")
    }
  }

  def debug(dataFrame: DataFrame)(f: => Unit): DataFrame = {
    dataFrame.explain(true)
    f
    dataFrame.show()
    dataFrame
  }

  def assertPushDown(dataFrame: DataFrame): DataFrame = debug(dataFrame) {
    val plan = dataFrame.queryExecution.sparkPlan
    withClue("The given plan should not contain Filter element, some of the predicates were not pushed down.") {
      findFilterOption(plan) should not be defined
    }
  }

  def assertNoPushDown(dataFrame: DataFrame): DataFrame = debug(dataFrame) {
    val plan = dataFrame.queryExecution.sparkPlan
    findFilter(plan)
    val scan = findCassandraScan(plan)
    withClue("The given plan should not contain pushed down predicates") {
      scan.cqlQueryParts.whereClause.predicates shouldBe empty
    }
  }

  def assertNonPushedColumns(dataFrame: DataFrame, nonPushedColumns: String*): DataFrame = debug(dataFrame) {
    val plan = dataFrame.queryExecution.sparkPlan
    val filter = findFilter(plan)
    val nonPushedFromPlan = filter.condition.children.collect {
      case e: AttributeReference => e.name
    }
    nonPushedFromPlan.toSet should be(nonPushedColumns.toSet)
  }

  def assertPushedPredicate(dataFrame: DataFrame, pushedPredicate: Filter*): DataFrame = debug(dataFrame) {
    val plan = dataFrame.queryExecution.sparkPlan
    val scan = findCassandraScan(plan)

    val handled = scan.cqlQueryParts.whereClause
    val expected = CassandraScanBuilder.filterToCqlWhereClause(scan.tableDef, pushedPredicate.toArray)

    def comparablePredicates(where: CqlWhereClause): Set[String] = {
      where.toString.drop(2).dropRight(2).split("],\\[").toSet
    }

    withClue("The given df contains unexpected set of push down filters") {
      comparablePredicates(handled) shouldBe comparablePredicates(expected)
    }
  }
}
