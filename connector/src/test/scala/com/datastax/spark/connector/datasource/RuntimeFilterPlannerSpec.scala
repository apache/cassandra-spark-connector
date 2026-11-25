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

package com.datastax.spark.connector.datasource

import com.datastax.spark.connector.ColumnName
import com.datastax.spark.connector.cql.{ClusteringColumn, ColumnDef, PartitionKeyColumn, RegularColumn, TableDef}
import com.datastax.spark.connector.datasource.ScanHelper.CqlQueryParts
import com.datastax.spark.connector.rdd.CqlWhereClause
import com.datastax.spark.connector.types.{ColumnType, DecimalType, DoubleType, FloatType, IntType, TextType}
import org.apache.spark.SparkConf
import org.apache.spark.sql.cassandra.CassandraSourceRelation.RuntimeFilteringEnabledParam
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RuntimeFilterPlannerSpec extends AnyFlatSpec with Matchers {

  private def table(keyTypes: ColumnType[_]*): TableDef = TableDef("ks", "t",
    keyTypes.zipWithIndex.map { case (t, i) => ColumnDef(s"k$i", PartitionKeyColumn, t) },
    Seq(ColumnDef("c", ClusteringColumn(0), IntType)),
    Seq(ColumnDef("v", RegularColumn, TextType)))

  private def schema(columns: String*) = StructType(columns.map(StructField(_, StringType)))

  private def columns(
    tableDef: TableDef,
    readColumns: Seq[String],
    where: CqlWhereClause = CqlWhereClause.empty,
    conf: SparkConf = new SparkConf()): Seq[String] =
    RuntimeFilterPlanner.runtimeFilterColumns(
      tableDef, CqlQueryParts(readColumns.map(ColumnName(_)).toIndexedSeq, where), schema(readColumns: _*), conf)

  "Runtime filter columns" should "be all partition key columns" in {
    columns(table(IntType), Seq("k0", "c", "v")) shouldBe Seq("k0")
    columns(table(IntType, TextType), Seq("k0", "k1")) shouldBe Seq("k0", "k1")
  }

  they should "be none if the scan does not return every partition key column" in {
    columns(table(IntType, TextType), Seq("k0", "v")) shouldBe empty
    columns(table(IntType), Seq("v")) shouldBe empty
    RuntimeFilterPlanner.runtimeFilterColumns(
      table(IntType), CqlQueryParts(IndexedSeq.empty), StructType(Seq.empty), new SparkConf()) shouldBe empty
  }

  they should "be none for decimal, float and double keys" in {
    for (t <- Seq(DecimalType, DoubleType, FloatType)) {
      columns(table(t), Seq("k0")) shouldBe empty
      columns(table(IntType, t), Seq("k0", "k1")) shouldBe empty
    }
  }

  they should "be none if the scan already restricts the partition key" in {
    columns(table(IntType), Seq("k0"), CqlWhereClause(Seq(""""k0" = ?"""), Seq(1))) shouldBe empty
    columns(table(IntType), Seq("k0"), CqlWhereClause(Seq(""""k0" IN (?, ?)"""), Seq(1, 2))) shouldBe empty
    columns(table(IntType, TextType), Seq("k0", "k1"), CqlWhereClause(Seq(""""k1" = ?"""), Seq("a"))) shouldBe empty
  }

  they should "be kept with restrictions on clustering and regular columns" in {
    columns(table(IntType), Seq("k0"), CqlWhereClause(Seq(""""c" > ?""", """"v" = ?"""), Seq(1, "a"))) shouldBe
      Seq("k0")
  }

  they should "be none for Solr queries" in {
    columns(table(IntType), Seq("k0"), CqlWhereClause(Seq("solr_query = '*:*'"), Seq.empty)) shouldBe empty
  }

  they should "be none if disabled or without pushdown" in {
    columns(table(IntType), Seq("k0"), conf = new SparkConf().set(RuntimeFilteringEnabledParam.name, "false")) shouldBe
      empty
    columns(table(IntType), Seq("k0"), conf = new SparkConf().set("pushdown", "false")) shouldBe empty
  }

  "Intersecting runtime values" should "keep the values in both sets" in {
    RuntimeFilterPlanner.intersect(Seq(1, 2, 3), Seq(3, 2, 7)) shouldBe Seq(2, 3)
    RuntimeFilterPlanner.intersect(Seq("a"), Seq("b")) shouldBe empty
  }

  it should "compare blobs by content" in {
    val result = RuntimeFilterPlanner.intersect(Seq(Array[Byte](1, 2), Array[Byte](3)), Seq(Array[Byte](1, 2)))
    result.map(_.asInstanceOf[Array[Byte]].toSeq) shouldBe Seq(Seq[Byte](1, 2))
  }
}
