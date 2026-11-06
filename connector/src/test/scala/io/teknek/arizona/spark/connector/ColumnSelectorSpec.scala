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

package io.teknek.arizona.spark.connector

import org.scalatest.{Matchers, WordSpec}

import _root_.io.teknek.arizona.spark.connector.cql._
import _root_.io.teknek.arizona.spark.connector.types.{TimestampType, VarCharType, IntType}

class ColumnSelectorSpec extends WordSpec with Matchers {
  "A ColumnSelector#selectFrom method" should {
    val column1 = ColumnDef("c1", PartitionKeyColumn, IntType)
    val column2 = ColumnDef("c2", PartitionKeyColumn, VarCharType)
    val column3 = ColumnDef("c3", ClusteringColumn(0), VarCharType)
    val column4 = ColumnDef("c4", ClusteringColumn(1), VarCharType)
    val column5 = ColumnDef("c5", RegularColumn, VarCharType)
    val column6 = ColumnDef("c6", RegularColumn, TimestampType)

    val tableDef = TableDef("keyspace", "table", Seq(column1, column2), Seq(column3, column4), Seq(column5, column6))

    "return all columns" in {
      val columns = AllColumns.selectFrom(tableDef)
      columns should equal(tableDef.columns.map(_.ref))
    }

    "return partition key columns" in {
      val columns = PartitionKeyColumns.selectFrom(tableDef)
      columns should equal(tableDef.partitionKey.map(_.ref))
    }

    "return some columns" in {
      val columns = SomeColumns("c1", "c3", "c5").selectFrom(tableDef)
      columns.map(_.columnName) should be equals Seq("c1", "c3", "c5")
    }

    "return selections with function calls" in {
      val selection = SomeColumns(
        ColumnName("c1"),
        FunctionCallRef("f", Left(ColumnName("c2"))::Nil)).selectFrom(tableDef)

      selection.map(_.cql) should be equals Seq(""""c1"""", """f("c2")""")
    }

    "throw a NoSuchElementException when selected column name is invalid" in {
      a[NoSuchElementException] should be thrownBy {
        SomeColumns("c1", "c3", "unknown_column").selectFrom(tableDef)
      }
    }

    "throw a NoSuchElementException when a function call has a missing column as an actual parameter" in {
      a[NoSuchElementException] should be thrownBy {
        SomeColumns("c1", FunctionCallRef("f", Left(ColumnName("unknown_column"))::Nil)).selectFrom(tableDef)
      }
    }

  }

}
