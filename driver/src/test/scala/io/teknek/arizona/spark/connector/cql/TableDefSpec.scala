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

package io.teknek.arizona.spark.connector.cql

import _root_.io.teknek.arizona.spark.connector.types._
import org.scalatest.{Matchers, WordSpec}

import scala.collection.SortedMap

class TableDefSpec extends WordSpec with Matchers {

  "A TableDef#cql method" should {
    "produce valid CQL" when {
      "it contains no clustering columns" in {
        val column1 = ColumnDef("c1", PartitionKeyColumn, IntType)
        val column2 = ColumnDef("c2", RegularColumn, VarCharType)
        val tableDef = TableDef("keyspace", "table", Seq(column1), Seq.empty, Seq(column2))
        tableDef.cql should be(
          """CREATE TABLE "keyspace"."table" (
            |  "c1" int,
            |  "c2" varchar,
            |  PRIMARY KEY (("c1"))
            |)""".stripMargin
        )
      }

      "it contains clustering columns" in {
        val column1 = ColumnDef("c1", PartitionKeyColumn, IntType)
        val column2 = ColumnDef("c2", ClusteringColumn(0), VarCharType)
        val column3 = ColumnDef("c3", RegularColumn, VarCharType)
        val tableDef = TableDef("keyspace", "table", Seq(column1), Seq(column2), Seq(column3))
        tableDef.cql should be(
          """CREATE TABLE "keyspace"."table" (
            |  "c1" int,
            |  "c2" varchar,
            |  "c3" varchar,
            |  PRIMARY KEY (("c1"), "c2")
            |)""".stripMargin
        )
      }

      "it contains compound partition key and multiple clustering columns" in {
        val column1 = ColumnDef("c1", PartitionKeyColumn, IntType)
        val column2 = ColumnDef("c2", PartitionKeyColumn, VarCharType)
        val column3 = ColumnDef("c3", ClusteringColumn(0), VarCharType)
        val column4 = ColumnDef("c4", ClusteringColumn(1), VarCharType)
        val column5 = ColumnDef("c5", RegularColumn, VarCharType)
        val tableDef = TableDef("keyspace", "table", Seq(column1, column2), Seq(column3, column4), Seq(column5))
        tableDef.cql should be(
          """CREATE TABLE "keyspace"."table" (
            |  "c1" int,
            |  "c2" varchar,
            |  "c3" varchar,
            |  "c4" varchar,
            |  "c5" varchar,
            |  PRIMARY KEY (("c1", "c2"), "c3", "c4")
            |)""".stripMargin
        )
      }

      "it contains a column of a collection type" in {
        val column1 = ColumnDef("c1", PartitionKeyColumn, IntType)
        val column2 = ColumnDef("c2", RegularColumn, MapType(BigIntType, VarCharType))
        val tableDef = TableDef("keyspace", "table", Seq(column1), Seq.empty, Seq(column2))
        tableDef.cql should be(
          """CREATE TABLE "keyspace"."table" (
            |  "c1" int,
            |  "c2" map<bigint, varchar>,
            |  PRIMARY KEY (("c1"))
            |)""".stripMargin
        )
      }

      "it contains compound partition key, if not exists, multiple clustering columns with sorting and options" in {
        val column1 = ColumnDef("c1", PartitionKeyColumn, IntType)
        val column2 = ColumnDef("c2", PartitionKeyColumn, VarCharType)
        val column3 = ColumnDef("c3", ClusteringColumn(0), VarCharType)
        val column4 = ColumnDef("c4", ClusteringColumn(1, ClusteringColumn.Descending), VarCharType)
        val column5 = ColumnDef("c5", RegularColumn, VarCharType)
        //
        val tableDefSortOnly = TableDef("keyspace", "table", Seq(column1, column2), Seq(column3, column4),
          Seq(column5), ifNotExists = true)
        tableDefSortOnly.cql should be(
          """CREATE TABLE IF NOT EXISTS "keyspace"."table" (
            |  "c1" int,
            |  "c2" varchar,
            |  "c3" varchar,
            |  "c4" varchar,
            |  "c5" varchar,
            |  PRIMARY KEY (("c1", "c2"), "c3", "c4")
            |) WITH CLUSTERING ORDER BY ("c3" ASC, "c4" DESC)""".stripMargin
        )
        //
        val tableDefSortAndOptions = TableDef("keyspace", "table", Seq(column1, column2),
          Seq(column3, column4), Seq(column5), ifNotExists = true,
          tableOptions = Map("dclocal_read_repair_chance"-> "0.1" ))
        tableDefSortAndOptions.cql should be(
          """CREATE TABLE IF NOT EXISTS "keyspace"."table" (
            |  "c1" int,
            |  "c2" varchar,
            |  "c3" varchar,
            |  "c4" varchar,
            |  "c5" varchar,
            |  PRIMARY KEY (("c1", "c2"), "c3", "c4")
            |) WITH CLUSTERING ORDER BY ("c3" ASC, "c4" DESC)
            |  AND dclocal_read_repair_chance = 0.1""".stripMargin
        )
        //
        val tableDefOptionsOnly = TableDef("keyspace", "table", Seq(column1, column2),
          Seq(column3), Seq(column5), ifNotExists = true,
          tableOptions = Map("dclocal_read_repair_chance"-> "0.1"))
        tableDefOptionsOnly.cql should be(
          """CREATE TABLE IF NOT EXISTS "keyspace"."table" (
            |  "c1" int,
            |  "c2" varchar,
            |  "c3" varchar,
            |  "c5" varchar,
            |  PRIMARY KEY (("c1", "c2"), "c3")
            |) WITH dclocal_read_repair_chance = 0.1""".stripMargin
        )
      }
    }
  }
}
