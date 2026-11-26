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

import com.datastax.spark.connector.RowsInBatch
import com.datastax.spark.connector.cql.{ClusteringColumn, ColumnDef, PartitionKeyColumn, RegularColumn, TableDef}
import com.datastax.spark.connector.types.{CounterType, IntType, TextType}
import com.datastax.spark.connector.writer.{BatchGroupingKey, WriteConf}
import org.apache.spark.SparkConf
import org.apache.spark.sql.cassandra.CassandraSourceRelation.WriteClusteringParam
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CassandraWriteClusteringSpec extends AnyFlatSpec with Matchers {

  private val wide = TableDef("ks", "wide",
    Seq(ColumnDef("p1", PartitionKeyColumn, IntType), ColumnDef("P2", PartitionKeyColumn, TextType)),
    Seq(ColumnDef("c1", ClusteringColumn(0), IntType), ColumnDef("c2", ClusteringColumn(1), TextType)),
    Seq(ColumnDef("v", RegularColumn, TextType)))
  private val narrow = TableDef("ks", "narrow",
    Seq(ColumnDef("k", PartitionKeyColumn, IntType)), Seq.empty, Seq(ColumnDef("v", RegularColumn, TextType)))
  private val counters = TableDef("ks", "counters",
    Seq(ColumnDef("k", PartitionKeyColumn, IntType)), Seq(ColumnDef("c", ClusteringColumn(0), IntType)),
    Seq(ColumnDef("n", RegularColumn, CounterType)))

  private def conf(setting: String) = new SparkConf().set(WriteClusteringParam.name, setting)

  private def clustering(table: TableDef, setting: String, writeConf: WriteConf = WriteConf()) =
    CassandraWriteClustering.clustering(table, writeConf, conf(setting))

  "Write clustering" should "be off by default" in {
    CassandraWriteClustering.clustering(wide, WriteConf(), new SparkConf()) shouldBe None
    CassandraWriteClustering.setting(new SparkConf()) shouldBe CassandraWriteClustering.Off
  }

  it should "cluster by partition key and sort by primary key, in key order" in {
    clustering(wide, "on") shouldBe Some(CassandraWriteClustering(Seq("p1", "P2"), Seq("p1", "P2", "c1", "c2")))
    clustering(narrow, "on") shouldBe Some(CassandraWriteClustering(Seq("k"), Seq("k")))
  }

  it should "in auto mode cluster only tables with clustering columns" in {
    clustering(wide, "auto") shouldBe defined
    clustering(narrow, "auto") shouldBe None
  }

  it should "in auto mode not cluster counter tables" in {
    clustering(counters, "auto") shouldBe None
    clustering(counters, "on") shouldBe defined
  }

  it should "in auto mode cluster only if rows are batched by partition" in {
    clustering(wide, "auto", WriteConf(batchGroupingKey = BatchGroupingKey.None)) shouldBe None
    clustering(wide, "auto", WriteConf(batchGroupingKey = BatchGroupingKey.ReplicaSet)) shouldBe None
    clustering(wide, "auto", WriteConf(batchSize = RowsInBatch(1))) shouldBe None
    clustering(wide, "auto", WriteConf(batchSize = RowsInBatch(10))) shouldBe defined
  }

  it should "never cluster when off" in {
    clustering(wide, "off") shouldBe None
    clustering(wide, "false") shouldBe None
  }

  it should "accept any case and reject unknown values" in {
    clustering(wide, " ON ") shouldBe defined
    an[IllegalArgumentException] should be thrownBy clustering(wide, "sometimes")
  }
}
