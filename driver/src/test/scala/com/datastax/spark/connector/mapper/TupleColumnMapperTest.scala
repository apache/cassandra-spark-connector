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

package com.datastax.spark.connector.mapper

import java.util.Date

import com.datastax.spark.connector.ColumnName
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.types._
import org.apache.commons.lang3.SerializationUtils
import org.junit.Assert._
import org.junit.Test

class TupleColumnMapperTest {

  private val c1 = ColumnDef("column1", PartitionKeyColumn, IntType)
  private val c2 = ColumnDef("column2", ClusteringColumn(0), IntType)
  private val c3 = ColumnDef("column3", RegularColumn, IntType)
  private val tableDef = TableDef("test", "table", Seq(c1), Seq(c2), Seq(c3))
  private val selectedColumns = IndexedSeq(c1, c2, c3).map(_.ref)

  @Test
  def testGetters(): Unit = {
    val columnMap = new TupleColumnMapper[(Int, String, Boolean)]
      .columnMapForWriting(tableDef, selectedColumns)
    val getters = columnMap.getters
    assertEquals(ColumnName("column1"), getters("_1"))
    assertEquals(ColumnName("column2"), getters("_2"))
    assertEquals(ColumnName("column3"), getters("_3"))
  }

  @Test
  def testIncompleteGetters(): Unit = {
    // Incomplete getter mapping is allowed, because the user might want to save
    // only a part of the object to Cassandra
    val selectedColumns = IndexedSeq(ColumnName("column1"), ColumnName("column3"))
    val columnMap = new TupleColumnMapper[(Int, Int, Int)]
      .columnMapForWriting(tableDef, selectedColumns)
    val getters = columnMap.getters
    assertEquals(ColumnName("column1"), getters("_1"))
    assertEquals(ColumnName("column3"), getters("_2"))
  }

  @Test
  def testConstructor(): Unit = {
    val columnMap = new TupleColumnMapper[(Int, String, Boolean)]
      .columnMapForReading(tableDef, selectedColumns)
    assertEquals(
      Seq(ColumnName("column1"), ColumnName("column2"), ColumnName("column3")),
      columnMap.constructor)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testIncompleteConstructor(): Unit = {
    // Incomplete constructor mapping is not allowed, because
    // it is not possible to call a constructor if any parameters are missing
    val selectedColumns = IndexedSeq(ColumnName("column1"), ColumnName("column3"))
    val columnMap = new TupleColumnMapper[(Int, Int, Int)].columnMapForReading(tableDef, selectedColumns)
    columnMap.constructor
  }

  @Test
  def testSerialize(): Unit = {
    val columnMap = new TupleColumnMapper[(Int, String, Boolean)]
      .columnMapForReading(tableDef, selectedColumns)
    SerializationUtils.roundtrip(columnMap)
  }

  @Test
  def testImplicit(): Unit = {
    val columnMap = implicitly[ColumnMapper[(Int, String, Boolean)]]
      .columnMapForWriting(tableDef, selectedColumns)
    val getters = columnMap.getters
    assertEquals(ColumnName("column1"), getters("_1"))
    assertEquals(ColumnName("column2"), getters("_2"))
    assertEquals(ColumnName("column3"), getters("_3"))
  }

  @Test
  def testNewTable(): Unit = {
    val columnMapper = new TupleColumnMapper[(Int, String, Boolean, List[Date])]
    val table = columnMapper.newTable("keyspace", "table")
    assertEquals("keyspace", table.keyspaceName)
    assertEquals("table", table.tableName)
    assertEquals(4, table.columns.size)
    assertEquals(1, table.partitionKey.size)
    assertEquals(IntType, table.partitionKey(0).columnType)
    assertEquals(VarCharType, table.columns(1).columnType)
    assertEquals(BooleanType, table.columns(2).columnType)
    assertEquals(ListType(TimestampType), table.columns(3).columnType)
  }

}
