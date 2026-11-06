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

package io.teknek.arizona.spark.connector.writer

import java.time.Instant

import scala.collection.immutable.Map
import scala.reflect.runtime.universe._
import org.apache.commons.lang3.SerializationUtils
import org.junit.Assert._
import org.junit.Test
import _root_.io.teknek.arizona.spark.connector.UDTValue
import _root_.io.teknek.arizona.spark.connector.cql._
import _root_.io.teknek.arizona.spark.connector.types._

case class StringWrapper(str: String)

object StringWrapperConverter extends TypeConverter[String] {
  override def targetTypeTag = typeTag[String]
  override def convertPF = {
    case StringWrapper(str) => str
  }
}

class DefaultRowWriterTest {

  @Test
  def testSerializability(): Unit = {
    val table = TableDef("test", "table", Nil, Nil, Nil)
    val rowWriter = new DefaultRowWriter[DefaultRowWriterTest](table, IndexedSeq.empty)
    SerializationUtils.roundtrip(rowWriter)
  }

  case class RowOfStrings(c1: String, c2: String, c3: String)

  @Test
  def testTypeConversionsAreApplied(): Unit = {
    val column1 = ColumnDef("c1", PartitionKeyColumn, IntType)
    val column2 = ColumnDef("c2", ClusteringColumn(0), DecimalType)
    val column3 = ColumnDef("c3", RegularColumn, TimestampType)
    val table = TableDef("test", "table", Seq(column1), Seq(column2), Seq(column3))
    val rowWriter = new DefaultRowWriter[RowOfStrings](
      table, table.columnRefs)

    val obj = RowOfStrings("1", "1.11", "2015-01-01 12:11:34")
    val buf = Array.ofDim[Any](3)
    rowWriter.readColumnValues(obj, buf)
    val i1 = rowWriter.columnNames.indexOf("c1")
    val i2 = rowWriter.columnNames.indexOf("c2")
    val i3 = rowWriter.columnNames.indexOf("c3")

    assertEquals(java.lang.Integer.valueOf(1), buf(i1))
    assertEquals(new java.math.BigDecimal("1.11"), buf(i2))
    assertTrue(buf(i3).isInstanceOf[Instant])
  }

  case class RowWithUDT(c1: UDTValue)

  @Test
  def testTypeConversionsInUDTValuesAreApplied(): Unit = {
    val udtColumn = UDTFieldDef("field", IntType)
    val udt = UserDefinedType("udt", IndexedSeq(udtColumn))

    val column = ColumnDef("c1", PartitionKeyColumn, udt)
    val table = TableDef("test", "table", Seq(column), Nil, Nil)
    val rowWriter = new DefaultRowWriter[RowWithUDT](table, table.columnRefs)

    // we deliberately put a String 12345 here:
    val udtValue = UDTValue.fromMap(Map("field" -> "12345"))
    val obj = RowWithUDT(udtValue)

    val buf = Array.ofDim[Any](1)
    rowWriter.readColumnValues(obj, buf)

    // UDTValue should not be converted to any other type,
    // because column is of UserDefinedType
    assertTrue(buf(0).isInstanceOf[UDTValue])

    // the field value must be converted to java.lang.Integer, because its field type is IntType
    assertEquals(java.lang.Integer.valueOf(12345), buf(0).asInstanceOf[UDTValue].getRaw("field"))
  }

  case class RowWithStringWrapper(c1: StringWrapper)

  @Test
  def testCustomTypeConvertersAreUsed(): Unit = {
    TypeConverter.registerConverter(StringWrapperConverter)
    try {
      val column = ColumnDef("c1", PartitionKeyColumn, TextType)
      val table = TableDef("test", "table", Seq(column), Nil, Nil)
      val rowWriter = new DefaultRowWriter[RowWithStringWrapper](table, table.columnRefs)

      val obj = RowWithStringWrapper(StringWrapper("some text"))
      val buf = Array.ofDim[Any](1)
      rowWriter.readColumnValues(obj, buf)

      // if our custom type converter wasn't used,
      // we'd get "StringWrapper(some text)" here instead of "some text":
      assertEquals("some text", buf(0))
    } finally {
      TypeConverter.unregisterConverter(StringWrapperConverter)
    }
  }
}
