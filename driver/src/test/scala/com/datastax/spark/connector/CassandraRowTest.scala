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

package com.datastax.spark.connector

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.text.SimpleDateFormat

import org.junit.Assert._
import org.scalatest.{FunSuite, Matchers}

class CassandraRowTest extends FunSuite with Matchers {

  test("basicAccessTest") {
    val row = new CassandraRow(CassandraRowMetadata(Array("value").toIndexedSeq), Array("1").toIndexedSeq)
    assertEquals(1, row.size)
    assertEquals(Some("1"), row.getStringOption(0))
    assertEquals(Some("1"), row.getStringOption("value"))
    assertEquals("1", row.getString(0))
    assertEquals("1", row.getString("value"))
  }

  test("nullAccessTest") {
    val row = new CassandraRow(CassandraRowMetadata(Array("value").toIndexedSeq), Array(null).toIndexedSeq)
    assertEquals(None, row.getStringOption(0))
    assertEquals(None, row.getStringOption("value"))
    assertEquals(1, row.size)
  }

  test("NoneAccessTest") {
    val row = new CassandraRow(CassandraRowMetadata(Array("value").toIndexedSeq), Array(None).toIndexedSeq)
    assertEquals(None, row.getStringOption(0))
    assertEquals(None, row.getStringOption("value"))
    assertEquals(1, row.size)
  }


  test("nullToStringTest") {
    val row = new CassandraRow(CassandraRowMetadata(Array("value").toIndexedSeq), Array(null).toIndexedSeq)
    assertEquals("CassandraRow{value: null}", row.toString())
  }

  test("nonExistentColumnAccessTest") {
    val row = new CassandraRow(CassandraRowMetadata(Array("value").toIndexedSeq), Array(null).toIndexedSeq)
    intercept[ColumnNotFoundException] {
      row.getString("wring-column")
    }
  }

  test("primitiveConversionTest") {
    val dateStr = "2014-04-08 14:47:00+0100"
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ")
    val date = dateFormat.parse(dateStr)
    val integer = Integer.valueOf(2)
    val string = "3"

    val row = new CassandraRow(CassandraRowMetadata(Array("date", "integer", "string").toIndexedSeq), Array(date, integer, string).toIndexedSeq)
    assertEquals(3, row.size)
    assertEquals(date, row.getDate("date"))
    assertEquals(date.getTime, row.getLong("date"))
    assertEquals(dateFormat.format(date), row.getString("date"))

    assertEquals("2", row.getString("integer"))
    assertEquals(2, row.getInt("integer"))
    assertEquals(2L, row.getLong("integer"))
    assertEquals(2.0, row.getDouble("integer"), 0.0001)
    assertEquals(2.0f, row.getFloat("integer"), 0.0001f)
    assertEquals(BigInt(2), row.getVarInt("integer"))
    assertEquals(BigDecimal(2), row.getDecimal("integer"))

    assertEquals("3", row.getString("string"))
    assertEquals(3, row.getInt("string"))
    assertEquals(3L, row.getLong("string"))
    assertEquals(BigInt(3), row.getVarInt("string"))
    assertEquals(BigDecimal(3), row.getDecimal("string"))
  }

  test("collectionConversionTest") {
    val list = new java.util.ArrayList[String]() // list<varchar>
    list.add("1")
    list.add("1")
    list.add("2")

    val set = new java.util.HashSet[String]() // set<varchar>
    set.add("apple")
    set.add("banana")
    set.add("mango")

    val map = new java.util.HashMap[String, Int]() // map<varchar, int>
    map.put("a", 1)
    map.put("b", 2)
    map.put("c", 3)

    val row = new CassandraRow(CassandraRowMetadata(Array("list", "set", "map").toIndexedSeq), Array(list, set, map).toIndexedSeq)

    val scalaList = row.getList[Int]("list")
    assertEquals(Vector(1, 1, 2), scalaList)

    val scalaListAsSet = row.getSet[Int]("list")
    assertEquals(Set(1, 2), scalaListAsSet)

    val scalaSet = row.getSet[String]("set")
    assertEquals(Set("apple", "banana", "mango"), scalaSet)

    val scalaMap = row.getMap[String, Long]("map")
    assertEquals(Map("a" ->1, "b" ->2, "c" ->3), scalaMap)

    val scalaMapAsSet = row.getSet[(String, String)]("map")
    assertEquals(Set("a" ->"1", "b" ->"2", "c" ->"3"), scalaMapAsSet)
  }

  test("serializationTest") {
    val row = new CassandraRow(CassandraRowMetadata(Array("value").toIndexedSeq), Array("1").toIndexedSeq)
    val bs = new ByteArrayOutputStream
    val os = new ObjectOutputStream(bs)
    os.writeObject(row)
    os.close()
    val is = new ObjectInputStream(new ByteArrayInputStream(bs.toByteArray))
    val row2 = is.readObject().asInstanceOf[CassandraRow]
    is.close()

    assertEquals("1", row2.getString("value"))
  }

}
