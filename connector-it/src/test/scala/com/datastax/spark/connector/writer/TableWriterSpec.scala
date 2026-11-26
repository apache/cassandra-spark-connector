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

package com.datastax.spark.connector.writer

import java.io.IOException

import com.datastax.oss.driver.api.core.DefaultProtocolVersion

import scala.jdk.CollectionConverters._
import scala.concurrent.Future
import com.datastax.spark.connector.cluster.DefaultCluster
import com.datastax.spark.connector.{SomeColumns, _}
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.mapper.DefaultColumnMapper
import com.datastax.spark.connector.types._
import org.apache.spark.SparkException

case class Address(street: String, city: String, zip: Int)
case class KV(key: Int, value: String)
case class KeyValue(key: Int, group: Long, value: String)
case class KeyValueWithTransient(key: Int, group: Long, value: String, @transient transientField: String)
case class KeyValueWithTTL(key: Int, group: Long, value: String, ttl: Int)
case class KeyValueWithTimestamp(key: Int, group: Long, value: String, timestamp: Long)
case class KeyValueWithConversion(key: String, group: Int, value: String)
case class ClassWithWeirdProps(devil: String, cat: Int, value: String)

class SuperKeyValue(val key: Int, val value: String) extends Serializable

class SubKeyValue(k: Int, v: String, val group: Long) extends SuperKeyValue(k, v)

case class CustomerId(id: String)
object CustomerIdConverter extends TypeConverter[String] {
  def targetTypeTag = scala.reflect.runtime.universe.typeTag[String]
  def convertPF = { case CustomerId(id) => id }
}

class TableWriterSpec extends SparkCassandraITFlatSpecBase with DefaultCluster {

  override lazy val conn = CassandraConnector(defaultConf)

  override def beforeClass {
    conn.withSessionDo { session =>
      createKeyspace(session)

      awaitAll(
        Future {
          session.execute( s"""CREATE TABLE $ks.key_value (key INT, group BIGINT, value TEXT, PRIMARY KEY (key, group))""")
        },
        Future {
          session.execute( s"""CREATE TABLE $ks.solr_query (key INT, group BIGINT, value TEXT, solr_query TEXT, PRIMARY KEY (key))""")
        },
        Future {
          session.execute( s"""CREATE TABLE $ks.nulls (key INT PRIMARY KEY, text_value TEXT, int_value INT)""")
        },
        Future {
          session.execute( s"""CREATE TABLE $ks.collections (key INT PRIMARY KEY, l list<text>, s set<text>, m map<text, text>)""")
        },
        Future {
          session.execute( s"""CREATE TABLE $ks.collections_mod (key INT PRIMARY KEY, lcol list<text>, scol set<text>, mcol map<text, text>)""")
        },
        Future {
          session.execute( s"""CREATE TABLE $ks.blobs (key INT PRIMARY KEY, b blob)""")
        },
        Future {
          session.execute( s"""CREATE TABLE $ks.counters (pkey INT, ckey INT, c1 counter, c2 counter, PRIMARY KEY (pkey, ckey))""")
        },
        Future {
          session.execute( s"""CREATE TABLE $ks.counters2 (pkey INT PRIMARY KEY, c counter)""")
        },
        Future {
          session.execute( s"""CREATE TABLE $ks.\"camelCase\" (\"primaryKey\" INT PRIMARY KEY, \"textValue\" text)""")
        },
        Future {
          session.execute( s"""CREATE TABLE $ks.single_column (pk INT PRIMARY KEY)""")
        },
        Future {
          session.execute( s"""CREATE TABLE $ks.map_tuple (a TEXT, b TEXT, c TEXT, PRIMARY KEY (a))""")
        },
        Future {
          session.execute( s"""CREATE TABLE $ks.static_test (key INT, group BIGINT, value TEXT STATIC, PRIMARY KEY (key, group))""")
        },
        Future {
          session.execute( s"""CREATE TABLE $ks.unset_test (a TEXT, b TEXT, c TEXT, PRIMARY KEY (a))""")
        },
        Future {
          session.execute( s"""CREATE TYPE $ks.address (street text, city text, zip int)""")
          session.execute( s"""CREATE TABLE $ks.udts(key INT PRIMARY KEY, name text, addr frozen<address>)""")
          session.execute( s"""CREATE TABLE $ks.udtcollection(key INT PRIMARY KEY, addrlist list<frozen<address>>, addrmap map<text, frozen<address>>)""")

        },
        Future {
          session.execute( s"""CREATE TABLE $ks.tuples (key INT PRIMARY KEY, value frozen<tuple<int, int, varchar>>)""")
        },
        Future {
          session.execute( s"""CREATE TABLE $ks.tuples2 (key INT PRIMARY KEY, value frozen<tuple<int, int, varchar>>)""")
        },
        Future {
          session.execute( s"""CREATE TYPE $ks.address2 (street text, number frozen<tuple<int, int>>)""")
          session.execute( s"""CREATE TABLE $ks.nested_tuples (key INT PRIMARY KEY, addr frozen<address2>)""")
        },
        Future {
          session.execute( s"""CREATE TABLE $ks.write_if_not_exists_test (id INT PRIMARY KEY, value TEXT)""")
        }
      )
    }
  }

  def protocolVersion = conn.withSessionDo(_.getContext.getProtocolVersion)

  private def verifyKeyValueTable(tableName: String) {
    conn.withSessionDo { session =>
      val result = session.execute(s"""SELECT * FROM $ks.""" + tableName).all().asScala
      result should have size 3
      for (row <- result) {
        Some(row.getInt("key")) should contain oneOf(1, 2, 3)
        Some(row.getLong("group")) should contain oneOf(1, 2, 3)
        Some(row.getString("value")) should contain oneOf("value1", "value2", "value3")
      }
    }
  }

  "A TableWriter" should "write RDD of tuples to an existing table" in {
    conn.withSessionDo(_.execute(s"""TRUNCATE $ks.key_value"""))
    val col = Seq((1, 1L, "value1"), (2, 2L, "value2"), (3, 3L, "value3"))
    sc.parallelize(col).saveToCassandra(ks, "key_value", SomeColumns("key", "group", "value"))
    verifyKeyValueTable("key_value")
  }

  it should "write to a table with a solr_query containing table without requiring that column" in {
    val col = Seq((1, 1L, "value1"), (2, 2L, "value2"), (3, 3L, "value3"))
    sc.parallelize(col).saveToCassandra(ks, "solr_query")
    verifyKeyValueTable("solr_query")
  }

  it should "write RDD of tuples to a new table" in {
    val pkey = ColumnDef("key", PartitionKeyColumn, IntType)
    val group = ColumnDef("group", ClusteringColumn(0), BigIntType)
    val value = ColumnDef("value", RegularColumn, TextType)
    val table = TableDef(ks, "new_kv_table", Seq(pkey), Seq(group), Seq(value))
    val rows = Seq((1, 1L, "value1"), (2, 2L, "value2"), (3, 3L, "value3"))
    sc.parallelize(rows).saveAsCassandraTableEx(table, SomeColumns("key", "group", "value"))
    verifyKeyValueTable("new_kv_table")
  }

  it should "write an RDD of PV4 Tuples to PV3 without breaking" in skipIfProtocolVersionGTE(DefaultProtocolVersion.V4){
    val rows = Seq((Byte.MinValue, Short.MinValue, java.sql.Date.valueOf("2016-08-03")))
    sc.parallelize(rows).saveAsCassandraTable(ks, "pv3")

    sc.cassandraTable[(Int, Int, String)](ks, "pv3").collect should contain
      theSameElementsAs(Seq(Byte.MinValue.toInt, Short.MinValue.toInt, "2016-08-03 00:00:00.0"))

  }

  it should "write RDD of tuples applying proper data type conversions" in {
    conn.withSessionDo(_.execute(s"""TRUNCATE $ks.key_value"""))
    val col = Seq(("1", "1", "value1"), ("2", "2", "value2"), ("3", "3", "value3"))
    sc.parallelize(col).saveToCassandra(ks, "key_value")
    verifyKeyValueTable("key_value")
  }

  it should "write RDD of case class objects" in {
    conn.withSessionDo(_.execute(s"""TRUNCATE $ks.key_value"""))
    val col = Seq(KeyValue(1, 1L, "value1"), KeyValue(2, 2L, "value2"), KeyValue(3, 3L, "value3"))
    sc.parallelize(col).saveToCassandra(ks, "key_value")
    verifyKeyValueTable("key_value")
  }

  it should "write RDD of case class objects to a new table using auto mapping" in {
    val col = Seq(KeyValue(1, 1L, "value1"), KeyValue(2, 2L, "value2"), KeyValue(3, 3L, "value3"))
    sc.parallelize(col).saveAsCassandraTable(ks, "new_kv_table_from_case_class")
    verifyKeyValueTable("new_kv_table_from_case_class")
  }

  it should "write RDD of case class objects applying proper data type conversions" in {
    conn.withSessionDo(_.execute(s"""TRUNCATE $ks.key_value"""))
    val col = Seq(
      KeyValueWithConversion("1", 1, "value1"),
      KeyValueWithConversion("2", 2, "value2"),
      KeyValueWithConversion("3", 3, "value3")
    )
    sc.parallelize(col).saveToCassandra(ks, "key_value")
    verifyKeyValueTable("key_value")
  }

  it should "write RDD of CassandraRow objects" in {
    conn.withSessionDo(_.execute(s"""TRUNCATE $ks.key_value"""))
    val col = Seq(
      CassandraRow.fromMap(Map("key" -> 1, "group" -> 1L, "value" -> "value1")),
      CassandraRow.fromMap(Map("key" -> 2, "group" -> 2L, "value" -> "value2")),
      CassandraRow.fromMap(Map("key" -> 3, "group" -> 3L, "value" -> "value3"))
    )
    sc.parallelize(col).saveToCassandra(ks, "key_value")
    verifyKeyValueTable("key_value")
  }

  it should "write RDD of CassandraRow objects applying proper data type conversions" in {
    conn.withSessionDo(_.execute(s"""TRUNCATE $ks.key_value"""))
    val col = Seq(
      CassandraRow.fromMap(Map("key" -> "1", "group" -> BigInt(1), "value" -> "value1")),
      CassandraRow.fromMap(Map("key" -> "2", "group" -> BigInt(2), "value" -> "value2")),
      CassandraRow.fromMap(Map("key" -> "3", "group" -> BigInt(3), "value" -> "value3"))
    )
    sc.parallelize(col).saveToCassandra(ks, "key_value")
    verifyKeyValueTable("key_value")
  }

  it should "pass the exception back to the driver on a write failure" in {
    val ioException = intercept[SparkException] {
      sc.parallelize[(Int, Option[Long], String)](Seq((1, None, "Hello")))
        .saveToCassandra(ks, "key_value")
    }

    ioException.getMessage should include ("Invalid null value")
  }

  it should "write to a table with only partition key and static columns without clustering" in {
    sc.parallelize(1 to 10)
      .map( x => KV(x, x.toString))
      .saveToCassandra(ks, "static_test", SomeColumns("key", "value"))

    sc.cassandraTable(ks, "static_test").count should be (10)
  }

  it should "throw an exception if writing to a table with only pk and non-static columns" in {
    val ex  = intercept[IllegalArgumentException] {
      sc.parallelize(1 to 10)
        .map(x => KV(x, x.toString))
        .saveToCassandra(ks, "key_value", SomeColumns("key", "value"))
    }
    val message = ex.getMessage
    message should include ("primary key")
    message should include ("group")
  }

  it should "ignore unset inserts" in {
    conn.withSessionDo {
      _.execute(
        s"""INSERT into $ks.unset_test (A, B, C) VALUES ('Original', 'Original', 'Original')""")
    }

    sc.parallelize(Seq(("Original", Unset, "New"))).saveToCassandra(ks, "unset_test")
    val result = sc.cassandraTable[(String, Option[String], Option[String])](ks, "unset_test")
      .collect
    if (protocolVersion.getCode >= DefaultProtocolVersion.V4.getCode) {
      result(0) should be(("Original", Some("Original"), Some("New")))
    } else {
      result(0) should be(("Original", None, Some("New")))
    }
  }

  it should "ignore CassandraOptions set to UNSET" in {
    conn.withSessionDo {
      _.execute(
        s"""INSERT into $ks.unset_test (A, B, C) VALUES ('Original', 'Original','Original')"""
      )
    }
    sc.parallelize(Seq(("Original", CassandraOption.Unset, "New")))
      .saveToCassandra(ks, "unset_test")
    val result = sc.cassandraTable[(String, Option[String], Option[String])](ks, "unset_test")
      .collect
    if (protocolVersion.getCode >= DefaultProtocolVersion.V4.getCode) {
      result(0) should be(("Original", Some("Original"), Some("New")))
    } else {
      result(0) should be(("Original", None, Some("New")))
    }
  }

  it should "delete with Cassandra Options set to Null" in {
    conn.withSessionDo {
      _.execute(
        s"""INSERT into $ks.unset_test (A, B, C) VALUES ('Original', 'Original', 'Original')"""
      )
    }
    sc.parallelize(Seq(("Original", CassandraOption.Null, "New"))).saveToCassandra(ks, "unset_test")
    val result = sc.cassandraTable[(String, Option[String], Option[String])](ks, "unset_test")
      .collect
    result(0) should be(("Original", None, Some("New")))
  }

  it should "write RDD of tuples to a table with camel case column names" in {
    val col = Seq((1, "value1"), (2, "value2"), (3, "value3"))
    sc.parallelize(col).saveToCassandra(ks, "camelCase", SomeColumns("primaryKey", "textValue"))
    conn.withSessionDo { session =>
      val result = session.execute(s"""SELECT * FROM $ks."camelCase"""").all().asScala
      result should have size 3
      for (row <- result) {
        Some(row.getInt(0)) should contain oneOf(1, 2, 3)
        Some(row.getString(1)) should contain oneOf("value1", "value2", "value3")
      }
    }
  }

  it should "write empty values" in {
    conn.withSessionDo(_.execute(s"""TRUNCATE $ks.key_value"""))
    val col = Seq((1, 1L, None))
    sc.parallelize(col).saveToCassandra(ks, "key_value", SomeColumns("key", "group", "value"))
    conn.withSessionDo { session =>
      val result = session.execute(s"""SELECT * FROM $ks.key_value""").all().asScala
      result should have size 1
      for (row <- result) {
        row.getString(2) should be (null)
      }
    }
  }

  it should "write null values" in {
    val key = 1.asInstanceOf[AnyRef]
    val row = new CassandraRow(
      CassandraRowMetadata(IndexedSeq("key", "text_value", "int_value")),
      IndexedSeq(key, null, null))

    sc.parallelize(Seq(row)).saveToCassandra(ks, "nulls")
    conn.withSessionDo { session =>
      val result = session.execute(s"""SELECT * FROM $ks.nulls""").all().asScala
      result should have size 1
      for (r <- result) {
        r.getInt(0) shouldBe key
        r.isNull(1) shouldBe true
        r.isNull(2) shouldBe true
      }
    }
  }

  it should "write only specific column data if ColumnNames is passed as 'columnNames'" in {
    conn.withSessionDo(_.execute(s"""TRUNCATE $ks.key_value"""))
    val col = Seq((1, 1L, None))
    sc.parallelize(col).saveToCassandra(ks, "key_value", SomeColumns("key", "group"))
    conn.withSessionDo { session =>
      val result = session.execute(s"""SELECT * FROM $ks.key_value""").all().asScala
      result should have size 1
      for (row <- result) {
        row.getInt(0) should be (1)
        row.getString(2) should be (null)
      }
    }
  }

  it should "distinguish (deprecated) implicit `seqToSomeColumns`" in {
    conn.withSessionDo(_.execute(s"""TRUNCATE $ks.key_value"""))
    val col = Seq((2, 1L, None))
    sc.parallelize(col).saveToCassandra(ks, "key_value", SomeColumns("key", "group"))
    conn.withSessionDo { session =>
      val result = session.execute(s"""SELECT * FROM $ks.key_value""").all().asScala
      result should have size 1
      for (row <- result) {
        row.getInt(0) should be (2)
        row.getString(2) should be (null)
      }
    }
  }

  it should "write collections" in {
    val col = Seq(
      (1, Vector("item1", "item2"), Set("item1", "item2"), Map("key1" -> "value1", "key2" -> "value2")),
      (2, Vector.empty[String], Set.empty[String], Map.empty[String, String]))
    sc.parallelize(col).saveToCassandra(ks, "collections", SomeColumns("key", "l", "s", "m"))

    conn.withSessionDo { session =>
      val result = session.execute(s"""SELECT * FROM $ks.collections""").all().asScala
      result should have size 2
      val rows = result.groupBy(_.getInt(0)).mapValues(_.head)
      val row0 = rows(1)
      val row1 = rows(2)
      row0.getList("l", classOf[String]).asScala.toSeq shouldEqual Seq("item1", "item2")
      row0.getSet("s", classOf[String]).asScala.toSeq shouldEqual Seq("item1", "item2")
      row0.getMap("m", classOf[String], classOf[String]).asScala.toMap shouldEqual Map("key1" -> "value1", "key2" -> "value2")
      row1.isNull("l") shouldEqual true
      row1.isNull("m") shouldEqual true
      row1.isNull("s") shouldEqual true
    }
  }

  it should "write blobs" in {
    val col = Seq((1, Some(Array[Byte](0, 1, 2, 3))), (2, None))
    sc.parallelize(col).saveToCassandra(ks, "blobs", SomeColumns("key", "b"))
    conn.withSessionDo { session =>
      val result = session.execute(s"""SELECT * FROM $ks.blobs""").all().asScala
      result should have size 2
      val rows = result.groupBy(_.getInt(0)).mapValues(_.head)
      val row0 = rows(1)
      val row1 = rows(2)
      row0.getByteBuffer("b").remaining shouldEqual 4
      row1.isNull("b") shouldEqual true
    }
  }

  it should "increment and decrement counters" in {
    val col1 = Seq((0, 0, 1, 1))
    sc.parallelize(col1).saveToCassandra(ks, "counters", SomeColumns("pkey", "ckey", "c1", "c2"))
    conn.withSessionDo { session =>
      val result = session.execute(s"""SELECT * FROM $ks.counters""").one()
      result.getLong("c1") shouldEqual 1L
      result.getLong("c2") shouldEqual 1L
    }
    val col2 = Seq((0, 0, 1))
    sc.parallelize(col1).saveToCassandra(ks, "counters", SomeColumns("pkey", "ckey", "c2"))
    conn.withSessionDo { session =>
      val result = session.execute(s"""SELECT * FROM $ks.counters""").one()
      result.getLong("c1") shouldEqual 1L
      result.getLong("c2") shouldEqual 2L
    }
  }

  it should "increment and decrement counters in batches" in {
    val rowCount = 10000
    val col = for (i <- 1 to rowCount) yield (i, 1)
    sc.parallelize(col).saveToCassandra(ks, "counters2", SomeColumns("pkey", "c"))
    sc.cassandraTable(ks, "counters2").cassandraCount() should be(rowCount)
  }

  it should "write values of user-defined classes" in {
    conn.withSessionDo(_.execute(s"""TRUNCATE $ks.key_value"""))
    TypeConverter.registerConverter(CustomerIdConverter)
    try {
      val col = Seq((1, 1L, CustomerId("foo")))
      sc.parallelize(col).saveToCassandra(ks, "key_value", SomeColumns("key", "group", "value"))

      conn.withSessionDo { session =>
        val result = session.execute(s"""SELECT * FROM $ks.key_value""").all().asScala
        result should have size 1
        for (row <- result)
          row.getString(2) shouldEqual "foo"
      }
    } finally {
      TypeConverter.unregisterConverter(CustomerIdConverter)
    }
  }

  it should "write values of user-defined-types from case classes into Cassandra" in {
    val address = Address(city = "Oakland", zip = 90210, street = "Broadway")
    val col = Seq((1, "Joe", address))
    sc.parallelize(col).saveToCassandra(ks, "udts", SomeColumns("key", "name", "addr"))

    conn.withSessionDo { session =>
      val result = session.execute(s"""SELECT key, name, addr FROM $ks.udts""").all().asScala
      result should have size 1
      for (row <- result) {
        row.getInt(0) shouldEqual 1
        row.getString(1) shouldEqual "Joe"
        row.getUdtValue(2).getString("city") shouldEqual "Oakland"
        row.getUdtValue(2).getInt("zip") shouldEqual 90210
      }
    }
  }

  it should "write values of user-defined-types in Cassandra Collections" in {
    val address = Address(city = "New Orleans", zip = 20401, street = "Magazine")
    val rows = Seq((1, Seq(address), Map("home" -> address)))
    sc.parallelize(rows).saveToCassandra(ks, "udtcollection")
    val result = sc.cassandraTable[(Int, Seq[Address], Map[String, Address])](ks, "udtcollection").collect
    result should contain theSameElementsAs (rows)
  }

  it should "write values of user-defined-types in Cassandra" in {
    val address = UDTValue.fromMap(Map("city" -> "Warsaw", "zip" -> 10000, "street" -> "Marszałkowska"))
    val col = Seq((1, "Joe", address))
    sc.parallelize(col).saveToCassandra(ks, "udts", SomeColumns("key", "name", "addr"))

    conn.withSessionDo { session =>
      val result = session.execute(s"""SELECT key, name, addr FROM $ks.udts""").all().asScala
      result should have size 1
      for (row <- result) {
        row.getInt(0) shouldEqual 1
        row.getString(1) shouldEqual "Joe"
        row.getUdtValue(2).getString("city") shouldEqual "Warsaw"
        row.getUdtValue(2).getInt("zip") shouldEqual 10000
      }
    }
  }

  it should "write null values of user-defined-types in Cassandra" in {
    val address = null
    val col = Seq((1, "Joe", address))
    sc.parallelize(col).saveToCassandra(ks, "udts", SomeColumns("key", "name", "addr"))

    conn.withSessionDo { session =>
      val result = session.execute(s"""SELECT key, name, addr FROM "$ks".udts""").all().asScala
      result should have size 1
      for (row <- result) {
        row.getInt(0) shouldEqual 1
        row.getString(1) shouldEqual "Joe"
        row.getUdtValue(2) should be (null)
      }
    }
  }

  it should "write values of user-defined-types with null fields in Cassandra" in {
    val address = UDTValue.fromMap(Map("city" -> "Warsaw", "zip" -> 10000, "street" -> null))
    val col = Seq((1, "Joe", address))
    sc.parallelize(col).saveToCassandra(ks, "udts", SomeColumns("key", "name", "addr"))

    conn.withSessionDo { session =>
      val result = session.execute(s"""SELECT key, name, addr FROM "$ks".udts""").all().asScala
      result should have size 1
      for (row <- result) {
        row.getInt(0) shouldEqual 1
        row.getString(1) shouldEqual "Joe"
        row.getUdtValue(2).getString("city") shouldEqual "Warsaw"
        row.getUdtValue(2).getString("street") should be (null)
        row.getUdtValue(2).getInt("zip") shouldEqual 10000
      }
    }
  }


  it should "write values of TupleValue type" in {
    val tuple = TupleValue(1, 2, "three")
    val col = Seq((1, tuple))
    sc.parallelize(col).saveToCassandra(ks, "tuples", SomeColumns("key", "value"))

    conn.withSessionDo { session =>
      val result = session.execute(s"""SELECT key, value FROM $ks.tuples""").all().asScala
      result should have size 1
      for (row <- result) {
        row.getInt(0) shouldEqual 1
        row.getTupleValue(1).getInt(0) shouldEqual 1
        row.getTupleValue(1).getInt(1) shouldEqual 2
        row.getTupleValue(1).getString(2) shouldEqual "three"
      }
    }
  }

  it should "write column values of tuple type given as Scala tuples" in {
    val tuple = (1, 2, "three")  // Scala tuple
    val col = Seq((1, tuple))
    sc.parallelize(col).saveToCassandra(ks, "tuples", SomeColumns("key", "value"))

    conn.withSessionDo { session =>
      val result = session.execute(s"""SELECT key, value FROM $ks.tuples""").all().asScala
      result should have size 1
      for (row <- result) {
        row.getInt(0) shouldEqual 1
        row.getTupleValue(1).getInt(0) shouldEqual 1
        row.getTupleValue(1).getInt(1) shouldEqual 2
        row.getTupleValue(1).getString(2) shouldEqual "three"
      }
    }
  }

  it should "write Scala tuples nested in UDTValues" in {
    val number = (1, 2)
    val address = UDTValue.fromMap(Map("street" -> "foo", "number" -> number))
    val col = Seq((1, address))
    sc.parallelize(col).saveToCassandra(ks, "nested_tuples", SomeColumns("key", "addr"))

    conn.withSessionDo { session =>
      val result = session.execute(s"""SELECT key, addr FROM $ks.nested_tuples""").all().asScala
      result should have size 1
      for (row <- result) {
        row.getInt(0) shouldEqual 1
        row.getUdtValue(1).getString(0) shouldEqual "foo"
        row.getUdtValue(1).getTupleValue(1).getInt(0) shouldEqual 1
        row.getUdtValue(1).getTupleValue(1).getInt(1) shouldEqual 2
      }
    }
  }

  it should "convert components in nested Scala tuples to proper types" in {
    val number = ("1", "2")  // Strings, but should be Ints
    val address = UDTValue.fromMap(Map("street" -> "foo", "number" -> number))
    val col = Seq((1, address))
    sc.parallelize(col).saveToCassandra(ks, "nested_tuples", SomeColumns("key", "addr"))

    conn.withSessionDo { session =>
      val result = session.execute(s"""SELECT key, addr FROM $ks.nested_tuples""").all().asScala
      for (row <- result) {
        row.getUdtValue(1).getTupleValue(1).getInt(0) shouldEqual 1
        row.getUdtValue(1).getTupleValue(1).getInt(1) shouldEqual 2
      }
    }
  }

  it should "write to single-column tables" in {
    val col = Seq(1, 2, 3, 4, 5).map(Tuple1.apply)
    sc.parallelize(col).saveToCassandra(ks, "single_column", SomeColumns("pk"))
    conn.withSessionDo { session =>
      val result = session.execute(s"""SELECT * FROM $ks.single_column""").all().asScala
      result should have size 5
      result.map(_.getInt(0)).toSet should be (Set(1, 2, 3, 4, 5))
    }
  }

  it should "throw IOException if table is not found" in {
    val col = Seq(("1", "1", "value1"), ("2", "2", "value2"), ("3", "3", "value3"))
    intercept[IOException] {
      sc.parallelize(col).saveToCassandra(ks, "unknown_table")
    }
  }

  it should "write RDD of case class objects with default TTL" in {
    conn.withSessionDo(_.execute(s"""TRUNCATE $ks.key_value"""))
    val col = Seq(KeyValue(1, 1L, "value1"), KeyValue(2, 2L, "value2"), KeyValue(3, 3L, "value3"))
    sc.parallelize(col).saveToCassandra(ks, "key_value", writeConf = WriteConf(ttl = TTLOption.constant(100)))

    verifyKeyValueTable("key_value")

    conn.withSessionDo { session =>
      val result = session.execute(s"""SELECT TTL(value) FROM $ks.key_value""").all().asScala
      result should have size 3
      result.foreach(_.getInt(0) should be > 50)
      result.foreach(_.getInt(0) should be <= 100)
    }
  }

  it should "write RDD of case class objects with default timestamp" in {
    conn.withSessionDo(_.execute(s"""TRUNCATE $ks.key_value"""))
    val col = Seq(KeyValue(1, 1L, "value1"), KeyValue(2, 2L, "value2"), KeyValue(3, 3L, "value3"))
    val ts = System.currentTimeMillis() - 1000L
    sc.parallelize(col).saveToCassandra(ks, "key_value", writeConf = WriteConf(timestamp = TimestampOption.constant(ts * 1000L)))

    verifyKeyValueTable("key_value")

    conn.withSessionDo { session =>
      val result = session.execute(s"""SELECT WRITETIME(value) FROM $ks.key_value""").all().asScala
      result should have size 3
      result.foreach(_.getLong(0) should be (ts * 1000L))
    }
  }

  it should "write RDD of case class objects with per-row TTL" in {
    conn.withSessionDo(_.execute(s"""TRUNCATE $ks.key_value"""))
    val col = Seq(KeyValueWithTTL(1, 1L, "value1", 100), KeyValueWithTTL(2, 2L, "value2", 200), KeyValueWithTTL(3, 3L, "value3", 300))
    sc.parallelize(col).saveToCassandra(ks, "key_value", writeConf = WriteConf(ttl = TTLOption.perRow("ttl")))

    verifyKeyValueTable("key_value")

    conn.withSessionDo { session =>
      val result = session.execute(s"""SELECT key, TTL(value) FROM $ks.key_value""").all().asScala
      result should have size 3
      result.foreach(row => {
        row.getInt(1) should be > (100 * row.getInt(0) - 50)
        row.getInt(1) should be <= (100 * row.getInt(0))
      })
    }
  }

  it should "write RDD of case class objects with per-row timestamp" in {
    conn.withSessionDo(_.execute(s"""TRUNCATE $ks.key_value"""))
    val ts = System.currentTimeMillis() - 1000L
    val col = Seq(KeyValueWithTimestamp(1, 1L, "value1", ts * 1000L + 100L), KeyValueWithTimestamp(2, 2L, "value2", ts * 1000L + 200L), KeyValueWithTimestamp(3, 3L, "value3", ts * 1000L + 300L))
    sc.parallelize(col).saveToCassandra(ks, "key_value", writeConf = WriteConf(timestamp = TimestampOption.perRow("timestamp")))

    verifyKeyValueTable("key_value")

    conn.withSessionDo { session =>
      val result = session.execute(s"""SELECT key, WRITETIME(value) FROM $ks.key_value""").all().asScala
      result should have size 3
      result.foreach(row => {
        row.getLong(1) should be (ts * 1000L + row.getInt(0) * 100L)
      })
    }
  }

  it should "write RDD of case class objects with per-row TTL with custom mapping" in {
    conn.withSessionDo(_.execute(s"""TRUNCATE $ks.key_value"""))
    val col = Seq(KeyValueWithTTL(1, 1L, "value1", 100), KeyValueWithTTL(2, 2L, "value2", 200), KeyValueWithTTL(3, 3L, "value3", 300))
    implicit val mapping = new DefaultColumnMapper[KeyValueWithTTL](Map("ttl" -> "ttl_placeholder"))

    sc.parallelize(col).saveToCassandra(ks, "key_value",
      writeConf = WriteConf(ttl = TTLOption.perRow("ttl_placeholder")))

    verifyKeyValueTable("key_value")

    conn.withSessionDo { session =>
      val result = session.execute(s"""SELECT key, TTL(value) FROM $ks.key_value""").all().asScala
      result should have size 3
      result.foreach(row => {
        row.getInt(1) should be > (100 * row.getInt(0) - 50)
        row.getInt(1) should be <= (100 * row.getInt(0))
      })
    }
  }

  it should "write RDD of case class objects with per-row timestamp with custom mapping" in {
    conn.withSessionDo(_.execute(s"""TRUNCATE $ks.key_value"""))
    val ts = System.currentTimeMillis() - 1000L
    val col = Seq(KeyValueWithTimestamp(1, 1L, "value1", ts * 1000L + 100L), KeyValueWithTimestamp(2, 2L, "value2", ts * 1000L + 200L), KeyValueWithTimestamp(3, 3L, "value3", ts * 1000L + 300L))

    implicit val mapper =
      new DefaultColumnMapper[KeyValueWithTimestamp](Map("timestamp" -> "timestamp_placeholder"))

    sc.parallelize(col).saveToCassandra(ks, "key_value",
      writeConf = WriteConf(timestamp = TimestampOption.perRow("timestamp_placeholder")))

    verifyKeyValueTable("key_value")

    conn.withSessionDo { session =>
      val result = session.execute(s"""SELECT key, WRITETIME(value) FROM $ks.key_value""").all().asScala
      result should have size 3
      result.foreach(row => {
        row.getLong(1) should be (ts * 1000L + row.getInt(0) * 100L)
      })
    }
  }

  it should "write RDD of case class objects applying proper data type conversions and aliases" in {
    conn.withSessionDo(_.execute(s"""TRUNCATE $ks.key_value"""))
    val col = Seq(
      ClassWithWeirdProps("1", 1, "value1"),
      ClassWithWeirdProps("2", 2, "value2"),
      ClassWithWeirdProps("3", 3, "value3")
    )
    sc.parallelize(col).saveToCassandra(ks, "key_value", columns = SomeColumns(
      "key" as "devil", "group" as "cat", "value"
    ))
    verifyKeyValueTable("key_value")
  }

  it should "write an RDD of tuples mapped to different ordering of fields" in {
    val col = Seq (("x","a","b"))
    sc.parallelize(col)
      .saveToCassandra(ks,
        "map_tuple",
        SomeColumns(("a" as "_2"), ("c" as "_1")))
    conn.withSessionDo { session =>
      val result = session.execute(s"""SELECT * FROM $ks.map_tuple""").all().asScala
      result should have size 1
      val row = result(0)
      row.getString("a") should be ("a")
      row.getString("c") should be ("x")
    }
  }

  it should "write an RDD of tuples with only some fields aliased" in {
     val col = Seq (("c","a","b"))
    sc.parallelize(col)
      .saveToCassandra(ks,
        "map_tuple",
        SomeColumns(("a" as "_2"),("b" as "_3"), ("c" as "_1")))
    conn.withSessionDo { session =>
      val result = session.execute(s"""SELECT * FROM $ks.map_tuple""").all().asScala
      result should have size 1
      val row = result(0)
      row.getString("a") should be ("a")
      row.getString("b") should be ("b")
      row.getString("c") should be ("c")
    }
  }

  it should "throw an exception if you try to alias tuple fields which don't exist" in {
    val col = Seq (("c"))
    intercept[IllegalArgumentException] {
      sc.parallelize(col).saveToCassandra(ks,
        "map_tuple",
        SomeColumns(("a" as "_2"),("b" as "_3"), ("c" as "_1")))
    }
  }

  it should "throw an exception when aliasing some tuple fields explicitly and others implicitly" in {
    val col = Seq (("c","a"))
    intercept[IllegalArgumentException] {
      sc.parallelize(col).saveToCassandra(ks,
        "map_tuple",
        SomeColumns(("a" as "_2"),("b")))
    }
  }

  it should "write RDD of objects with inherited fields" in {
    conn.withSessionDo(_.execute(s"""TRUNCATE $ks.key_value"""))
    val col = Seq(
      new SubKeyValue(1, "value1", 1L),
      new SubKeyValue(2, "value2", 2L),
      new SubKeyValue(3, "value3", 3L)
    )
    sc.parallelize(col).saveToCassandra(ks, "key_value")
    verifyKeyValueTable("key_value")
  }

  it should "write RDD of case class objects with transient fields" in {
    conn.withSessionDo(_.execute(s"""TRUNCATE $ks.key_value"""))
    val col = Seq(KeyValueWithTransient(1, 1L, "value1", "a"), KeyValueWithTransient(2, 2L, "value2", "b"), KeyValueWithTransient(3, 3L, "value3", "c"))
    sc.parallelize(col).saveToCassandra(ks, "key_value")
    verifyKeyValueTable("key_value")
  }

  it should "be able to append and prepend elements to a C* list" in {

    val listElements = sc.parallelize(Seq(
      (1, Vector("One")),
      (1, Vector("Two")),
      (1, Vector("Three"))))

    val prependElements = sc.parallelize(Seq(
      (1, Vector("PrependOne")),
      (1, Vector("PrependTwo")),
      (1, Vector("PrependThree"))))

    listElements.saveToCassandra(ks, "collections_mod", SomeColumns("key", "lcol".append))
    prependElements.saveToCassandra(ks, "collections_mod", SomeColumns("key", "lcol".prepend))

    val testList = sc.cassandraTable[(Seq[String])](ks, "collections_mod")
      .where("key = 1")
      .select("lcol").take(1)(0)
    testList.take(3) should contain allOf("PrependOne", "PrependTwo", "PrependThree")
    testList.drop(3) should contain allOf("One", "Two", "Three")
  }

  it should "be able to remove elements from a C* list " in {
    val listElements = sc.parallelize(Seq(
      (2, Vector("One")),
      (2, Vector("Two")),
      (2, Vector("Three"))))
    listElements.saveToCassandra(ks, "collections_mod", SomeColumns("key", "lcol".append))

    sc.parallelize(Seq(
      (2, Vector("Two")),
      (2, Vector("Three"))))
      .saveToCassandra(ks, "collections_mod", SomeColumns("key", "lcol".remove))

    val testList = sc.cassandraTable[(Seq[String])](ks, "collections_mod")
      .where("key = 2")
      .select("lcol").take(1)(0)
    testList should contain noneOf("Two", "Three")
    testList should contain("One")
  }

  it should "be able to add elements to a C* set " in {
    val setElements = sc.parallelize(Seq(
      (3, Set("One")),
      (3, Set("Two")),
      (3, Set("Three"))))
    setElements.saveToCassandra(ks, "collections_mod", SomeColumns("key", "scol".append))
    val testSet = sc.cassandraTable[(Set[String])](ks, "collections_mod")
      .where("key = 3")
      .select("scol").take(1)(0)

    testSet should contain allOf("One", "Two", "Three")
  }

  it should "be able to remove elements from a C* set" in {
    val setElements = sc.parallelize(Seq(
      (4, Set("One")),
      (4, Set("Two")),
      (4, Set("Three"))))
    setElements.saveToCassandra(ks, "collections_mod", SomeColumns("key", "scol".append))

    sc.parallelize(Seq((4, Set("Two")), (4, Set("Three"))))
      .saveToCassandra(ks, "collections_mod", SomeColumns("key", "scol".remove))

    val testSet = sc.cassandraTable[(Set[String])](ks, "collections_mod")
      .where("key = 4")
      .select("scol").take(1)(0)

    testSet should contain noneOf("Two", "Three")
    testSet should contain("One")
  }

  it should "be able to add key value pairs to a C* map" in {
    val setElements = sc.parallelize(Seq(
      (5, Map("One" -> "One")),
      (5, Map("Two" -> "Two")),
      (5, Map("Three" -> "Three"))))
    setElements.saveToCassandra(ks, "collections_mod", SomeColumns("key", "mcol".append))

    val testMap = sc.cassandraTable[(Map[String, String])](ks, "collections_mod")
      .where("key = 5")
      .select("mcol").take(1)(0)

    testMap.toSeq should contain allOf(("One", "One"), ("Two", "Two"), ("Three", "Three"))
  }

  it should "throw an exception if you try to apply a collection behavior to a normal column" in {
    conn.withSessionDo(_.execute(s"""TRUNCATE $ks.key_value"""))
    val col = Seq((1, 1L, "value1"), (2, 2L, "value2"), (3, 3L, "value3"))
    val e = intercept[IllegalArgumentException] {
      sc.parallelize(col).saveToCassandra(ks, "key_value", SomeColumns("key", "group".overwrite, "value"))
    }
    e.getMessage should include("group")
  }

  it should "throw an exception if you try to remove values from a map" in {
    val setElements = sc.parallelize(Seq(
      (5, Map("One" -> "One")),
      (5, Map("Two" -> "Two")),
      (5, Map("Three" -> "Three"))))
    val e = intercept[IllegalArgumentException] {
      setElements.saveToCassandra(ks, "collections_mod", SomeColumns("key", "mcol".remove))
    }
    e.getMessage should include("mcol")
  }

  it should "throw an exception if you prepend anything but a list" in {
    val setElements = sc.parallelize(Seq(
      (5, Map("One" -> "One"), Set("One"))))
    val e = intercept[IllegalArgumentException] {
      setElements.saveToCassandra(ks, "collections_mod", SomeColumns("key", "mcol".prepend,
        "scol".prepend))
    }
    e.getMessage should include("mcol")
    e.getMessage should include("scol")
  }

  it should "insert and not overwrite existing keys when ifNotExists is true" in {
    conn.withSessionDo { session =>
      session.execute(s"""TRUNCATE $ks.write_if_not_exists_test""")
      session.execute(s"""INSERT INTO $ks.write_if_not_exists_test (id, value) VALUES (1, 'old')""")
    }

    sc.parallelize(Seq((1, "new"), (2, "new"))).saveToCassandra(ks, "write_if_not_exists_test",
      writeConf = WriteConf(ifNotExists = true))

    val results = sc.cassandraTable[(Int, String)](ks, "write_if_not_exists_test")
      .select("id", "value").collect()
    results should  contain theSameElementsAs  Seq((1, "old"), (2, "new"))
  }

  it should "insert and overwrite existing keys when ifNotExists is false or with default values" in {
    conn.withSessionDo { session =>
      session.execute(s"""TRUNCATE $ks.write_if_not_exists_test""")
      session.execute(s"""INSERT INTO $ks.write_if_not_exists_test (id, value) VALUES (1, 'old')""")
    }

    sc.parallelize(Seq((1, "new"), (2, "new"))).saveToCassandra(ks, "write_if_not_exists_test")

    val results = sc.cassandraTable[(Int, String)](ks, "write_if_not_exists_test")
      .select("id", "value").collect()
    results should contain theSameElementsAs Seq((1, "new"), (2, "new"))
  }

  "Idempotent Queries" should "not be used with list append" in {
    val listAppend = TableWriter(conn, ks, "collections_mod", SomeColumns("key", "lcol".append), WriteConf.fromSparkConf(sc.getConf))
    listAppend.isIdempotent should be (false)
  }

  it should "not be used with list prepend" in {
    val listPrepend = TableWriter(conn, ks, "collections_mod", SomeColumns("key", "lcol".prepend), WriteConf.fromSparkConf(sc.getConf))
    listPrepend.isIdempotent should be (false)
  }

  it should "not be used with counter modifications" in {
    val counterUpdate = TableWriter(conn, ks, "counters", SomeColumns("pkey", "ckey", "c1", "c2"), WriteConf.fromSparkConf(sc.getConf))
    counterUpdate.isIdempotent should be (false)
  }

  it should "be used with ifNotExists updates" in {
    val ifNotExists = TableWriter(conn, ks, "write_if_not_exists_test", AllColumns, writeConf = WriteConf(ifNotExists = true))
    ifNotExists.isIdempotent should be (true)
  }

  it should "be used with generic writes" in {
    val genericWrite = TableWriter(conn, ks, "key_value", AllColumns, WriteConf.fromSparkConf(sc.getConf))
    genericWrite.isIdempotent should be (true)
  }

  it should "be used with collections that aren't lists" in {
    val listOverwrite = TableWriter(conn, ks, "collections_mod", SomeColumns("key", "lcol".overwrite), WriteConf.fromSparkConf(sc.getConf))
    listOverwrite.isIdempotent should be (true)
    val setOverwrite = TableWriter(conn, ks, "collections_mod", SomeColumns("key", "scol".overwrite), WriteConf.fromSparkConf(sc.getConf))
    setOverwrite.isIdempotent should be (true)
    val mapOverwrite = TableWriter(conn, ks, "collections_mod", SomeColumns("key", "mcol".overwrite), WriteConf.fromSparkConf(sc.getConf))
    mapOverwrite.isIdempotent should be (true)
  }

}
