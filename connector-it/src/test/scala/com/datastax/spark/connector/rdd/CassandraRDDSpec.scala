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

package com.datastax.spark.connector.rdd

import java.io.IOException
import java.time.{Instant, LocalDate, ZoneId, ZonedDateTime}
import java.util.Date
import com.datastax.oss.driver.api.core.DefaultProtocolVersion._
import com.datastax.oss.driver.api.core.Version.V4_0_0
import com.datastax.oss.driver.api.core.config.DefaultDriverOption
import com.datastax.oss.driver.api.core.cql.SimpleStatement
import com.datastax.oss.driver.api.core.cql.SimpleStatement._
import com.datastax.spark.connector._
import com.datastax.spark.connector.ccm.CcmConfig.{DSE_V5_1_0, DSE_V6_7_0, V3_6_0}
import com.datastax.spark.connector.cluster.DefaultCluster
import com.datastax.spark.connector.cql.{CassandraConnector, CassandraConnectorConf}
import com.datastax.spark.connector.mapper.{DefaultColumnMapper, JavaBeanColumnMapper, JavaTestBean, JavaTestUDTBean}
import com.datastax.spark.connector.rdd.partitioner.dht.TokenFactory
import com.datastax.spark.connector.types.{CassandraOption, TypeConverter}
import com.datastax.spark.connector.util.RuntimeUtil
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException

import scala.concurrent.Future
import scala.jdk.CollectionConverters._
import scala.reflect.runtime.universe.typeTag

case class KeyValue(key: Int, group: Long, value: String)
case class KeyValueWithConversion(key: String, group: Int, value: Long)
case class CustomerId(id: String)
case class Key(key: Int)
case class KeyGroup(key: Int, group: Int)
case class Value(value: String)
case class WriteTimeClass(id: Int, value: String, writeTimeOfValue: Long)
case class TTLClass(id: Int, value: String, ttlOfValue: Int)
case class ClassWithWeirdProps(devil: Int, cat: Long, value: String)


class MutableKeyValue(var key: Int, var group: Long) extends Serializable {
  var value: String = null
}

class MutableKeyValueWithConversion(var key: String, var group: Int) extends Serializable {
  var value: Long = 0L
}

class SuperKeyValue extends Serializable {
  var key: Int = 0
  var value: String = ""
}

class SubKeyValue extends SuperKeyValue {
  var group: Long = 0L
}

case class Address(street: String, city: String, zip: Int)
case class ClassWithUDT(key: Int, name: String, addr: Address)
case class ClassWithTuple(key: Int, value: (Int, String))
case class ClassWithSmallInt(key: Int, value: Short)

case class TypeWithNestedTuple(id: Int, t: (Int, (String, Double)))
case class TypeWithTupleSetter(id: Int) {
  var t: (Int, (String, Double)) = null
}

class CassandraRDDSpec extends SparkCassandraITFlatSpecBase with DefaultCluster {

  override lazy val conn = CassandraConnector(defaultConf)
  val bigTableRowCount = 100000

  conn.withSessionDo { session =>
    createKeyspace(session)

    awaitAll(
      Future {
        skipIfProtocolVersionLT(V4) {
          markup(s"Making PV4 Types")
          executor.execute(newInstance( s"""CREATE TABLE $ks.short_value (key INT, value SMALLINT, PRIMARY KEY (key))"""))
          executor.execute(newInstance( s"""INSERT INTO $ks.short_value (key, value) VALUES (1,100)"""))
          executor.execute(newInstance( s"""INSERT INTO $ks.short_value (key, value) VALUES (2,200)"""))
          executor.execute(newInstance( s"""INSERT INTO $ks.short_value (key, value) VALUES (3,300)"""))
        }
      },

      Future {
        executor.execute(newInstance( s"""CREATE TABLE $ks.key_value (key INT, group BIGINT, value TEXT, PRIMARY KEY (key, group))"""))
        executor.execute(newInstance( s"""INSERT INTO $ks.key_value (key, group, value) VALUES (1, 100, '0001')"""))
        executor.execute(newInstance( s"""INSERT INTO $ks.key_value (key, group, value) VALUES (2, 100, '0002')"""))
        executor.execute(newInstance( s"""INSERT INTO $ks.key_value (key, group, value) VALUES (3, 300, '0003')"""))
      },

      Future {
        executor.execute(newInstance( s"""CREATE TABLE $ks.simple_kv (key INT, value TEXT, PRIMARY KEY (key))"""))
        executor.execute(newInstance( s"""INSERT INTO $ks.simple_kv (key, value) VALUES (1, '0001')"""))
        executor.execute(newInstance( s"""INSERT INTO $ks.simple_kv (key, value) VALUES (2, '0002')"""))
        executor.execute(newInstance( s"""INSERT INTO $ks.simple_kv (key, value) VALUES (3, '0003')"""))
      },

      Future {
        executor.execute(newInstance( s"""CREATE TABLE $ks.collections (key INT PRIMARY KEY, l list<text>, s set<text>, m map<text, text>)"""))
        executor.execute(newInstance( s"""INSERT INTO $ks.collections (key, l, s, m) VALUES (1, ['item1', 'item2'], {'item1', 'item2'}, {'key1': 'value1', 'key2': 'value2'})"""))
        executor.execute(newInstance( s"""INSERT INTO $ks.collections (key, l, s, m) VALUES (2, null, null, null)"""))
      },

      Future {
        executor.execute(newInstance( s"""CREATE TABLE $ks.blobs (key INT PRIMARY KEY, b blob)"""))
        executor.execute(newInstance( s"""INSERT INTO $ks.blobs (key, b) VALUES (1, 0x0102030405060708090a0b0c)"""))
        executor.execute(newInstance( s"""INSERT INTO $ks.blobs (key, b) VALUES (2, null)"""))
      },

      Future {
        executor.execute(newInstance( s"""CREATE TABLE $ks.composite_key (key_c1 INT, key_c2 INT, group INT, value TEXT, PRIMARY KEY ((key_c1, key_c2), group))"""))
        executor.execute(newInstance( s"""INSERT INTO $ks.composite_key (key_c1, key_c2, group, value) VALUES (1, 1, 1, 'value1')"""))
        executor.execute(newInstance( s"""INSERT INTO $ks.composite_key (key_c1, key_c2, group, value) VALUES (1, 1, 2, 'value2')"""))
        executor.execute(newInstance( s"""INSERT INTO $ks.composite_key (key_c1, key_c2, group, value) VALUES (1, 2, 3, 'value3')"""))
        executor.execute(newInstance( s"""INSERT INTO $ks.composite_key (key_c1, key_c2, group, value) VALUES (2, 2, 4, 'value4')"""))
      },

      Future {
        executor.execute(newInstance( s"""CREATE TABLE $ks.clustering_time (key INT, time TIMESTAMP, value TEXT, PRIMARY KEY (key, time))"""))
        executor.execute(newInstance( s"""INSERT INTO $ks.clustering_time (key, time, value) VALUES (1, '2014-07-12 20:00:01', 'value1')"""))
        executor.execute(newInstance( s"""INSERT INTO $ks.clustering_time (key, time, value) VALUES (1, '2014-07-12 20:00:02', 'value2')"""))
        executor.execute(newInstance( s"""INSERT INTO $ks.clustering_time (key, time, value) VALUES (1, '2014-07-12 20:00:03', 'value3')"""))
      },

      Future {
        executor.execute(newInstance( s"""CREATE TYPE $ks.address (street text, city text, zip int)"""))
        executor.execute(newInstance( s"""CREATE TABLE $ks.udts(key INT PRIMARY KEY, name text, addr frozen<address>)"""))
        executor.execute(newInstance( s"""INSERT INTO $ks.udts(key, name, addr) VALUES (1, 'name', {street: 'Some Street', city: 'Paris', zip: 11120})"""))
      },

      Future {
        executor.execute(newInstance( s"""CREATE TYPE $ks.nested (field int, cassandra_another_field int, cassandra_yet_another_field int)"""))
        executor.execute(newInstance( s"""CREATE TABLE $ks.udts_nested(cassandra_property_1 INT PRIMARY KEY, cassandra_camel_case_property text, nested frozen<nested>)"""))
      },

      Future {
        executor.execute(newInstance( s"""CREATE TABLE $ks.tuples(key INT PRIMARY KEY, value FROZEN<TUPLE<INT, VARCHAR>>)"""))
        executor.execute(newInstance( s"""INSERT INTO $ks.tuples(key, value) VALUES (1, (1, 'first'))"""))
      },

      Future {
        executor.execute(newInstance(s"""CREATE TABLE $ks.delete_short_rows_partition(key INT, group INT, value VARCHAR, PRIMARY KEY (key,group))"""))
        executor.execute(newInstance(s"""INSERT INTO $ks.delete_short_rows_partition(key, group, value) VALUES (10, 10, '1010')"""))
        executor.execute(newInstance(s"""INSERT INTO $ks.delete_short_rows_partition(key, group, value) VALUES (10, 11, '1011')"""))
        executor.execute(newInstance(s"""INSERT INTO $ks.delete_short_rows_partition(key, group, value) VALUES (10, 12, '1012')"""))
        executor.execute(newInstance(s"""INSERT INTO $ks.delete_short_rows_partition(key, group, value) VALUES (20, 20, '2020')"""))
        executor.execute(newInstance(s"""INSERT INTO $ks.delete_short_rows_partition(key, group, value) VALUES (20, 21, '2021')"""))
        executor.execute(newInstance(s"""INSERT INTO $ks.delete_short_rows_partition(key, group, value) VALUES (20, 22, '2022')"""))
      },

      Future {
        executor.execute(newInstance(s"""CREATE TABLE $ks.delete_short_rows(key INT, group INT, value VARCHAR, PRIMARY KEY (key,group))"""))
        executor.execute(newInstance(s"""INSERT INTO $ks.delete_short_rows(key, group, value) VALUES (10, 10, '1010')"""))
        executor.execute(newInstance(s"""INSERT INTO $ks.delete_short_rows(key, group, value) VALUES (10, 11, '1011')"""))
        executor.execute(newInstance(s"""INSERT INTO $ks.delete_short_rows(key, group, value) VALUES (10, 12, '1012')"""))
        executor.execute(newInstance(s"""INSERT INTO $ks.delete_short_rows(key, group, value) VALUES (20, 20, '2020')"""))
        executor.execute(newInstance(s"""INSERT INTO $ks.delete_short_rows(key, group, value) VALUES (20, 21, '2021')"""))
        executor.execute(newInstance(s"""INSERT INTO $ks.delete_short_rows(key, group, value) VALUES (20, 22, '2022')"""))
      },

      Future {
        executor.execute(newInstance(s"""CREATE TABLE $ks.delete_wide_rows4(key INT, group TEXT, group2 INT, value VARCHAR, PRIMARY KEY ((key, group), group2))"""))
        executor.execute(newInstance(s"""INSERT INTO $ks.delete_wide_rows4(key, group, group2, value) VALUES (10, '1', 1, '1010')"""))
        executor.execute(newInstance(s"""INSERT INTO $ks.delete_wide_rows4(key, group, group2, value) VALUES (10, '1', 2, '1011')"""))
        executor.execute(newInstance(s"""INSERT INTO $ks.delete_wide_rows4(key, group, group2, value) VALUES (10, '2', 1, '1012')"""))
        executor.execute(newInstance(s"""INSERT INTO $ks.delete_wide_rows4(key, group, group2, value) VALUES (1, '2', 2, '2020')"""))
        executor.execute(newInstance(s"""INSERT INTO $ks.delete_wide_rows4(key, group, group2, value) VALUES (2, '2', 1, '2021')"""))
      },

      Future {
        executor.execute(newInstance(s"""CREATE TABLE $ks.delete_wide_rows3(key INT, group INT, value VARCHAR, PRIMARY KEY (key, group))"""))
        executor.execute(newInstance(s"""INSERT INTO $ks.delete_wide_rows3(key, group, value) VALUES (10, 10, '1010')"""))
        executor.execute(newInstance(s"""INSERT INTO $ks.delete_wide_rows3(key, group, value) VALUES (10, 11, '1011')"""))
        executor.execute(newInstance(s"""INSERT INTO $ks.delete_wide_rows3(key, group, value) VALUES (10, 12, '1012')"""))
        executor.execute(newInstance(s"""INSERT INTO $ks.delete_wide_rows3(key, group, value) VALUES (1, 20, '2020')"""))
        executor.execute(newInstance(s"""INSERT INTO $ks.delete_wide_rows3(key, group, value) VALUES (2, 21, '2021')"""))
      },

      Future {
        executor.execute(newInstance(s"""CREATE TABLE $ks.delete_wide_rows2(key INT, group INT, value VARCHAR, PRIMARY KEY (key, group))"""))
        executor.execute(newInstance(s"""INSERT INTO $ks.delete_wide_rows2(key, group, value) VALUES (10, 10, '1010')"""))
        executor.execute(newInstance(s"""INSERT INTO $ks.delete_wide_rows2(key, group, value) VALUES (10, 11, '1011')"""))
        executor.execute(newInstance(s"""INSERT INTO $ks.delete_wide_rows2(key, group, value) VALUES (10, 12, '1012')"""))
        executor.execute(newInstance(s"""INSERT INTO $ks.delete_wide_rows2(key, group, value) VALUES (20, 20, '2020')"""))
        executor.execute(newInstance(s"""INSERT INTO $ks.delete_wide_rows2(key, group, value) VALUES (20, 21, '2021')"""))
        executor.execute(newInstance(s"""INSERT INTO $ks.delete_wide_rows2(key, group, value) VALUES (20, 22, '2022')"""))
      },

      Future {
        executor.execute(newInstance(s"""CREATE TABLE $ks.delete_wide_rows1(key INT, group INT, value VARCHAR, PRIMARY KEY (key, group))"""))
        executor.execute(newInstance(s"""INSERT INTO $ks.delete_wide_rows1(key, group, value) VALUES (10, 10, '1010')"""))
        executor.execute(newInstance(s"""INSERT INTO $ks.delete_wide_rows1(key, group, value) VALUES (10, 11, '1011')"""))
        executor.execute(newInstance(s"""INSERT INTO $ks.delete_wide_rows1(key, group, value) VALUES (10, 12, '1012')"""))
        executor.execute(newInstance(s"""INSERT INTO $ks.delete_wide_rows1(key, group, value) VALUES (20, 20, '2020')"""))
        executor.execute(newInstance(s"""INSERT INTO $ks.delete_wide_rows1(key, group, value) VALUES (20, 21, '2021')"""))
        executor.execute(newInstance(s"""INSERT INTO $ks.delete_wide_rows1(key, group, value) VALUES (20, 22, '2022')"""))
       },

      Future {
        executor.execute(newInstance(
          s"""CREATE TYPE $ks."Attachment" (
            |  "Id" text,
            |  "MimeType" text,
            |  "FileName" text
            |)
          """.stripMargin))
        executor.execute(newInstance(
          s"""CREATE TABLE $ks."Interaction" (
            |  "Id" text PRIMARY KEY,
            |  "Attachments" map<text,frozen<"Attachment">>,
            |  "ContactId" text
            |)
          """.stripMargin))
        executor.execute(newInstance(
          s"""INSERT INTO $ks."Interaction"(
            |  "Id",
            |  "Attachments",
            |  "ContactId"
            |)
            |VALUES (
            |  '000000a5ixIEvmPD',
            |  null,
            |  'xcb9HMoQ'
            |)
          """.stripMargin))
        executor.execute(newInstance(
          s"""UPDATE $ks."Interaction"
            |SET
            |  "Attachments" = "Attachments" + {'rVpgK':
            |  {"Id":'rVpgK',
            |  "MimeType":'text/plain',
            |  "FileName":'notes.txt'}}
            |WHERE "Id" = '000000a5ixIEvmPD'
          """.stripMargin))
      },

      Future {
        val tableName = "caseclasstuplegrouped"
        executor.execute(newInstance(s"""CREATE TABLE IF NOT EXISTS $ks.$tableName (key INT, group INT, value VARCHAR, PRIMARY KEY (key, group))"""))
        executor.execute(newInstance(s"""INSERT INTO $ks.$tableName (key, group, value) VALUES (10, 10, '1010')"""))
        executor.execute(newInstance(s"""INSERT INTO $ks.$tableName (key, group, value) VALUES (10, 11, '1011')"""))
        executor.execute(newInstance(s"""INSERT INTO $ks.$tableName (key, group, value) VALUES (10, 12, '1012')"""))
        executor.execute(newInstance(s"""INSERT INTO $ks.$tableName (key, group, value) VALUES (20, 20, '2020')"""))
        executor.execute(newInstance(s"""INSERT INTO $ks.$tableName (key, group, value) VALUES (20, 21, '2021')"""))
        executor.execute(newInstance(s"""INSERT INTO $ks.$tableName (key, group, value) VALUES (20, 22, '2022')"""))
      },
      Future {
        createKeyspace(session, s""""MixedSpace"""")
        executor.execute(newInstance(s"""CREATE TABLE "MixedSpace"."MixedCase"(key INT PRIMARY KEY, value INT)"""))
        executor.execute(newInstance(s"""CREATE TABLE "MixedSpace"."MiXEDCase"(key INT PRIMARY KEY, value INT)"""))
        executor.execute(newInstance(s"""CREATE TABLE "MixedSpace"."MixedCASE"(key INT PRIMARY KEY, value INT)"""))
        executor.execute(newInstance(s"""CREATE TABLE "MixedSpace"."MoxedCAs" (key INT PRIMARY KEY, value INT)"""))
      },
      Future {
        skipIfProtocolVersionLT(V4) {
          executor.execute(newInstance(
            s"""
               |CREATE TABLE $ks.user(
               |  id int PRIMARY KEY,
               |  login text,
               |  firstname text,
               |  lastname text,
               |  country text)""".stripMargin))

          executor.execute(newInstance(
            s"""
               |CREATE MATERIALIZED VIEW $ks.user_by_country
               |  AS SELECT *  //denormalize ALL columns
               |  FROM user
               |  WHERE country IS NOT NULL AND id IS NOT NULL
               |  PRIMARY KEY(country, id);""".stripMargin))

          executor.execute(newInstance(s"INSERT INTO $ks.user(id,login,firstname,lastname,country) VALUES(1, 'jdoe', 'John', 'DOE', 'US')"))

          executor.execute(newInstance(s"INSERT INTO $ks.user(id,login,firstname,lastname,country) VALUES(2, 'hsue', 'Helen', 'SUE', 'US')"))
          executor.execute(newInstance(s"INSERT INTO $ks.user(id,login,firstname,lastname,country) VALUES(3, 'rsmith', 'Richard', 'SMITH', 'UK')"))
          executor.execute(newInstance(s"INSERT INTO $ks.user(id,login,firstname,lastname,country) VALUES(4, 'doanduyhai', 'DuyHai', 'DOAN', 'FR')"))
        }
      },
      Future {
        executor.execute(newInstance( s"""CREATE TABLE $ks.big_table (key INT PRIMARY KEY, value INT)"""))
        val insert = session.prepare( s"""INSERT INTO $ks.big_table(key, value) VALUES (?, ?)""")
        awaitAll {
          for (k <- (0 until bigTableRowCount).grouped(100); i <- k) yield {
            executor.executeAsync(insert.bind(i.asInstanceOf[AnyRef], i.asInstanceOf[AnyRef]))
          }
        }
      },

      Future {
        executor.execute(newInstance( s"""CREATE TABLE $ks.write_time_ttl_test (id INT PRIMARY KEY, value TEXT, value2 TEXT)"""))
      },

      Future {
        def nestedTupleTable(name: String) = s"""CREATE TABLE $ks.$name(
          |  id int PRIMARY KEY,
          |  t frozen <tuple <int, tuple<text, double>>>
          |)""".stripMargin

        executor.execute(newInstance(nestedTupleTable("tuple_test3")))
        executor.execute(newInstance(s"insert into $ks.tuple_test3  (id, t) VALUES (0, (1, ('foo', 2.3)))"))
        executor.execute(newInstance(nestedTupleTable("tuple_test4")))
        executor.execute(newInstance(nestedTupleTable("tuple_test5")))
      },

      Future {
        skipIfProtocolVersionLT(V4) {
          executor.execute(newInstance(s"CREATE TABLE $ks.date_test (key int primary key, dd date)"))
          executor.execute(newInstance(s"INSERT INTO $ks.date_test (key, dd) VALUES (1, '1930-05-31')"))
        }
      }
    )
    executor.waitForCurrentlyExecutingTasks()
  }

  "A CassandraRDD" should "allow to read a Cassandra table as Array of CassandraRow" in {
    val result = sc.cassandraTable(ks, "key_value").collect()
    result should have length 3
    result.head.getInt("key") should (be >= 1 and be <= 3)
    result.head.getLong("group") should (be >= 100L and be <= 300L)
    result.head.getString("value") should startWith("000")
  }

  it should "allow to read a Cassandra table as Array of pairs of primitives" in {
    val result = sc.cassandraTable[(Int, Long)](ks, "key_value").select("key", "group").collect()
    result should have length 3
    result.head._1 should (be >= 1 and be <= 3)
    result.head._2 should (be >= 100L and be <= 300L)
  }

  it should "allow to read a Cassandra table as Array of tuples" in {
    val result = sc.cassandraTable[(Int, Long, String)](ks, "key_value").collect()
    result should have length 3
    result.head._1 should (be >= 1 and be <= 3)
    result.head._2 should (be >= 100L and be <= 300L)
    result.head._3 should startWith("000")
  }

  it should "allow to read a Cassandra table as Array of user-defined case class objects" in {
    val result = sc.cassandraTable[KeyValue](ks, "key_value").collect()
    result should have length 3
    result.head.key should (be >= 1 and be <= 3)
    result.head.group should (be >= 100L and be <= 300L)
    result.head.value should startWith("000")
  }

  it should "allow to read a Cassandra table as Array of user-defined objects with inherited fields" in {
    val result = sc.cassandraTable[SubKeyValue](ks, "key_value").collect()
    result should have length 3
    result.map(kv => (kv.key, kv.group, kv.value)).toSet shouldBe Set(
      (1, 100, "0001"),
      (2, 100, "0002"),
      (3, 300, "0003")
    )
  }

  it should "allow to read a Cassandra table as Array of user-defined class objects" in {
    val result = sc.cassandraTable[SampleScalaClass](ks, "simple_kv").collect()
    result should have length 3
    result.head.key should (be >= 1 and be <= 3)
    result.head.value should startWith("000")
  }

  it should "allow to read a Cassandra table as Array of user-defined class (with multiple constructors) objects" in {
    val result = sc.cassandraTable[SampleScalaClassWithMultipleCtors](ks, "simple_kv").collect()
    result should have length 3
    result.head.key should (be >= 1 and be <= 3)
    result.head.value should startWith("000")
  }

  it should "allow to read a Cassandra table as Array of user-defined class (with no fields) objects" in {
    val result = sc.cassandraTable[SampleScalaClassWithNoFields](ks, "simple_kv").collect()
    result should have length 3
  }

  it should "not overflow on reasonable but large split_size_in_mb" in {
    sc.cassandraTable(ks, "simple_kv")
      .withReadConf(ReadConf(splitSizeInMB = 10000))
      .splitSize should be (10485760000L)
  }

  it should "allow to read a Cassandra table as Array of user-defined case class (nested) objects" in {
    val result = sc.cassandraTable[SampleWithNestedScalaCaseClass#InnerClass](ks, "simple_kv").collect()
    result should have length 3
    result.head.key should (be >= 1 and be <= 3)
    result.head.value should startWith("000")
  }

  it should "allow to read a Cassandra table as Array of user-defined case class (deeply nested) objects" in {
    val result = sc.cassandraTable[SampleWithDeeplyNestedScalaCaseClass#IntermediateClass#InnerClass](ks, "simple_kv").collect()
    result should have length 3
    result.head.key should (be >= 1 and be <= 3)
    result.head.value should startWith("000")
  }

  it should "allow to read a Cassandra table as Array of user-defined case class (nested in object) objects" in {
    val result = sc.cassandraTable[SampleObject.ClassInObject](ks, "simple_kv").collect()
    result should have length 3
    result.head.key should (be >= 1 and be <= 3)
    result.head.value should startWith("000")
  }

  it should "allow to read a Cassandra table as Array of user-defined mutable objects" in {
    val result = sc.cassandraTable[MutableKeyValue](ks, "key_value").collect()
    result should have length 3
    result.head.key should (be >= 1 and be <= 3)
    result.head.group should (be >= 100L and be <= 300L)
    result.head.value should startWith("000")
  }

  it should "allow to read a Cassandra table as Array of user-defined case class objects with custom mapping specified by aliases" in {
    val result = sc.cassandraTable[ClassWithWeirdProps](ks, "key_value")
      .select("key" as "devil", "group" as "cat", "value").collect()
    result should have length 3
    result.head.devil should (be >= 1 and be <= 3)
    result.head.cat should (be >= 100L and be <= 300L)
    result.head.value should startWith("000")
  }

  it should "allow to read a Cassandra table into CassandraRow objects with custom mapping specified by aliases" in {
    val result = sc.cassandraTable(ks, "key_value")
      .select("key" as "devil", "group" as "cat", "value").collect()
    result should have length 3
    result.head.getInt("devil") should (be >= 1 and be <= 3)
    result.head.getLong("cat") should (be >= 100L and be <= 300L)
    result.head.getString("value") should startWith("000")
  }

  it should "apply proper data type conversions for tuples" in {
    val result = sc.cassandraTable[(String, Int, Long)](ks, "key_value").collect()
    result should have length 3
    Some(result.head._1) should contain oneOf("1", "2", "3")
    result.head._2 should (be >= 100 and be <= 300)
    result.head._3 should (be >= 1L and be <= 3L)
  }

  it should "apply proper data type conversions for user-defined case class objects" in {
    val result = sc.cassandraTable[KeyValueWithConversion](ks, "key_value").collect()
    result should have length 3
    Some(result.head.key) should contain oneOf("1", "2", "3")
    result.head.group should (be >= 100 and be <= 300)
    result.head.value should (be >= 1L and be <= 3L)
  }

  it should "apply proper data type conversions for user-defined mutable objects" in {
    val result = sc.cassandraTable[MutableKeyValueWithConversion](ks, "key_value").collect()
    result should have length 3
    Some(result.head.key) should contain oneOf("1", "2", "3")
    result.head.group should (be >= 100 and be <= 300)
    result.head.value should (be >= 1L and be <= 3L)
  }

  it should "map columns to objects using user-defined function" in {
    val result = sc.cassandraTable[MutableKeyValue](ks, "key_value")
      .as((key: Int, group: Long, value: String) => (key, group, value)).collect()
    result should have length 3
    result.head._1 should (be >= 1 and be <= 3)
    result.head._2 should (be >= 100L and be <= 300L)
    result.head._3 should startWith("000")
  }

  it should "map columns to objects using user-defined function with type conversion" in {
    val result = sc.cassandraTable[MutableKeyValue](ks, "key_value")
      .as((key: String, group: String, value: Option[String]) => (key, group, value)).collect()
    result should have length 3
    Some(result.head._1) should contain oneOf("1", "2", "3")
    Some(result.head._2) should contain oneOf("100", "300")
    Some(result.head._3) should contain oneOf(Some("0001"), Some("0002"), Some("0003"))
  }

  it should "allow for selecting a subset of columns" in {
    val result = sc.cassandraTable(ks, "key_value").select("value").collect()
    result should have length 3
    result.head.size shouldEqual 1
    result.head.getString("value") should startWith("000")
  }

  it should "allow for selecting a subset of rows" in {
    val result = sc.cassandraTable(ks, "key_value").where("group < ?", 200L).collect()
    result should have length 2
    result.head.size shouldEqual 3
    result.head.getInt("group") shouldEqual 100
    result.head.getString("value") should startWith("000")
  }

  it should "support single partition where clauses" in {
    val someCass = sc
      .cassandraTable[KeyValue](ks, "key_value")
      .where("key = 1")
      .where("group = 100")
    val result = someCass.collect
    result should contain theSameElementsAs Seq(KeyValue(1, 100, "0001"))
  }

  it should "support in clauses" in {
     val someCass = sc
      .cassandraTable[KeyValue](ks, "key_value")
      .where("key in (1,2,3)")
      .where("group = 100")
    val result = someCass.collect
    result should contain theSameElementsAs Seq(KeyValue(1, 100, "0001"), KeyValue(2, 100, "0002"))
  }

  it should "allow for reading collections" in {
    val result = sc.cassandraTable(ks, "collections").collect()
    val rowById = result.groupBy(_.getInt("key")).mapValues(_.head)
    rowById(1).getList[String]("l") shouldEqual Vector("item1", "item2")
    rowById(1).getSet[String]("s") shouldEqual Set("item1", "item2")
    rowById(1).getMap[String, String]("m") shouldEqual Map("key1" -> "value1", "key2" -> "value2")

    rowById(2).getList[String]("l") shouldEqual Vector.empty
    rowById(2).getSet[String]("s") shouldEqual Set.empty
    rowById(2).getMap[String, String]("m") shouldEqual Map.empty
  }

  it should "allow for reading blobs" in {
    val result = sc.cassandraTable(ks, "blobs").collect()
    val rowById = result.groupBy(_.getInt("key")).mapValues(_.head)
    rowById(1).getBytes("b").limit() shouldEqual 12
    rowById(1).get[Array[Byte]]("b") shouldEqual Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
    rowById(2).getBytesOption("b") shouldEqual None
  }

  it should "allow for reading Cassandra Options from nulls" in {
    val result = sc.cassandraTable[(Int, CassandraOption[Array[Byte]])](ks, "blobs").collect
    result.filter(_._1 == 2)(0)._2 should be(CassandraOption.Unset)
  }

  it should "allow for reading Cassandra Options from values" in {
    val result = sc.cassandraTable[(Int, CassandraOption[Array[Byte]])](ks, "blobs").collect
    result.filter(_._1 == 1)(0)._2.get shouldEqual Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
  }

  it should "allow for converting fields to custom types by user-defined TypeConverter" in {
    val customConverter = new TypeConverter[CustomerId] {
      def targetTypeTag = typeTag[CustomerId]

      def convertPF = { case x: String => CustomerId(x) }
    }
    TypeConverter.registerConverter(customConverter)

    try {
      val result = sc.cassandraTable[(Int, Long, CustomerId)](ks, "key_value").collect()
      result should have length 3
      result(0)._3 shouldNot be(null)
      result(1)._3 shouldNot be(null)
      result(2)._3 shouldNot be(null)
    } finally {
      TypeConverter.unregisterConverter(customConverter)
    }
  }

  it should "allow for reading tables with composite partitioning key" in {
    val result = sc.cassandraTable[(Int, Int, Int, String)](ks, "composite_key")
      .where("group >= ?", 3).collect()
    result should have length 2
  }

  private def dateInDefaultTimeZone(year: Int, month: Int, dayOfMonth: Int, hour: Int, minute: Int, second: Int): Date = {
    Date.from(instantInDefaultTimeZone(year, month, dayOfMonth, hour, minute, second))
  }

  private def instantInDefaultTimeZone(year: Int, month: Int, dayOfMonth: Int, hour: Int, minute: Int, second: Int): Instant = {
    val zonedDateTime = ZonedDateTime.of(year, month, dayOfMonth, hour, minute, second, 0, ZoneId.systemDefault())
    zonedDateTime.toInstant
  }

  it should "convert values passed to where to correct types (String -> Timestamp)" in {
    val start = instantInDefaultTimeZone(2014, 7, 12, 20, 0, 2).toString
    val result = sc.cassandraTable[(Int, Date, String)](ks, "clustering_time")
      .where("time >= ?", start).collect()
    result should have length 2
  }

  it should "convert values passed to where to correct types (Date -> Timestamp)" in {
    val result = sc.cassandraTable[(Int, Date, String)](ks, "clustering_time")
      .where("time >= ?", dateInDefaultTimeZone(2014, 7, 12, 20, 0, 2)).collect()
    result should have length 2
  }

  it should "convert values passed to where to correct types (String -> Timestamp) (double limit)" in {
    val start = instantInDefaultTimeZone(2014, 7, 12, 20, 0, 1).toString
    val end = instantInDefaultTimeZone(2014, 7, 12, 20, 0, 3).toString
    val result = sc.cassandraTable[(Int, Date, String)](ks, "clustering_time")
      .where("time > ? and time < ?", start, end).collect()
    result should have length 1
  }

  it should "convert values passed to where to correct types (Date -> Timestamp) (double limit)" in {
    val result = sc.cassandraTable[(Int, Date, String)](ks, "clustering_time")
      .where("time > ? and time < ?", dateInDefaultTimeZone(2014, 7, 12, 20, 0, 1), dateInDefaultTimeZone(2014, 7, 12, 20, 0, 3)).collect()
    result should have length 1
  }

  it should "accept partitioning key in where" in {
    val result = sc.cassandraTable[(Int, Date, String)](ks, "clustering_time")
      .where("key = ?", 1).collect()
    result should have length 3
  }

  it should "accept partitioning key and clustering column predicate in where" in {
    val result = sc.cassandraTable[(Int, Date, String)](ks, "clustering_time")
      .where("key = ? AND time >= ?", 1, dateInDefaultTimeZone(2014, 7, 12, 20, 0, 2)).collect()
    result should have length 2
  }

  it should "accept composite partitioning key in where" in {
    val result = sc.cassandraTable[(Int, Int, Int, String)](ks, "composite_key")
      .where("key_c1 = ? AND key_c2 = ?", 1, 1).collect()
    result should have length 2
  }

  it should "allow to fetch columns from a table with user defined Cassandra type (UDT)" in {
    val result = sc.cassandraTable(ks, "udts").select("key", "name").collect()
    result should have length 1
    val row = result.head
    row.getInt(0) should be(1)
    row.getString(1) should be("name")
  }

  it should "allow to fetch UDT columns as UDTValue objects" in {
    val result = sc.cassandraTable(ks, "udts").select("key", "name", "addr").collect()
    result should have length 1
    val row = result.head
    row.getInt(0) should be(1)
    row.getString(1) should be("name")

    val udtValue = row.getUDTValue(2)
    udtValue.size should be(3)
    udtValue.getString("street") should be("Some Street")
    udtValue.getString("city") should be("Paris")
    udtValue.getInt("zip") should be(11120)
  }

  it should "allow to save UDT columns from mapped Java objects and read them as UDTValue or Java objects" in {
    val judt = new JavaTestUDTBean
    val jb = new JavaTestBean

    // maps to field
    judt.setField(3)
    // maps to another_field
    judt.setAnotherField(4)
    // maps to yet_another_field
    judt.setCompletelyUnrelatedField(5)

    // maps to property_1
    jb.setProperty1(1)
    // maps to camel_case_property
    jb.setCamelCaseProperty(2)
    // maps to nested
    jb.setNested(judt)

    // We need to bridge the Object mapper in Java to Scala with the JavaBeanColumnMapper
    implicit val columnMapper = new JavaBeanColumnMapper[JavaTestBean]()
    sc.parallelize(Seq(jb)).saveToCassandra(ks,"udts_nested")

    // Saving is done via POJO with annotations, now to read them back in
    val result = sc.cassandraTable(ks, "udts_nested").select("cassandra_property_1", "cassandra_camel_case_property", "nested").collect()
    result should have length 1
    val row = result.head
    row.getInt(0) should be(1)
    row.getInt(1) should be(2)

    val udtValue = row.getUDTValue(2)
    udtValue.size should be(3)
    udtValue.getInt("field") should be(3)
    udtValue.getInt("cassandra_another_field") should be(4)
    udtValue.getInt("cassandra_yet_another_field") should be(5)

    // Let's do one more test, this time reading it back as the POJO
    val bean = sc.cassandraTable[JavaTestBean](ks, "udts_nested").select("cassandra_property_1", "cassandra_camel_case_property", "nested").first()

    bean.getProperty1 should be(1)
    bean.getCamelCaseProperty should be(2)
    bean.getNested.getField should be(3)
    bean.getNested.getAnotherField should be(4)
    bean.getNested.getCompletelyUnrelatedField should be(5)

  }

  it should "allow to fetch UDT columns as objects of case classes" in {
    val result = sc.cassandraTable[ClassWithUDT](ks, "udts").select("key", "name", "addr").collect()
    result should have length 1
    val row = result.head
    row.key should be(1)
    row.name should be("name")

    val udtValue = row.addr
    udtValue.street should be("Some Street")
    udtValue.city should be("Paris")
    udtValue.zip should be(11120)
  }

  it should "allow to fetch tuple columns as TupleValue objects" in {
    val result = sc.cassandraTable(ks, "tuples").select("key", "value").collect()
    result should have length 1
    val row = result.head
    row.getInt(0) should be(1)

    val tuple = row.getTupleValue(1)
    tuple.size should be(2)
    tuple.getInt(0) should be(1)
    tuple.getString(1) should be("first")
  }

  it should "allow to fetch tuple columns as Scala tuples" in {
    val result = sc.cassandraTable[ClassWithTuple](ks, "tuples").select("key", "value").collect()
    result should have length 1
    val row = result.head
    row.key should be(1)
    row.value._1 should be(1)
    row.value._2 should be("first")
  }

  it should "throw appropriate IOException when the table was not found at the computation time" in {
    intercept[IOException] { sc.cassandraTable(ks, "unknown_table").collect() }
  }

  it should "be lazy and must not throw IOException if the table was not found at the RDD initialization time" in {
    sc.cassandraTable(ks, "unknown_table")
  }

  it should "not leak threads" in {

    def threadCount() = {
      // Before returning active thread count, wait while thread count is decreasing,
      // to give spark some time to terminate temporary threads and not count them
      val counts = Iterator.continually { Thread.sleep(500); Thread.activeCount() }
      counts.sliding(2)
        .dropWhile { case Seq(prev, current) => current < prev }
        .next().head
    }

    // compute a few RDDs so the thread pools get initialized
    // using parallel range, to initialize parallel collections fork-join-pools
    val iterationCount = 256
    for (i <- RuntimeUtil.toParallelIterable(1 to iterationCount).par)
      sc.cassandraTable(ks, "key_value").collect()

    // subsequent computations of RDD should reuse already created thread pools,
    // not instantiate new ones
    val startThreadCount = threadCount()
    val oldThreads = Thread.getAllStackTraces.keySet().asScala.toSet

    for (i <- RuntimeUtil.toParallelIterable(1 to iterationCount).par)
      sc.cassandraTable(ks, "key_value").collect()

    val endThreadCount = threadCount()
    val newThreads = Thread.getAllStackTraces.keySet().asScala.toSet
    val createdThreads = newThreads -- oldThreads
    println("Start thread count: " + startThreadCount)
    println("End thread count: " + endThreadCount)
    println("Threads created: ")
    createdThreads.map(_.getName).toSeq.sortBy(identity).foreach(println)

    // This is not very precise, but if there was a thread leak and we leaked even only
    // 1-thread per rdd, this test would not pass. Typically we observed the endThreadCount = startThreadCount +/- 3
    endThreadCount should be < startThreadCount + iterationCount * 3 / 4
  }

  it should "allow to read Cassandra table as Array of KV tuples of two pairs" in {
    val results = sc
      .cassandraTable[(Int, String)](ks, "composite_key")
      .select("group", "value", "key_c1", "key_c2")
      .keyBy[(Int, Int)]("key_c1", "key_c2")
      .collect()
    results should have length 4
    results should contain (((1, 1), (1, "value1")))
    results should contain (((1, 1), (2, "value2")))
    results should contain (((1, 2), (3, "value3")))
    results should contain (((2, 2), (4, "value4")))
  }

  it should "allow to read Cassandra table as Array of KV tuples of a pair and a case class" in {
    val results = sc
      .cassandraTable[Value](ks, "key_value")
      .select("key", "group", "value")
      .keyBy[(Int, Int)]("key", "group")
      .collect()
    results should have length 3
    val map = results.toMap
    map((1, 100)) should be (Value("0001"))
    map((2, 100)) should be (Value("0002"))
    map((3, 300)) should be (Value("0003"))
  }

  it should "allow to read Cassandra table as Array of KV tuples of a case class and a tuple" in {
    val results = sc
      .cassandraTable[(Int, Int, String)](ks, "key_value")
      .select("key", "group", "value")
      .keyBy[KeyGroup]
      .collect()
    results should have length 3
    results should contain ((KeyGroup(1, 100), (1, 100, "0001")))
    results should contain ((KeyGroup(2, 100), (2, 100, "0002")))
    results should contain ((KeyGroup(3, 300), (3, 300, "0003")))
  }

  it should "allow the use of PER PARTITION LIMITs " in from(cassandra = V3_6_0, dse = DSE_V5_1_0) {
    val result = sc.cassandraTable(ks, "clustering_time").perPartitionLimit(1).collect
    result.length should be (1)
  }

  it should "allow to read Cassandra table as Array of KV tuples of a case class and a tuple grouped by partition key" in {

    val tableName = "caseclasstuplegrouped"
    val results = sc
      .cassandraTable[(Int, Int, String)](ks, tableName)
      .select("key", "group", "value")
      .keyBy[Key]
      .spanByKey
      .collect()
      .toMap

    results should have size 2
    results should contain key Key(10)
    results should contain key Key(20)

    results(Key(10)) should contain inOrder(
      (10, 10, "1010"),
      (10, 11, "1011"),
      (10, 12, "1012"))

    results(Key(20)) should contain inOrder(
      (20, 20, "2020"),
      (20, 21, "2021"),
      (20, 22, "2022"))
  }

  it should "allow to read Cassandra table as Array of tuples of two case classes" in {
    val results = sc.cassandraTable[Value](ks, "key_value")
      .select("key", "group", "value")
      .keyBy[KeyGroup]
      .collect()
    results should have length 3
    results should contain((KeyGroup(1, 100), Value("0001")))
    results should contain((KeyGroup(2, 100), Value("0002")))
    results should contain((KeyGroup(3, 300), Value("0003")))
  }

  it should "allow to read Cassandra table as Array of String values" in {
    val results = sc.cassandraTable[String](ks, "key_value").select("value").collect()
    results should have length 3
    results should contain("0001")
    results should contain("0002")
    results should contain("0003")
  }

  it should "allow to read Cassandra table as Array of Int values" in {
    val results = sc.cassandraTable[Int](ks, "key_value").select("key").collect()
    results should have length 3
    results should contain(1)
    results should contain(2)
    results should contain(3)
  }

  it should "allow to read Cassandra table as Array of java.lang.Integer values" in {
    val results = sc.cassandraTable[Integer](ks, "key_value").select("key").collect()
    results should have length 3
    results should contain(1)
    results should contain(2)
    results should contain(3)
  }

  it should "allow to read Cassandra table as Array of List of values" in {
    val results = sc.cassandraTable[List[String]](ks, "collections").select("l").collect()
    results should have length 2
    results should contain(List("item1", "item2"))
  }

  it should "allow to read Cassandra table as Array of Set of values" in {
    val results = sc.cassandraTable[Set[String]](ks, "collections").select("l").collect()
    results should have length 2
    results should contain(Set("item1", "item2"))
  }

  // This is to trigger result set paging, unused in most other tests:
  it should "allow to count a high number of rows" in {
    val count = sc.cassandraTable(ks, "big_table").cassandraCount()
    count should be (bigTableRowCount)
  }

  it should "allow to fetch write time of a specified column as a tuple element" in {
    val writeTime = System.currentTimeMillis() * 1000L
    conn.withSessionDo { session =>
      session.execute(s"""TRUNCATE $ks.write_time_ttl_test""")
      session.execute(s"""INSERT INTO $ks.write_time_ttl_test (id, value, value2) VALUES (1, 'test', 'test2') USING TIMESTAMP $writeTime""")
    }
    val results = sc.cassandraTable[(Int, String, Long)](ks, "write_time_ttl_test")
      .select("id", "value", "value".writeTime).collect().headOption
    results.isDefined should be(true)
    results.get should be((1, "test", writeTime))
  }

  it should "allow to fetch ttl of a specified column as a tuple element" in {
    val ttl = 1000
    conn.withSessionDo { session =>
      session.execute(s"""TRUNCATE $ks.write_time_ttl_test""")
      session.execute(s"""INSERT INTO $ks.write_time_ttl_test (id, value, value2) VALUES (1, 'test', 'test2') USING TTL $ttl""")
    }
    val results = sc.cassandraTable[(Int, String, Int)](ks, "write_time_ttl_test")
      .select("id", "value", "value".ttl).collect().headOption
    results.isDefined should be(true)
    results.get._1 should be (1)
    results.get._2 should be ("test")
    results.get._3 should be > (ttl - 10)
    results.get._3 should be <= ttl
  }

  it should "allow to fetch both write time and ttl of a specified column as tuple elements" in {
    val writeTime = System.currentTimeMillis() * 1000L
    val ttl = 1000
    conn.withSessionDo { session =>
      session.execute(s"""TRUNCATE $ks.write_time_ttl_test""")
      session.execute(s"""INSERT INTO $ks.write_time_ttl_test (id, value, value2) VALUES (1, 'test', 'test2') USING TIMESTAMP $writeTime AND TTL $ttl""")
    }
    val results = sc.cassandraTable[(Int, String, Long, Int)](ks, "write_time_ttl_test")
      .select("id", "value", "value".writeTime, "value".ttl).collect().headOption
    results.isDefined should be(true)
    results.get._1 should be (1)
    results.get._2 should be ("test")
    results.get._3 should be (writeTime)
    results.get._4 should be > (ttl - 10)
    results.get._4 should be <= ttl
  }

  it should "allow to fetch write time of two different columns as tuple elements" in {
    val writeTime = System.currentTimeMillis() * 1000L
    conn.withSessionDo { session =>
      session.execute(s"""TRUNCATE $ks.write_time_ttl_test""")
      session.execute(s"""INSERT INTO $ks.write_time_ttl_test (id, value, value2) VALUES (1, 'test', 'test2') USING TIMESTAMP $writeTime""")
    }
    val results = sc.cassandraTable[(Int, Long, Long)](ks, "write_time_ttl_test")
      .select("id", "value".writeTime, "value2".writeTime).collect().headOption
    results.isDefined should be(true)
    results.get should be((1, writeTime, writeTime))
  }

  it should "allow to fetch ttl of two different columns as tuple elements" in {
    val ttl = 1000
    conn.withSessionDo { session =>
      session.execute(s"""TRUNCATE $ks.write_time_ttl_test""")
      session.execute(s"""INSERT INTO $ks.write_time_ttl_test (id, value, value2) VALUES (1, 'test', 'test2') USING TTL $ttl""")
    }
    val results = sc.cassandraTable[(Int, Int, Int)](ks, "write_time_ttl_test")
      .select("id", "value".ttl, "value2".ttl).collect().headOption
    results.isDefined should be(true)
    results.get._1 should be (1)
    results.get._2 should be > (ttl - 10)
    results.get._2 should be <= ttl
    results.get._3 should be > (ttl - 10)
    results.get._3 should be <= ttl
  }

  it should "allow to fetch writetime of a specified column and map it to a class field with custom mapping" in {
    val writeTime = System.currentTimeMillis() * 1000L
    conn.withSessionDo { session =>
      session.execute(s"""TRUNCATE $ks.write_time_ttl_test""")
      session.execute(s"""INSERT INTO $ks.write_time_ttl_test (id, value, value2) VALUES (1, 'test', 'test2') USING TIMESTAMP $writeTime""")
    }
    implicit val mapper = new DefaultColumnMapper[WriteTimeClass](Map("writeTimeOfValue" -> "value".writeTime.selectedAs))
    val results = sc.cassandraTable[WriteTimeClass](ks, "write_time_ttl_test")
      .select("id", "value", "value".writeTime).collect().headOption
    results.isDefined should be (true)
    results.head should be (WriteTimeClass(1, "test", writeTime))
  }

  it should "allow to fetch ttl of a specified column and map it to a class field with custom mapping" in {
    val ttl = 1000
    conn.withSessionDo { session =>
      session.execute(s"""TRUNCATE $ks.write_time_ttl_test""")
      session.execute(s"""INSERT INTO $ks.write_time_ttl_test (id, value, value2) VALUES (1, 'test', 'test2') USING TTL $ttl""")
    }
    implicit val mapper = new DefaultColumnMapper[TTLClass](Map("ttlOfValue" -> "value".ttl.selectedAs))
    val results = sc.cassandraTable[TTLClass](ks, "write_time_ttl_test")
      .select("id", "value", "value".ttl).collect().headOption
    results.isDefined should be (true)
    results.head.id should be (1)
    results.head.value should be ("test")
    results.head.ttlOfValue > (ttl - 10)
    results.head.ttlOfValue <= ttl
  }

  it should "allow to fetch writetime of a specified column and map it to a class field with aliases" in {
    val writeTime = System.currentTimeMillis() * 1000L
    conn.withSessionDo { session =>
      session.execute(s"""TRUNCATE $ks.write_time_ttl_test""")
      session.execute(s"""INSERT INTO $ks.write_time_ttl_test (id, value, value2) VALUES (1, 'test', 'test2') USING TIMESTAMP $writeTime""")
    }
    val results = sc.cassandraTable[WriteTimeClass](ks, "write_time_ttl_test")
      .select("id", "value", "value".writeTime as "writeTimeOfValue").collect().headOption
    results.isDefined should be (true)
    results.head should be (WriteTimeClass(1, "test", writeTime))
  }

  it should "allow to fetch ttl of a specified column and map it to a class field with aliases" in {
    val ttl = 1000
    conn.withSessionDo { session =>
      session.execute(s"""TRUNCATE $ks.write_time_ttl_test""")
      session.execute(s"""INSERT INTO $ks.write_time_ttl_test (id, value, value2) VALUES (1, 'test', 'test2') USING TTL $ttl""")
    }
    val results = sc.cassandraTable[TTLClass](ks, "write_time_ttl_test")
      .select("id", "value", "value".ttl as "ttlOfValue").collect().headOption
    results.isDefined should be (true)
    results.head.id should be (1)
    results.head.value should be ("test")
    results.head.ttlOfValue > (ttl - 10)
    results.head.ttlOfValue <= ttl
  }

  it should "allow to specify ascending ordering" in {
    val results = sc.cassandraTable[(Int, Date, String)](ks, "clustering_time")
      .where("key=1").withAscOrder.collect()
    results.map(_._3).toList shouldBe List("value1", "value2", "value3")
  }

  it should "allow to specify descending ordering" in {
    val results = sc.cassandraTable[(Int, Date, String)](ks, "clustering_time")
      .where("key=1").withDescOrder.collect()
    results.map(_._3).toList shouldBe List("value3", "value2", "value1")
  }

  it should "allow to specify rows number limit" in {
    val results = sc.cassandraTable[(Int, Date, String)](ks, "clustering_time").where("key=1").limit(2).collect()
    results should have length 2
    results(0)._3 shouldBe "value1"
    results(1)._3 shouldBe "value2"
  }

  it should "allow to specify rows number with take" in {
    val results = sc.cassandraTable[(Int, Date, String)](ks, "clustering_time").where("key=1").take(2)
    results should have length 2
    results(0)._3 shouldBe "value1"
    results(1)._3 shouldBe "value2"
  }

  it should "count the CassandraRDD items" in {
    val result = sc.cassandraTable(ks, "big_table").cassandraCount()
    result shouldBe bigTableRowCount
  }

  it should "count the CassandraRDD items with where predicate" in {
    val result = sc.cassandraTable(ks, "big_table").where("key=1").cassandraCount()
    result shouldBe 1
  }

  it should "allow to use empty RDD on undefined table" in {
    val result = sc.cassandraTable("unknown_ks", "unknown_table").toEmptyCassandraRDD.collect()
    result should have length 0
  }

  it should "allow to use empty RDD on defined table" in {
    val result = sc.cassandraTable(ks, "simple_kv").toEmptyCassandraRDD.collect()
    result should have length 0
  }

  it should "suggest similar tables or views if the table doesn't exist" in skipIfProtocolVersionLT(V4){
    val ioe = the [IOException] thrownBy sc.cassandraTable(ks, "user_by_county").collect()
    val message = ioe.getMessage
    message should include (s"$ks.user_by_country")
  }

  it should "suggest similar tables if table doesn't exist but keyspace does" in {
    val ioe = the [IOException] thrownBy sc.cassandraTable("MixedSpace","mixedcase").collect()
    val message = ioe.getMessage
    message should include ("MixedSpace.MixedCase")
    message should include ("MixedSpace.MiXEDCase")
    message should include ("MixedSpace.MixedCASE")
  }

  it should "suggest possible keyspace and table matches if the keyspace and table do not exist" in {
    val ioe = the [IOException] thrownBy sc.cassandraTable("MoxedSpace","mixdcase").collect()
    val message = ioe.getMessage
    message should include ("MixedSpace.MixedCase")
    message should include ("MixedSpace.MiXEDCase")
    message should include ("MixedSpace.MixedCASE")
  }

  it should "suggest possible keyspaces if the table exists but in a different keyspace" in {
    val ioe = the [IOException] thrownBy sc.cassandraTable("MoxedSpace","MoxedCAS").collect()
    val message = ioe.getMessage
    message should include ("MixedSpace.MoxedCAs")
  }

  it should "suggest possible keyspaces and tables if the table has a fuzzy match but they keyspace does not" in {
    val ioe = the [IOException] thrownBy sc.cassandraTable("rock","MixedCase").collect()
    val message = ioe.getMessage
    message should include ("MixedSpace.MixedCase")
  }

  it should "handle upper case characters in UDT fields" in {

    val tableRdd = sc.cassandraTable(ks, "Interaction")
    val dataColumns = tableRdd.map(row => row.getString("ContactId"))
    dataColumns.count shouldBe 1
  }

  it should "be able to read SMALLINT columns from" in skipIfProtocolVersionLT(V4) {
    val result = sc.cassandraTable[(Int, Short)](ks, "short_value").collect
    result should contain ((1, 100))
    result should contain ((2, 200))
    result should contain ((3, 300))
  }

  it should "be able to read a Materialized View" in skipIfProtocolVersionLT(V4){
    val result = sc.cassandraTable[(String, Int, String, String, String)](ks, "user_by_country")
      .where("country='US'")
      .collect
    result should contain theSameElementsAs Seq(
      ("US", 1, "John", "DOE", "jdoe"),
      ("US", 2, "Helen", "SUE", "hsue")
    )
  }

  it should "throw a meaningful exception when reading a table view" in from(cassandra = V4_0_0, dse = DSE_V6_7_0) {
    import org.apache.spark.sql.cassandra._

    intercept[NoSuchNamespaceException] {
      val data = spark.read.cassandraFormat("sstable_tasks", "system_views").load()
      data.show()
    }
  }

  it should "throw an exception when trying to write to a Materialized View" in skipIfProtocolVersionLT(V4){
    intercept[IllegalArgumentException] {
      sc.parallelize(Seq(("US", 1, "John", "DOE", "jdoe"))).saveToCassandra(ks, "user_by_country")
    }
  }

  it should "read rows with nested C* Tuples as case classes" in {
    val result = sc.cassandraTable[TypeWithNestedTuple](ks, "tuple_test3").collect()

    result.length should be (1)
    result.head.t should be ((1, ("foo", 2.3)))
  }

  it should "read rows with nested C* Tuples as case classes with setters" in {
    val result = sc.cassandraTable[TypeWithTupleSetter](ks, "tuple_test3").collect()

    result.length should be (1)
    result.head.t should be ((1, ("foo", 2.3)))
  }

  it should "read rows with nested C* Tuples as Scala Tuple" in {
    val result = sc.cassandraTable[(Int, (Int, (String, Double)))](ks, "tuple_test3").collect()

    result.length should be (1)
    result.head should be ((0, (1, ("foo", 2.3))))
  }

  it should "write Scala Tuple as C* Tuple" in {
    val rdd = sc.parallelize(List(
      (0, (1, ("foo", 2.3))),
      (4, (5, ("bar", 6.7)))
    ))

    rdd.saveToCassandra(ks, "tuple_test4", SomeColumns("id", "t"))

    conn.withSessionDo { session =>
      session.execute(s"select count(1) from $ks.tuple_test4").one().getLong(0) should be (2)
    }
  }

  it should "write case class with Scala Tuple as C* Tuple" in {
    val rdd = sc.parallelize(List(
      TypeWithNestedTuple(0, (1, ("foo", 2.3))),
      TypeWithNestedTuple(4, (5, ("bar", 6.7)))
    ))

    rdd.saveToCassandra(ks, "tuple_test5")

    conn.withSessionDo { session =>
      session.execute(s"select count(1) from $ks.tuple_test5").one().getLong(0) should be (2)
    }
  }

  it should "write Java dates as C* date type" in skipIfProtocolVersionLT(V4){
    val rows = List(
      (6, new java.sql.Date(new Date().getTime)),
      (7, new Date()),
      (9, LocalDate.now()))

    sc.parallelize(rows).saveToCassandra(ks, "date_test")

    val resultSet = conn.withSessionDo { session =>
      session.execute(
        s"select count(1) from $ks.date_test where key in (${rows.map(_._1.toString).mkString(",")})")
    }
    resultSet.one().getLong(0) should be(rows.size)
  }

  it should "read C* row with dates as Java dates" in skipIfProtocolVersionLT(V4){
    val expected: LocalDate = LocalDate.of(1930, 5, 31)
    val row = sc.cassandraTable(ks, "date_test").where("key = 1").first

    row.getInt("key") should be(1)
    row.getDate("dd") should be(Date.from(expected.atStartOfDay(ZoneId.systemDefault()).toInstant))
  }

  it should "read LocalDate as tuple value with given type" in skipIfProtocolVersionLT(V4){
    val expected: LocalDate = LocalDate.of(1930, 5, 31)
    val date = sc.cassandraTable[(Int, Date)](ks, "date_test").where("key = 1").first._2
    val localDate = sc.cassandraTable[(Int, LocalDate)](ks, "date_test").where("key = 1").first._2

    date should be(Date.from(expected.atStartOfDay(ZoneId.systemDefault()).toInstant))
    localDate should be(expected)
  }

  it should "adjust local connections based on the runtime config" in {
    val expected: Int = math.max(1, Runtime.getRuntime.availableProcessors() - 1)
    markup(s"Expected = $expected, 1 is default")
    val rdd = sc.cassandraTable(ks, "big_table")
    val poolingOptions = rdd.connector.withSessionDo(_.getContext.getConfig).getDefaultProfile //TODO we gotta do something here I suppose
    poolingOptions.getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE) should be (expected)
  }

  it should "allow forcing a larger maxConnection based on a runtime conf change" in {
    val expected = 15
    val conf = sc.getConf
        .set(CassandraConnectorConf.RemoteConnectionsPerExecutorParam.name, expected.toString)
        .set(CassandraConnectorConf.LocalConnectionsPerExecutorParam.name, expected.toString)
    val rdd = sc.cassandraTable(ks, "big_table").withConnector(CassandraConnector(conf))

    val poolingOptions = rdd.connector.withSessionDo(_.getContext.getConfig).getDefaultProfile //TODO we gotta do something here I suppose
    poolingOptions.getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE) should be (expected)
    poolingOptions.getInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE) should be (expected)
  }


  "RDD.coalesce"  should "not loose data" in {
    val rdd = sc.cassandraTable(ks, "big_table").coalesce(4)
    rdd.count should be (bigTableRowCount)
  }

  it should "set exact number of partitions" in {
    val rdd = sc.cassandraTable(ks, "big_table").coalesce(8)
    rdd.partitions.length should be (8 +-1 )
  }

  it should "set exact number of partitions (1)" in {
    val rdd = sc.cassandraTable(ks, "big_table").coalesce(1)
    rdd.partitions.length should be (1)
  }

  it should "work with 0" in {
    val rdd = sc.cassandraTable(ks, "big_table").coalesce(0)
    rdd.partitions.length should be (1)
  }

  "RDD.repartition"  should "not loose data" in {
    val rdd = sc.cassandraTable(ks, "big_table").repartition(4)
    rdd.count should be (bigTableRowCount)
  }

  it should "set exact number of partitions" in {
    val rdd = sc.cassandraTable(ks, "big_table").repartition(4)
    rdd.partitions.length should be (4)
  }

  "RDD.deleteFromCassandra" should "delete rows just selected from the C*" in {

    sc.cassandraTable(ks, "delete_wide_rows1").where("key = 20")
      .deleteFromCassandra(ks, "delete_wide_rows1")

    val results = sc
      .cassandraTable[(Int, Int, String)](ks, "delete_wide_rows1")
      .select("key", "group", "value")
      .collect()

    results should have size 3

    results should contain theSameElementsAs Seq(
      (10, 10, "1010"),
      (10, 11, "1011"),
      (10, 12, "1012"))

  }

  it should "delete rows with specified mapping" in {

    sc.cassandraTable[(Int, Int)](ks, "delete_wide_rows2")
      .select("group", "key").where("key = 20")
      .deleteFromCassandra(ks, "delete_wide_rows2", keyColumns = SomeColumns("group", "key"))

    val results = sc
      .cassandraTable[(Int, Int, String)](ks, "delete_wide_rows2")
      .select("key", "group", "value")
      .collect()

    results should have size 3

    results should contain theSameElementsAs Seq(
      (10, 10, "1010"),
      (10, 11, "1011"),
      (10, 12, "1012"))

  }

  it should "delete rows with joins" in {

    sc.cassandraTable(ks, "delete_wide_rows3").joinWithCassandraTable(ks, "key_value").map(_._1)
      .deleteFromCassandra(ks, "delete_wide_rows3")

    val results = sc
      .cassandraTable[(Int, Int, String)](ks, "delete_wide_rows3")
      .select("key", "group", "value")
      .collect()

    results should have size 3

    results should contain inOrder(
      (10, 10, "1010"),
      (10, 11, "1011"),
      (10, 12, "1012"))

  }

  it should "delete rows with composite key" in {

    sc.parallelize(Seq((10, "1", 1), (10, "2", 1)))
      .deleteFromCassandra(ks, "delete_wide_rows4")

    val results = sc
      .cassandraTable[(Int, String, Int, String)](ks, "delete_wide_rows4")
      .select("key", "group", "group2", "value")
      .collect()

    results should have size 3
  }

  it should "delete just selected column from the C*" in {

   sc.cassandraTable(ks, "delete_short_rows").where("key = 20")
      .deleteFromCassandra(ks, "delete_short_rows", SomeColumns("value"))

    val results = sc.cassandraTable[(Int, Int, Option[String])](ks, "delete_short_rows")
      .collect()

    results should have size 6

    results should contain theSameElementsAs Seq(
      (10, 10, Some("1010")),
      (10, 11, Some("1011")),
      (10, 12, Some("1012")),
      (20, 20, None),
      (20, 21, None),
      (20, 22, None))
  }

  it should "delete base on partition key only" in {

    sc.parallelize(Seq(Key(20)))
      .deleteFromCassandra(ks, "delete_short_rows_partition", keyColumns = SomeColumns("key"))

    val results = sc.cassandraTable[(Int, Int, Option[String])](ks, "delete_short_rows_partition")
      .collect()

    results should have size 3

    results should contain theSameElementsAs Seq(
      (10, 10, Some("1010")),
      (10, 11, Some("1011")),
      (10, 12, Some("1012")))

  }

  "DataSize Estimates" should "handle overflows in the size estimates for a table" in {
    fudgeSizeEstimatesTable("key_value", Long.MaxValue)
    val partitions = sc.cassandraTable(ks, "key_value").partitions
    partitions.length should be <= (sc.defaultParallelism * 3)
  }


  /**
    * Fudges the size estimates information for the given table
    * Attempts to replace all records for existing ranges with a single record
    * giving a mean size of sizeFudgeInMB
    */
  def fudgeSizeEstimatesTable(tableName: String, sizeFudgeInMB: Long) = {

    val tokenFactory = TokenFactory.forSystemLocalPartitioner(conn)

    conn.withSessionDo { case session =>
      session.execute(SimpleStatement.builder(
        """DELETE FROM system.size_estimates
          |where keyspace_name = ?
          |AND table_name = ?""".stripMargin)
        .addPositionalValues(ks, tableName)
        .build())

      session.execute(
        SimpleStatement.builder(
        """
          |INSERT INTO system.size_estimates (
          |  keyspace_name,
          |  table_name,
          |  range_start,
          |  range_end,
          |  mean_partition_size,
          |  partitions_count)
          |  VALUES (?,?,?,?,?,?)
        """.
          stripMargin).addPositionalValues(
        ks,
        tableName,
        tokenFactory.minToken.toString,
        tokenFactory.maxToken.toString,
        sizeFudgeInMB * 1024 * 1024: java.lang.Long,
        1L: java.lang.Long).build())
    }
  }
}
