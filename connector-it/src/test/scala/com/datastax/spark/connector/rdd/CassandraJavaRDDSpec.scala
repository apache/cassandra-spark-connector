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
import java.net.{InetAddress, InetSocketAddress}

import com.datastax.spark.connector._
import com.datastax.spark.connector.cluster.DefaultCluster
import com.datastax.spark.connector.cql.{CassandraConnector, IpBasedContactInfo}
import com.datastax.spark.connector.japi.CassandraJavaUtil._
import com.datastax.spark.connector.japi.CassandraRow
import com.datastax.spark.connector.types.TypeConverter
import org.apache.commons.lang3.tuple
import org.apache.spark.api.java.function.{Function => JFunction}

import scala.jdk.CollectionConverters._
import scala.concurrent.Future

class CassandraJavaRDDSpec extends SparkCassandraITFlatSpecBase with DefaultCluster {

  override lazy val conn = CassandraConnector(defaultConf)

  override def beforeClass {
    conn.withSessionDo { session =>
      createKeyspace(session)

      awaitAll(
        Future {
          session.execute(s"CREATE TABLE $ks.test_table (key INT, value TEXT, PRIMARY KEY (key))")
          session.execute(s"CREATE INDEX test_table_idx ON $ks.test_table (value)")
          session.execute(s"INSERT INTO $ks.test_table (key, value) VALUES (1, 'one')")
          session.execute(s"INSERT INTO $ks.test_table (key, value) VALUES (2, 'two')")
          session.execute(s"INSERT INTO $ks.test_table (key, value) VALUES (3,  null)")
        },

        Future {
          session.execute(s"CREATE TABLE $ks.test_table2 (some_key INT, some_value TEXT, PRIMARY KEY (some_key))")
          session.execute(s"INSERT INTO $ks.test_table2 (some_key, some_value) VALUES (1, 'one')")
          session.execute(s"INSERT INTO $ks.test_table2 (some_key, some_value) VALUES (2, 'two')")
          session.execute(s"INSERT INTO $ks.test_table2 (some_key, some_value) VALUES (3, null)")
        },

        Future {
          session.execute(s"CREATE TABLE $ks.test_table3 (key INT, value TEXT, sub_class_field TEXT, PRIMARY KEY (key))")
          session.execute(s"INSERT INTO $ks.test_table3 (key, value, sub_class_field) VALUES (1, 'one', 'a')")
          session.execute(s"INSERT INTO $ks.test_table3 (key, value, sub_class_field) VALUES (2, 'two', 'b')")
          session.execute(s"INSERT INTO $ks.test_table3 (key, value, sub_class_field) VALUES (3,  null, 'c')")
        },

        Future {
          session.execute(s"CREATE TABLE $ks.collections (key INT PRIMARY KEY, l list<text>, s set<text>, m map<text, text>)")
          session.execute(s"INSERT INTO $ks.collections (key, l, s, m) VALUES (1, ['item1', 'item2'], {'item1', 'item2'}, {'key1': 'value1', 'key2': 'value2'})")
          session.execute(s"INSERT INTO $ks.collections (key, l, s, m) VALUES (2, null, null, null)")
        },

        Future {
          session.execute(s"CREATE TABLE $ks.nulls (key INT PRIMARY KEY, i int, vi varint, t text, d timestamp, l list<int>)")
          session.execute(s"INSERT INTO $ks.nulls (key, i, vi, t, d, l) VALUES (1, null, null, null, null, null)")
        },

        Future {
          session.execute(s"CREATE TYPE $ks.address (street text, city text, zip int)")
          session.execute(s"CREATE TABLE $ks.udts(key INT PRIMARY KEY, name text, addr frozen<address>)")
          session.execute(s"INSERT INTO $ks.udts(key, name, addr) VALUES (1, 'name', {street: 'Some Street', city: 'Paris', zip: 11120})")
        },

        Future {
          session.execute(s"CREATE TABLE $ks.tuples(key INT PRIMARY KEY, value FROZEN<TUPLE<INT, VARCHAR>>)")
          session.execute(s"INSERT INTO $ks.tuples(key, value) VALUES (1, (1, 'first'))")
        },

        Future {
          session.execute(s"CREATE TABLE $ks.wide_rows(key INT, group INT, value VARCHAR, PRIMARY KEY (key, group))")
          session.execute(s"INSERT INTO $ks.wide_rows(key, group, value) VALUES (10, 10, '1010')")
          session.execute(s"INSERT INTO $ks.wide_rows(key, group, value) VALUES (10, 11, '1011')")
          session.execute(s"INSERT INTO $ks.wide_rows(key, group, value) VALUES (10, 12, '1012')")
          session.execute(s"INSERT INTO $ks.wide_rows(key, group, value) VALUES (20, 20, '2020')")
          session.execute(s"INSERT INTO $ks.wide_rows(key, group, value) VALUES (20, 21, '2021')")
          session.execute(s"INSERT INTO $ks.wide_rows(key, group, value) VALUES (20, 22, '2022')")
        },

        Future {
          session.execute(s"CREATE TABLE $ks.limit_test_table (key INT, value TEXT, PRIMARY KEY (key))")
          for (i <- 0 to 30) {
            session.execute(s"INSERT INTO $ks.limit_test_table (key, value) VALUES ($i, '$i')")
          }
        }
      )
    }
  }

  "CassandraJavaRDD" should "allow to read data as CassandraRows " in {
    val rows = javaFunctions(sc).cassandraTable(ks, "test_table").collect()
    assert(rows.size == 3)
    assert(rows.asScala.exists(row ⇒ row.getString("value") == "one" && row.getInt("key") == 1))
    assert(rows.asScala.exists(row ⇒ row.getString("value") == "two" && row.getInt("key") == 2))
    assert(rows.asScala.exists(row ⇒ row.getString("value") == null && row.getInt("key") == 3))
  }

  it should "allow to read data as Java beans " in {
    val beans = javaFunctions(sc).cassandraTable(ks, "test_table", mapRowTo(classOf[SampleJavaBean])).collect()
    assert(beans.size == 3)
    assert(beans.asScala.exists(bean ⇒ bean.getValue == "one" && bean.getKey == 1))
    assert(beans.asScala.exists(bean ⇒ bean.getValue == "two" && bean.getKey == 2))
    assert(beans.asScala.exists(bean ⇒ bean.getValue == null && bean.getKey == 3))
  }

  it should "allow to read data as Java beans with inherited fields" in {
    val beans = javaFunctions(sc).cassandraTable(ks, "test_table3", mapRowTo(classOf[SampleJavaBeanSubClass])).collect()
    assert(beans.size == 3)
    assert(beans.asScala.exists(bean ⇒ bean.getValue == "one" && bean.getKey == 1 && bean.getSubClassField == "a"))
    assert(beans.asScala.exists(bean ⇒ bean.getValue == "two" && bean.getKey == 2 && bean.getSubClassField == "b"))
    assert(beans.asScala.exists(bean ⇒ bean.getValue == null && bean.getKey == 3 && bean.getSubClassField == "c"))
  }

  it should "allow to read data as Java beans with custom mapping defined by aliases" in {
    val beans = javaFunctions(sc)
      .cassandraTable(ks, "test_table", mapRowTo(classOf[SampleWeirdJavaBean]))
      .select(column("key").as("devil"), column("value").as("cat"))
      .collect()
    assert(beans.size == 3)
    assert(beans.asScala.exists(bean ⇒ bean.getCat == "one" && bean.getDevil == 1))
    assert(beans.asScala.exists(bean ⇒ bean.getCat == "two" && bean.getDevil == 2))
    assert(beans.asScala.exists(bean ⇒ bean.getCat == null && bean.getDevil == 3))
  }

  it should "allow to read data as Java beans (with multiple constructors)" in {
    val beans = javaFunctions(sc).cassandraTable(ks, "test_table", mapRowTo(classOf[SampleJavaBeanWithMultipleCtors])).collect()
    assert(beans.size == 3)
    assert(beans.asScala.exists(bean ⇒ bean.getValue == "one" && bean.getKey == 1))
    assert(beans.asScala.exists(bean ⇒ bean.getValue == "two" && bean.getKey == 2))
    assert(beans.asScala.exists(bean ⇒ bean.getValue == null && bean.getKey == 3))
  }

  it should "throw NoSuchMethodException when trying to read data as Java beans (without no-args constructor)" in {
    intercept[NoSuchMethodException](javaFunctions(sc)
      .cassandraTable(ks, "test_table", mapRowTo(classOf[SampleJavaBeanWithoutNoArgsCtor])).collect())
  }

  it should "allow to read data as nested Java beans" in {
    val beans = javaFunctions(sc).cassandraTable(ks, "test_table", mapRowTo(classOf[SampleWithNestedJavaBean#InnerClass])).collect()
    assert(beans.size == 3)
    assert(beans.asScala.exists(bean ⇒ bean.getValue == "one" && bean.getKey == 1))
    assert(beans.asScala.exists(bean ⇒ bean.getValue == "two" && bean.getKey == 2))
    assert(beans.asScala.exists(bean ⇒ bean.getValue == null && bean.getKey == 3))
  }

  it should "allow to read data as deeply nested Java beans" in {
    val beans = javaFunctions(sc).cassandraTable(ks, "test_table",
      mapRowTo(classOf[SampleWithDeeplyNestedJavaBean#IntermediateClass#InnerClass])).collect()
    assert(beans.size == 3)
    assert(beans.asScala.exists(bean ⇒ bean.getValue == "one" && bean.getKey == 1))
    assert(beans.asScala.exists(bean ⇒ bean.getValue == "two" && bean.getKey == 2))
    assert(beans.asScala.exists(bean ⇒ bean.getValue == null && bean.getKey == 3))
  }


  it should "allow to select a subset of columns" in {
    val rows = javaFunctions(sc).cassandraTable(ks, "test_table")
      .select("key").collect()
    assert(rows.size == 3)
    assert(rows.asScala.exists(row ⇒ !row.contains("value") && row.getInt("key") == 1))
    assert(rows.asScala.exists(row ⇒ !row.contains("value") && row.getInt("key") == 2))
    assert(rows.asScala.exists(row ⇒ !row.contains("value") && row.getInt("key") == 3))
  }

  it should "return selected columns" in {
    val rdd = javaFunctions(sc).cassandraTable(ks, "test_table")
      .select("key")
    assert(rdd.selectedColumnNames().length === 1)
    assert(rdd.selectedColumnNames().contains("key"))
  }

  it should "allow to use where clause to filter records" in {
    val rows = javaFunctions(sc).cassandraTable(ks, "test_table")
      .where("value = ?", "two").collect()
    assert(rows.size === 1)
    assert(rows.asScala.exists(row => row.getString("value") == "two" && row.getInt("key") == 2))
  }

  it should "allow to read rows as an array of a single-column type supported by TypeConverter" in {
    val rows1 = javaFunctions(sc)
      .cassandraTable(ks, "test_table", mapColumnTo(classOf[java.lang.String]))
      .select("value")
      .collect()
    rows1 should have size 3
    rows1 should contain("one")
    rows1 should contain("two")
    rows1 should contain(null)

    val rows2 = javaFunctions(sc)
      .cassandraTable(ks, "test_table", mapColumnTo(classOf[java.lang.Integer]))
      .select("key")
      .collect()

    rows2 should have size 3
    rows2 should contain(1)
    rows2 should contain(2)
    rows2 should contain(3)

    val rows3 = javaFunctions(sc)
      .cassandraTable(ks, "test_table", mapColumnTo(classOf[java.lang.Double]))
      .select("key")
      .collect()

    rows3 should have size 3
    rows3 should contain(1d)
    rows3 should contain(2d)
    rows3 should contain(3d)
  }

  it should "allow to read rows as an array of a single-column list" in {
    val rows = javaFunctions(sc)
      .cassandraTable(ks, "collections", mapColumnToListOf(classOf[String]))
      .select("l")
      .collect().asScala.map(_.asScala.toList)

    rows should have size 2
    rows should contain(List("item1", "item2"))
    rows should contain(List())
  }

  it should "allow to read rows as an array of a single-column set" in {
    val rows = javaFunctions(sc)
      .cassandraTable(ks, "collections", mapColumnToSetOf(classOf[String]))
      .select("s")
      .collect().asScala.map(_.asScala.toSet)

    rows should have size 2
    rows should contain(Set("item1", "item2"))
    rows should contain(Set())
  }

  it should "allow to read rows as an array of a single-column map" in {
    val rows = javaFunctions(sc)
      .cassandraTable(ks, "collections", mapColumnToMapOf(classOf[String], classOf[String]))
      .select("m")
      .collect().asScala.map(_.asScala.toMap)

    rows should have size 2
    rows should contain(Map("key1" → "value1", "key2" → "value2"))
    rows should contain(Map())
  }

  it should "allow to read rows as an array of multi-column type" in {
    val rows = javaFunctions(sc)
      .cassandraTable(ks, "test_table", mapRowTo(classOf[SampleJavaBean]))
      .collect().asScala.map(x => (x.getKey, x.getValue))

    rows should have size 3
    rows should contain((1, "one"))
    rows should contain((2, "two"))
    rows should contain((3, null))
  }

  it should "allow to read rows as an array of multi-column type with explicit column name mapping" in {
    val rows = javaFunctions(sc)
      .cassandraTable(ks, "test_table2", mapRowTo(classOf[SampleJavaBean],
        tuple.Pair.of("key", "some_key"), tuple.Pair.of("value", "some_value")))
      .collect().asScala.map(x => (x.getKey, x.getValue))

    rows should have size 3
    rows should contain((1, "one"))
    rows should contain((2, "two"))
    rows should contain((3, null))
  }

  it should "allow to transform rows into KV pairs of two single-column types" in {
    val rows = javaFunctions(sc)
      .cassandraTable(ks, "test_table", mapColumnTo(classOf[java.lang.String]))
      .select("value", "key")
      .keyBy(
        mapColumnTo(classOf[java.lang.Integer]),
        mapToRow(classOf[java.lang.Integer]),
        classOf[Integer], "key")
      .collect()

    rows should have size 3
    rows should contain((1, "one"))
    rows should contain((2, "two"))
    rows should contain((3, null))
  }

  it should "allow to transform rows into KV pairs of a single-column type and a multi-column type" in {
    val rows = javaFunctions(sc)
      .cassandraTable(ks, "test_table", mapRowTo(classOf[SampleJavaBean]))
      .keyBy(
        mapColumnTo(classOf[java.lang.Integer]),
        mapToRow(classOf[java.lang.Integer]),
        classOf[Integer],
        "key")
      .collect().asScala.map { case (i, x) ⇒ (i, (x.getKey, x.getValue))}

    rows should have size 3
    rows should contain((1, (1, "one")))
    rows should contain((2, (2, "two")))
    rows should contain((3, (3, null)))
  }

  it should "allow to transform rows into KV pairs of a multi-column type and a single-column type" in {
    val rows = javaFunctions(sc)
      .cassandraTable(ks, "test_table", mapColumnTo(classOf[java.lang.Integer]))
      .select("key", "value")
      .keyBy(
        mapRowTo(classOf[SampleJavaBean]),
        mapToRow(classOf[SampleJavaBean]),
        classOf[SampleJavaBean])
      .collect().asScala.map { case (x, i) ⇒ ((x.getKey, x.getValue), i)}

    rows should have size 3
    rows should contain(((1, "one"), 1))
    rows should contain(((2, "two"), 2))
    rows should contain(((3, null), 3))
  }

  it should "allow to transform rows into KV pairs of multi-column types" in {
    val rows = javaFunctions(sc)
      .cassandraTable(ks, "test_table", mapRowTo(classOf[SampleJavaBean]))
      .keyBy(
        mapRowTo(classOf[SampleJavaBean]),
        mapToRow(classOf[SampleJavaBean]),
        classOf[SampleJavaBean])
      .collect().asScala.map { case (x, y) ⇒ ((x.getKey, x.getValue), (y.getKey, y.getValue))}

    rows should have size 3
    rows should contain(((1, "one"), (1, "one")))
    rows should contain(((2, "two"), (2, "two")))
    rows should contain(((3, null), (3, null)))
  }

  it should "allow to read Cassandra data as array of Integer" in {
    val rows = javaFunctions(sc)
      .cassandraTable(ks, "test_table", mapColumnTo(TypeConverter.JavaIntConverter))
      .select("key").collect()

    rows should have size 3
    rows should contain(1)
    rows should contain(2)
    rows should contain(3)
  }

  it should "allow to change the default Cassandra Connector to a custom one" in {
    // work with valid connector
    javaFunctions(sc).cassandraTable(ks, "test_table").collect()

    // doesn't work with invalid connector
    val invalidConnector = CassandraConnector(IpBasedContactInfo(Set(new InetSocketAddress(InetAddress.getByName(cluster.getConnectionHost), 9999))))
    intercept[IOException] {
      javaFunctions(sc).cassandraTable(ks, "test_table").withConnector(invalidConnector).collect()
    }
  }

  it should "allow to read null columns" in {
    val row = javaFunctions(sc)
      .cassandraTable(ks, "nulls")
      .select("i", "vi", "t", "d", "l")
      .first()

    row.getInt(0) should be (null)
    row.getVarInt(1) should be (null)
    row.getString(2) should be (null)
    row.getDate(3) should be (null)
    row.getList[Int](4) should be (new java.util.ArrayList[Int]())
  }

  it should "allow to fetch UDT columns" in {
    val result = javaFunctions(sc)
      .cassandraTable(ks, "udts")
      .select("key", "name", "addr").collect()

    result should have length 1
    val row = result.asScala.head
    row.getInt(0) should be(1)
    row.getString(1) should be("name")

    val udtValue = row.getUDTValue(2)
    udtValue.size should be(3)
    udtValue.getString("street") should be("Some Street")
    udtValue.getString("city") should be("Paris")
    udtValue.getInt("zip") should be(11120)
  }

  it should "allow to fetch tuple columns" in {
    val result = javaFunctions(sc)
      .cassandraTable(ks, "tuples")
      .select("key", "value").collect()

    result should have length 1
    val row = result.asScala.head
    row.getInt(0) should be(1)

    val tupleValue = row.getTupleValue(1)
    tupleValue.size should be(2)
    tupleValue.getInt(0) shouldBe 1
    tupleValue.getString(1) shouldBe "first"
  }

  it should "allow to read Cassandra table as Array of KV tuples of a case class and a tuple grouped by partition key" in {

    val f = new JFunction[CassandraRow, Int]() {
      override def call(row: CassandraRow) = row.getInt("key")
    }

    val results = javaFunctions(sc)
      .cassandraTable(ks, "wide_rows")
      .select("key", "group", "value")
      .spanBy[Int](f, classOf[Int])
      .collect()
      .asScala
      .toMap

    results should have size 2
    results should contain key 10
    results should contain key 20
    results(10).asScala.size should be(3)
    results(10).asScala.map(_.getInt("group")).toSeq should be(Seq(10, 11, 12))
    results(20).asScala.size should be(3)
    results(20).asScala.map(_.getInt("group")).toSeq should be(Seq(20, 21, 22))
  }

  it should "allow to set limit" in {
    val limit = 1
    val rdd = javaFunctions(sc).cassandraTable(ks, "limit_test_table").limit(limit.toLong)
    val result = rdd.collect()
    result.size shouldBe <= (rdd.getNumPartitions * limit)
  }

  it should "allow to set ascending ordering" in {
    val rdd = javaFunctions(sc).cassandraTable(ks, "wide_rows").where("key=10").withAscOrder
    val result = rdd.collect().asScala
    result(0).getInt("group") should be(10)
  }

  it should "allow to set descending ordering" in {
    val rdd = javaFunctions(sc).cassandraTable(ks, "wide_rows").where("key=20").withDescOrder
    val result = rdd.collect().asScala
    result(0).getInt("group") should be(22)
  }
}
