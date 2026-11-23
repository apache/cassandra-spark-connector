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

import com.datastax.oss.driver.api.core.CqlIdentifier.fromInternal
import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.analysis.{NoSuchNamespaceException, TableAlreadyExistsException}

import scala.jdk.CollectionConverters._
import scala.collection.mutable.{Map => MutableMap}

class CassandraCatalogTableSpec extends CassandraCatalogSpecBase {

  "A Cassandra Catalog Table Support" should "initialize successfully" in {
    spark.sessionState.catalogManager.currentCatalog.name() should be(defaultCatalog)
  }

  val testTable = "testTable"

  it should "create a table with a partition key" in {
    createDefaultKs()
    spark.sql(s"CREATE TABLE $defaultKs.$testTable (key Int, value STRING) USING cassandra PARTITIONED BY (key)")
    getTable(defaultKs, testTable).getPartitionKey.get(0).getName.asInternal() should be("key")
  }

  it should "create a table using the partition key prop" in {
    createDefaultKs()
    spark.sql(s"CREATE TABLE $defaultKs.$testTable (key Int, value STRING) USING cassandra TBLPROPERTIES (partition_key='key')")
    getTable(defaultKs, testTable).getPartitionKey.get(0).getName.asInternal() should be("key")
  }

  it should "create a table with a partition key and clustering key" in {
    createDefaultKs()
    spark.sql(s"CREATE TABLE $defaultKs.$testTable (key Int, value STRING) USING cassandra PARTITIONED BY (key) TBLPROPERTIES (clustering_key='value.asc')")
    val table = getTable(defaultKs, testTable)
    table.getPartitionKey.get(0).getName.asInternal() should be("key")
    table.getClusteringColumns().asScala.map { case (meta, order) => (meta.getName.asInternal(), order.name()) }.head shouldBe (("value", "ASC"))
  }

  it should "create a table with options" in {
    createDefaultKs()

    val DefaultTimeToLiveOption = "default_time_to_live" -> "33"
    val CompactionOption = "compaction" -> Map(
      "bucket_high" -> "42",
      "class" -> "org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy",
      "max_threshold" -> "11",
      "min_threshold" -> "3"
    )

    val options: List[(String, Any)] = List(
      DefaultTimeToLiveOption,
      CompactionOption,
      "clustering_key" -> "cc1.asc, cc2.desc, cc3.asc"
    )

    spark.sql(
      s"""CREATE TABLE $defaultKs.$testTable (
         |key_1 Int, key_2 Int, key_3 Int,
         |cc1 STRING, cc2 String, cc3 String,
         |value String) USING cassandra
         |PARTITIONED BY (key_1, key_2, key_3)
         |TBLPROPERTIES (${CassandraSourceUtil.optionsListToString(options)})""".stripMargin)

    val createdOptions = getTable(defaultKs, testTable)
      .getOptions.asScala

    createdOptions(fromInternal(DefaultTimeToLiveOption._1)) should be (DefaultTimeToLiveOption._2.toInt)
    createdOptions(fromInternal(CompactionOption._1)).asInstanceOf[java.util.Map[String, String]].asScala should contain theSameElementsAs (CompactionOption._2)
  }

  it should "create a table with multiple partition keys and clustering keys" in {
    createDefaultKs()
    spark.sql(
      s"""CREATE TABLE $defaultKs.$testTable (
         |key_1 Int, key_2 Int, key_3 Int,
         |cc1 STRING, cc2 String, cc3 String,
         |value String) USING cassandra
         |PARTITIONED BY (key_1, key_2, key_3)
         |TBLPROPERTIES (clustering_key='cc1.asc, cc2.desc, cc3.asc')""".stripMargin)
    val table = getTable(defaultKs, testTable)
    table.getPartitionKey.asScala.map(_.getName.asInternal()) should contain theSameElementsAs
      (Seq("key_1", "key_2", "key_3"))
    table.getClusteringColumns().asScala.map{ case (meta, order) => (meta.getName.asInternal(), order.name())} should contain theSameElementsAs
      (Seq(("cc1", "ASC"), ("cc2", "DESC"), ("cc3", "ASC")))
  }

  it should "create a table with multiple partition keys and clustering keys without sort order" in {
    createDefaultKs()
    spark.sql(
      s"""CREATE TABLE $defaultKs.$testTable (
         |key_1 Int, key_2 Int, key_3 Int,
         |cc1 STRING, cc2 String, cc3 String,
         |value String) USING cassandra
         |PARTITIONED BY (key_1, key_2, key_3)
         |TBLPROPERTIES (clustering_key='cc1, cc2, cc3')""".stripMargin)
    val table = getTable(defaultKs, testTable)
    table.getPartitionKey.asScala.map(_.getName.asInternal()) should contain theSameElementsAs
      (Seq("key_1", "key_2", "key_3"))
    table.getClusteringColumns().asScala.map{ case (meta, order) => (meta.getName.asInternal(), order.name())} should contain theSameElementsAs
      (Seq(("cc1", "ASC"), ("cc2", "ASC"), ("cc3", "ASC")))
  }

  it should "throw a sensible error when trying to create a table without a partition key" in {
    createDefaultKs()
    val exception = intercept[CassandraCatalogException] {
      spark.sql(
        s"""CREATE TABLE $defaultKs.$testTable (
           |key_1 Int, key_2 Int, key_3 Int,
           |cc1 STRING, cc2 String, cc3 String,
           |value String) USING cassandra
           |TBLPROPERTIES (clustering_key='cc1.asc, cc2.desc, cc3.asc')""".stripMargin)
    }
    exception.getMessage should include("need partition keys")
  }

  it should "throw an error if you try to partition on a transformed column" in {
    createDefaultKs()
    val exception = intercept[UnsupportedOperationException] {
      spark.sql(
        s"""CREATE TABLE $defaultKs.$testTable (
           |key_1 Int, key_2 Int, key_3 Int,
           |cc1 STRING, cc2 String, cc3 String,
           |value String) USING cassandra
           |PARTITIONED BY (years(key_1), key_2, key_3)
           |TBLPROPERTIES (clustering_key='cc1.asc, cc2.desc, cc3.asc')""".stripMargin)
    }
    exception.getMessage should include("can only by partitioned based on direct references to columns")
  }

  it should "throw a sensible error if you try to set a clustering column or partition key to a non existent column" in {
    createDefaultKs()
    val exception_1 = intercept[CassandraCatalogException]{
      spark.sql(
        s"""CREATE TABLE $defaultKs.$testTable
           |(key Int, value STRING) USING cassandra
           |PARTITIONED BY (key)
           |TBLPROPERTIES (clustering_key='nonexistent')""".stripMargin)
    }
    exception_1.getMessage should include ("clustering key but it does not exist in the schema")

    val exception_2 = intercept[CassandraCatalogException] {
      spark.sql(
        s"""CREATE TABLE $defaultKs.$testTable
           |(key Int, value STRING) USING cassandra
           |TBLPROPERTIES (partition_key='nonexistent')""".stripMargin)
    }
    exception_2.getMessage should include ("partition key but it does not exist in the schema")

  }

  it should "throw an error on a bad clustering column annotation" in {
    createDefaultKs()
    val exception = intercept[CassandraCatalogException] {
      spark.sql(
        s"""CREATE TABLE $defaultKs.$testTable (
           |key_1 Int, key_2 Int, key_3 Int,
           |cc1 STRING, cc2 String, cc3 String,
           |value String) USING cassandra
           |PARTITIONED BY (key_1, key_2, key_3)
           |TBLPROPERTIES (clustering_key='cc1.reversi, cc2.desc, cc3.asc')""".stripMargin)
    }
    exception.getMessage should include("must be ASC or DESC or blank")

    val exception2 = intercept[CassandraCatalogException] {
      spark.sql(
        s"""CREATE TABLE $defaultKs.$testTable (
           |key_1 Int, key_2 Int, key_3 Int,
           |cc1 STRING, cc2 String, cc3 String,
           |value String) USING cassandra
           |PARTITIONED BY (key_1, key_2, key_3)
           |TBLPROPERTIES (clustering_key='cc1.asc.desc, cc2.desc, cc3.asc')""".stripMargin)
    }
    exception2.getMessage should include("too many components")
  }

  it should "throw a table already exists exception if the table already exists" in {
    createDefaultKs()
    spark.sql(s"CREATE TABLE $defaultKs.$testTable (key Int, value STRING) USING cassandra PARTITIONED BY (key)")
    intercept[TableAlreadyExistsException] {
      spark.sql(s"CREATE TABLE $defaultKs.$testTable (key Int, value STRING) USING cassandra PARTITIONED BY (key)")
    }
  }

  it should "list tables in a keyspace" in {
    createDefaultKs()
    spark.sql(s"CREATE TABLE $defaultKs.${testTable}_1 (key Int, value STRING) USING cassandra PARTITIONED BY (key)")
    spark.sql(s"CREATE TABLE $defaultKs.${testTable}_2 (key Int, value STRING) USING cassandra PARTITIONED BY (key)")
    spark.sql(s"CREATE TABLE $defaultKs.${testTable}_3 (key Int, value STRING) USING cassandra PARTITIONED BY (key)")
    val results = spark.sql(s"SHOW TABLES from $defaultKs").collect()

    val expected = getMetadata()
      .getKeyspace(fromInternal(defaultKs)).get.getTables
      .keySet().asScala
      .map(_.asInternal())
    expected.size should be(3)

    results.map(_.getString(1)) should contain theSameElementsAs expected
  }

  it should "throw no such namespace when listing tables from a non existent namespace" in {
    intercept[NoSuchNamespaceException] {
      val results = spark.sql(s"SHOW TABLES from nonexistent").collect()
    }
  }

  it should "describe a table" in {
    createDefaultKs()
    spark.sql(
      s"""CREATE TABLE $defaultKs.$testTable (
         |key_1 Int, key_2 Int, key_3 Int,
         |cc1 STRING, cc2 String, cc3 String,
         |value String) USING cassandra
         |PARTITIONED BY (key_1, key_2, key_3)
         |TBLPROPERTIES (clustering_key='cc1.asc, cc2.desc, cc3.asc')""".stripMargin)
    val rows = spark.sql(s"DESCRIBE EXTENDED $defaultKs.$testTable").collect()

    val beforePartitionInformationMap = MutableMap[String, String]()
    val afterPartitionInformationMap = MutableMap[String, String]()
    val afterDetailedTableInformationMap = MutableMap[String, String]()

    var afterPartitionInformation = false
    var afterDetailedTableInformation = false

    rows.foreach(r => {
      if (r.getString(0) == "# Partition Information") {
        afterPartitionInformation = true
      } else if(r.getString(0) == "# Detailed Table Information") {
        afterDetailedTableInformation = true
      } else {
        if (afterDetailedTableInformation) {
          afterDetailedTableInformationMap.put(r.getString(0), r.getString(1))
        } else if (afterPartitionInformation) {
          afterPartitionInformationMap.put(r.getString(0), r.getString(1))
        } else {
          beforePartitionInformationMap.put(r.getString(0), r.getString(1))
        }
      }
    })

    val expectedColumns = Seq(
      ("key_1", "int"), ("key_2", "int"), ("key_3", "int"),
      ("cc3", "string"), ("cc2", "string"), ("cc1", "string"),
      ("value", "string"),
    )
    val expectedPartitionColumns = Seq(
      ("key_1", "int"), ("key_2", "int"), ("key_3", "int"),
    )

    for ((key, value) <- expectedColumns) {
      beforePartitionInformationMap.get(key) should be (Some(value))
    }
    for ((key, value) <- expectedPartitionColumns) {
      afterPartitionInformationMap.get(key) should be(Some(value))
    }
    afterDetailedTableInformationMap.get("Name") should be(Some("testTable"))
  }

  it should "describe table properties" in {
    createDefaultKs()
    spark.sql(
      s"""CREATE TABLE $defaultKs.$testTable (
         |key_1 Int, key_2 Int, key_3 Int,
         |cc1 STRING, cc2 String, cc3 String,
         |value String) USING cassandra
         |PARTITIONED BY (key_1, key_2, key_3)
         |TBLPROPERTIES (clustering_key='cc1.asc, cc2.desc, cc3.asc')""".stripMargin)
    val rows = spark.sql(s"SHOW TBLPROPERTIES $defaultKs.$testTable").collect()

    val results = rows.map( row => (row.getString(0), CassandraSourceUtil.parseProperty(row.getString(1)))).toMap
    results("clustering_key").asInstanceOf[String] should include ("cc1.ASC,cc2.DESC,cc3.ASC")
    results("gc_grace_seconds").asInstanceOf[String] should be("864000")
  }

  it should "alter table properties" in {
    createDefaultKs()
    spark.sql(
      s"""CREATE TABLE $defaultKs.$testTable (
         |key_1 Int, key_2 Int, key_3 Int,
         |cc1 STRING, cc2 String, cc3 String,
         |value String) USING cassandra
         |PARTITIONED BY (key_1, key_2, key_3)
         |TBLPROPERTIES (clustering_key='cc1.asc, cc2.desc, cc3.asc')""".stripMargin)

    spark.sql(
      s"""ALTER TABLE $defaultKs.$testTable SET TBLPROPERTIES ('gc_grace_seconds' = '22')"""
    )
    val result = getTable(defaultKs, testTable)
      .getOptions
      .get(fromInternal("gc_grace_seconds"))
      .asInstanceOf[Int].toString

    result should be("22")
  }

  it should "alter map table properties" in {
    createDefaultKs()
    spark.sql(
      s"""CREATE TABLE $defaultKs.$testTable (
         |key_1 Int, key_2 Int, key_3 Int,
         |cc1 STRING, cc2 String, cc3 String,
         |value String) USING cassandra
         |PARTITIONED BY (key_1, key_2, key_3)
         |TBLPROPERTIES (clustering_key='cc1.asc, cc2.desc, cc3.asc')""".stripMargin)

    spark.sql(
      s"""ALTER TABLE $defaultKs.$testTable SET TBLPROPERTIES (
         |'compaction'='{class=SizeTieredCompactionStrategy,bucket_high=1001}'
         |)""".stripMargin
    )

    val compactionOptions = getTable(defaultKs, testTable)
      .getOptions.asScala
      .get(fromInternal("compaction"))
      .get
      .asInstanceOf[java.util.Map[String, AnyRef]]
      .asScala

    compactionOptions.get("bucket_high").get.toString should be ("1001")
  }

  it should "alter add columns" in {
    createDefaultKs()
    spark.sql(
      s"""CREATE TABLE $defaultKs.$testTable (
         |key_1 Int, key_2 Int, key_3 Int,
         |cc1 STRING, cc2 String, cc3 String,
         |value String) USING cassandra
         |PARTITIONED BY (key_1, key_2, key_3)
         |TBLPROPERTIES (clustering_key='cc1.asc, cc2.desc, cc3.asc')""".stripMargin)

    spark.sql(
      s"""ALTER TABLE $defaultKs.$testTable ADD COLUMNS (newCol INT) """.stripMargin
    )

    val expectedColumns = Seq("key_1", "key_2", "key_3", "cc1", "cc2", "cc3", "value", "newCol")

    val table = getTable(defaultKs, testTable)

    expectedColumns.foreach(col => table.getColumn(fromInternal(col)).isPresent should be(true))
  }

  it should "alter remove columns" in {
    createDefaultKs()
    spark.sql(
      s"""CREATE TABLE $defaultKs.$testTable (
         |key_1 Int, key_2 Int, key_3 Int,
         |cc1 STRING, cc2 String, cc3 String,
         |value String) USING cassandra
         |PARTITIONED BY (key_1, key_2, key_3)
         |TBLPROPERTIES (clustering_key='cc1.asc, cc2.desc, cc3.asc')""".stripMargin)

    spark.sql(
      s"""ALTER TABLE $defaultKs.$testTable DROP COLUMNS (value) """.stripMargin
    )

    getTable(defaultKs, testTable)
      .getColumn(fromInternal("value"))
      .isPresent shouldBe (false)
  }

  it should "throw exceptions when removing illegal columns" in {
    createDefaultKs()
    spark.sql(
      s"""CREATE TABLE $defaultKs.$testTable (
         |key_1 Int, key_2 Int, key_3 Int,
         |cc1 STRING, cc2 String, cc3 String,
         |value String) USING cassandra
         |PARTITIONED BY (key_1, key_2, key_3)
         |TBLPROPERTIES (clustering_key='cc1.asc, cc2.desc, cc3.asc')""".stripMargin)

    val excep = intercept[SparkException] {
      spark.sql(
        s"""ALTER TABLE $defaultKs.$testTable DROP COLUMNS (key_1) """.stripMargin
      )
    }
    excep.getCause.getMessage should include ("cannot drop primary key")
  }
}
