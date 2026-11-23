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

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector._
import com.datastax.spark.connector.cluster.DefaultCluster

class TableWriterColumnNamesSpec extends SparkCassandraITAbstractSpecBase with DefaultCluster {

  override lazy val conn = CassandraConnector(defaultConf)

  case class KeyValue(key: Int, group: Long)

  override def beforeClass {
    conn.withSessionDo { session =>
      createKeyspace(session)
      session.execute(s"""CREATE TABLE $ks.key_value (key INT, group BIGINT, value TEXT, PRIMARY KEY (key, group))""")
      session.execute(s"""TRUNCATE $ks.key_value""")
    }
  }

  "TableWriter" must {
    "distinguish `AllColumns`" in {
      val all = Vector("key", "group", "value")

      val writer = TableWriter(
        conn,
        keyspaceName = ks,
        tableName = "key_value",
        columnNames = AllColumns,
        writeConf = WriteConf()
      )

      writer.columnNames.size should be (all.size)
      writer.columnNames should be(all)
    }

    "distinguish and use only specified column names if provided" in {
      val subset = Seq("key": ColumnRef, "group": ColumnRef)

      val writer = TableWriter(
        conn,
        keyspaceName = ks,
        tableName = "key_value",
        columnNames = SomeColumns(subset: _*),
        writeConf = WriteConf()
      )

      writer.columnNames.size should be (subset.size)
      writer.columnNames should be (Vector("key", "group"))
    }

    "distinguish and use only specified column names if provided, when aliases are specified" in {
      val subset = Seq[ColumnRef]("key" as "keyAlias", "group" as "groupAlias")

      val writer = TableWriter(
        conn,
        keyspaceName = ks,
        tableName = "key_value",
        columnNames = SomeColumns(subset: _*),
        writeConf = WriteConf()
      )

      writer.columnNames.size should be (subset.size)
      writer.columnNames should be (Vector("key", "group"))
    }

    "fail in the RowWriter if provided specified column names do not include primary keys" in {
      import com.datastax.spark.connector._

      intercept[IllegalArgumentException] {
        sc.parallelize(Seq((1, 1L, None))).saveToCassandra(ks, "key_value", SomeColumns("key", "value"))
      }
    }

    "do not use TTL when it is not specified" in {
      val writer = TableWriter(
        conn,
        keyspaceName = ks,
        tableName = "key_value",
        columnNames = AllColumns,
        writeConf = WriteConf(ttl = TTLOption.defaultValue, timestamp = TimestampOption.defaultValue)
      )

      writer.queryTemplateUsingInsert should endWith (""")""")
    }

    "use static TTL if it is specified" in {
      val writer = TableWriter(
        conn,
        keyspaceName = ks,
        tableName = "key_value",
        columnNames = AllColumns,
        writeConf = WriteConf(ttl = TTLOption.constant(1234), timestamp = TimestampOption.defaultValue)
      )

      writer.queryTemplateUsingInsert should endWith (""") USING TTL 1234""")
    }

    "use static timestamp if it is specified" in {
      val writer = TableWriter(
        conn,
        keyspaceName = ks,
        tableName = "key_value",
        columnNames = AllColumns,
        writeConf = WriteConf(ttl = TTLOption.defaultValue, timestamp = TimestampOption.constant(1400000000000L))
      )

      writer.queryTemplateUsingInsert should endWith (""") USING TIMESTAMP 1400000000000""")
    }

    "use both static TTL and static timestamp when they are specified" in {
      val writer = TableWriter(
        conn,
        keyspaceName = ks,
        tableName = "key_value",
        columnNames = AllColumns,
        writeConf = WriteConf(ttl = TTLOption.constant(1234), timestamp = TimestampOption.constant(1400000000000L))
      )

      writer.queryTemplateUsingInsert should endWith (""") USING TTL 1234 AND TIMESTAMP 1400000000000""")
    }

    "use per-row TTL and timestamp when the row writer provides them" in {
      val writer = TableWriter(
        conn,
        keyspaceName = ks,
        tableName = "key_value",
        columnNames = AllColumns,
        writeConf = WriteConf(ttl = TTLOption.perRow("ttl_column"), timestamp = TimestampOption.perRow("timestamp_column"))
      )

      writer.queryTemplateUsingInsert should endWith (""") USING TTL :ttl_column AND TIMESTAMP :timestamp_column""")
    }

    "use per-row TTL and static timestamp" in {
      val writer = TableWriter(
        conn,
        keyspaceName = ks,
        tableName = "key_value",
        columnNames = AllColumns,
        writeConf = WriteConf(ttl = TTLOption.perRow("ttl_column"), timestamp = TimestampOption.constant(1400000000000L))
      )

      writer.queryTemplateUsingInsert should endWith (""") USING TTL :ttl_column AND TIMESTAMP 1400000000000""")
    }

    "use per-row timestamp and static TTL" in {
      val writer = TableWriter(
        conn,
        keyspaceName = ks,
        tableName = "key_value",
        columnNames = AllColumns,
        writeConf = WriteConf(ttl = TTLOption.constant(1234), timestamp = TimestampOption.perRow("timestamp_column"))
      )

      writer.queryTemplateUsingInsert should endWith (""") USING TTL 1234 AND TIMESTAMP :timestamp_column""")
    }

    "use per-row TTL" in {
      val writer = TableWriter(
        conn,
        keyspaceName = ks,
        tableName = "key_value",
        columnNames = AllColumns,
        writeConf = WriteConf(ttl = TTLOption.perRow("ttl_column"), timestamp = TimestampOption.defaultValue)
      )

      writer.queryTemplateUsingInsert should endWith (""") USING TTL :ttl_column""")
    }

    "use per-row timestamp" in {
      val writer = TableWriter(
        conn,
        keyspaceName = ks,
        tableName = "key_value",
        columnNames = AllColumns,
        writeConf = WriteConf(ttl = TTLOption.defaultValue, timestamp = TimestampOption.perRow("timestamp_column"))
      )

      writer.queryTemplateUsingInsert should endWith (""") USING TIMESTAMP :timestamp_column""")
    }
  }
}
