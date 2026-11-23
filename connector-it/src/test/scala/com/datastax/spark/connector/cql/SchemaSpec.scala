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

package com.datastax.spark.connector.cql

import com.datastax.spark.connector.SparkCassandraITWordSpecBase
import com.datastax.spark.connector.ccm.CcmConfig
import com.datastax.spark.connector.cluster.DefaultCluster
import com.datastax.spark.connector.types._
import com.datastax.spark.connector.util.schemaFromCassandra
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions.col
import org.scalatest.Inspectors._

class SchemaSpec extends SparkCassandraITWordSpecBase with DefaultCluster {
  override lazy val conn = CassandraConnector(defaultConf)

  conn.withSessionDo { session =>
    createKeyspace(session)

    session.execute(
      s"""CREATE TYPE $ks.address (street varchar, city varchar, zip int)""")
    session.execute(
      s"""CREATE TABLE $ks.test(
         |  k1 int,
         |  k2 varchar,
         |  k3 timestamp,
         |  c1 bigint,
         |  c2 varchar,
         |  c3 uuid,
         |  d1_blob blob,
         |  d2_boolean boolean,
         |  d3_decimal decimal,
         |  d4_double double,
         |  d5_float float,
         |  d6_inet inet,
         |  d7_int int,
         |  d8_list list<int>,
         |  d9_map map<int, varchar>,
         |  d10_set frozen<set<int>>,
         |  d11_timestamp timestamp,
         |  d12_uuid uuid,
         |  d13_timeuuid timeuuid,
         |  d14_varchar varchar,
         |  d15_varint varint,
         |  d16_address frozen<address>,
         |  PRIMARY KEY ((k1, k2, k3), c1, c2, c3)
         |)
      """.stripMargin)
    session.execute(
      s"""CREATE INDEX test_d9_map_idx ON $ks.test (keys(d9_map))""")
    session.execute(
      s"""CREATE INDEX test_d9_m23423ap_idx ON $ks.test (full(d10_set))""")
    session.execute(
      s"""CREATE INDEX test_d7_int_idx ON $ks.test (d7_int)""")
    from(Some(CcmConfig.V5_0_0), None) {
      session.execute(s"ALTER TABLE $ks.test ADD d17_vector frozen<vector<int,3>>")
    }

    for (i <- 0 to 9) {
      session.execute(s"insert into $ks.test (k1,k2,k3,c1,c2,c3,d10_set) " +
        s"values ($i, 'text$i', $i, $i, 'text$i', 123e4567-e89b-12d3-a456-42661417400$i, {$i, ${i*10}})")
    }
  }

  val schema = schemaFromCassandra(conn)

  "A Schema" should {
    "allow to get a list of keyspaces" in {
      schema.keyspaces.map(_.keyspaceName) should contain(ks)
    }
    "allow to look up a keyspace by name" in {
      val keyspace = schema.keyspaceByName(ks)
      keyspace.keyspaceName shouldBe ks
    }
  }

  "A KeyspaceDef" should {
    "allow to get a list of tables in the given keyspace" in {
      val keyspace = schema.keyspaceByName(ks)
      keyspace.tables.map(_.tableName) shouldBe Set("test")
    }
    "allow to look up a table by name" in {
      val keyspace = schema.keyspaceByName(ks)
      val table = keyspace.tableByName("test")
      table.tableName shouldBe "test"
    }
    "allow to look up user type by name" in {
      val keyspace = schema.keyspaceByName(ks)
      val userType = keyspace.userTypeByName("address")
      userType.name shouldBe "address"
    }
  }

  "A TableDef" should {
    val keyspace = schema.keyspaceByName(ks)
    val table = keyspace.tableByName("test")

    "allow to read column definitions by name" in {
      table.columnByName("k1").columnName shouldBe "k1"
    }

    "allow to read primary key column definitions" in {
      table.primaryKey.size shouldBe 6
      table.primaryKey.map(_.columnName) shouldBe Seq(
        "k1", "k2", "k3", "c1", "c2", "c3")
      table.primaryKey.map(_.columnType) shouldBe Seq(
        IntType, VarCharType, TimestampType, BigIntType, VarCharType, UUIDType)
      forAll(table.primaryKey) { c => c.isPrimaryKeyColumn shouldBe true }
    }

    "allow to read partitioning key column definitions" in {
      table.partitionKey.size shouldBe 3
      table.partitionKey.map(_.columnName) shouldBe Seq("k1", "k2", "k3")
      forAll(table.partitionKey) { c => c.isPartitionKeyColumn shouldBe true }
      forAll(table.partitionKey) { c => c.isPrimaryKeyColumn shouldBe true }
    }

    "allow to read regular column definitions" in {
      val columns = table.regularColumns
      columns.size should be >= 16
      columns.map(_.columnName).toSet should contain allElementsOf Set(
        "d1_blob", "d2_boolean", "d3_decimal", "d4_double", "d5_float",
        "d6_inet", "d7_int", "d8_list", "d9_map", "d10_set",
        "d11_timestamp", "d12_uuid", "d13_timeuuid", "d14_varchar",
        "d15_varint", "d16_address")
    }

    "allow to read proper types of columns" in {
      table.columnByName("d1_blob").columnType shouldBe BlobType
      table.columnByName("d2_boolean").columnType shouldBe BooleanType
      table.columnByName("d3_decimal").columnType shouldBe DecimalType
      table.columnByName("d4_double").columnType shouldBe DoubleType
      table.columnByName("d5_float").columnType shouldBe FloatType
      table.columnByName("d6_inet").columnType shouldBe InetType
      table.columnByName("d7_int").columnType shouldBe IntType
      table.columnByName("d8_list").columnType shouldBe ListType(IntType)
      table.columnByName("d9_map").columnType shouldBe MapType(IntType, VarCharType)
      table.columnByName("d10_set").columnType shouldBe SetType(IntType, true)
      table.columnByName("d11_timestamp").columnType shouldBe TimestampType
      table.columnByName("d12_uuid").columnType shouldBe UUIDType
      table.columnByName("d13_timeuuid").columnType shouldBe TimeUUIDType
      table.columnByName("d14_varchar").columnType shouldBe VarCharType
      table.columnByName("d15_varint").columnType shouldBe VarIntType
      table.columnByName("d16_address").columnType shouldBe a [UserDefinedType]
      from(Some(CcmConfig.V5_0_0), None) {
        table.columnByName("d17_vector").columnType shouldBe VectorType(IntType, 3)
      }
    }

    "allow to list fields of a user defined type" in {
      val udt = table.columnByName("d16_address").columnType.asInstanceOf[UserDefinedType]
      udt.columnNames shouldBe Seq("street", "city", "zip")
      udt.columnTypes shouldBe Seq(VarCharType, VarCharType, IntType)
    }

    "should recognize column with collection index as indexed" in {
      table.indexedColumns.size shouldBe 3
      table.indexedColumns.map(_.columnName).toSet shouldBe Set("d7_int", "d9_map", "d10_set")
    }

    "allow for pushdown on frozen indexed collection" in {
      val value1 = spark.read.cassandraFormat(table.tableName, ks).load().where(col("d10_set").equalTo(Array(3, 30)))
      value1.explain()
      value1.collect().size shouldBe 1
    }

    "should hold all indices retrieved from cassandra" in {
      table.indexes.size shouldBe 3
    }
  }

}
