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

package com.datastax.spark.connector.sql

import scala.concurrent.Future
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.oss.driver.api.core.DefaultProtocolVersion
import com.datastax.spark.connector.cluster.DefaultCluster

class CassandraSQLSpec extends SparkCassandraITFlatSpecBase with DefaultCluster {

  override lazy val conn = CassandraConnector(defaultConf)

  val ks1 = ks + "_1"
  val ks2 = ks + "_2"

  override def beforeClass {

    conn.withSessionDo { session =>
      createKeyspace(session, ks1)
      createKeyspace(session, ks2)
      createKeyspace(session, """"sqlQuotedKeyspace"""")
      awaitAll(
        Future {
          session.execute("""CREATE TABLE "sqlQuotedKeyspace".test (id int PRIMARY KEY, t Tuple<text, int>)""")
          session.execute("""CREATE TABLE "sqlQuotedKeyspace".test_copy (id int PRIMARY KEY, t Tuple<text, int>)""")
          session.execute("""INSERT INTO "sqlQuotedKeyspace".test (id, t) VALUES (1, ('xyz', 2))""")
        },
        Future {
          session.execute( s"""CREATE TABLE $ks1.test1 (a INT, b INT, c INT, d INT, e INT, f INT, g INT, h INT, PRIMARY KEY ((a, b, c), d , e, f))""")
          session.execute( s"""CREATE INDEX test1_g ON $ks1.test1(g)""")
          session.execute( s"""INSERT INTO $ks1.test1 (a, b, c, d, e, f, g, h) VALUES (1, 1, 1, 1, 1, 1, 1, 1)""")
          session.execute( s"""INSERT INTO $ks1.test1 (a, b, c, d, e, f, g, h) VALUES (1, 1, 1, 1, 2, 1, 1, 2)""")
          session.execute( s"""INSERT INTO $ks1.test1 (a, b, c, d, e, f, g, h) VALUES (1, 1, 1, 2, 1, 1, 2, 1)""")
          session.execute( s"""INSERT INTO $ks1.test1 (a, b, c, d, e, f, g, h) VALUES (1, 1, 1, 2, 2, 1, 2, 2)""")
          session.execute( s"""INSERT INTO $ks1.test1 (a, b, c, d, e, f, g, h) VALUES (1, 2, 1, 1, 1, 2, 1, 1)""")
          session.execute( s"""INSERT INTO $ks1.test1 (a, b, c, d, e, f, g, h) VALUES (1, 2, 1, 1, 2, 2, 1, 2)""")
          session.execute( s"""INSERT INTO $ks1.test1 (a, b, c, d, e, f, g, h) VALUES (1, 2, 1, 2, 1, 2, 2, 1)""")
          session.execute( s"""INSERT INTO $ks1.test1 (a, b, c, d, e, f, g, h) VALUES (1, 2, 1, 2, 2, 2, 2, 2)""")
        },
        Future {
          session.execute( s"""CREATE TABLE $ks1.test2 (a INT, b INT, c INT, name TEXT, PRIMARY KEY (a, b))""")
          session.execute( s"""INSERT INTO $ks1.test2 (a, b, c, name) VALUES (1, 1, 1, 'Tom')""")
          session.execute( s"""INSERT INTO $ks1.test2 (a, b, c, name) VALUES (1, 2, 3, 'Larry')""")
          session.execute( s"""INSERT INTO $ks1.test2 (a, b, c, name) VALUES (1, 3, 3, 'Henry')""")
          session.execute( s"""INSERT INTO $ks1.test2 (a, b, c, name) VALUES (2, 1, 3, 'Jerry')""")
          session.execute( s"""INSERT INTO $ks1.test2 (a, b, c, name) VALUES (2, 2, 3, 'Alex')""")
          session.execute( s"""INSERT INTO $ks1.test2 (a, b, c, name) VALUES (2, 3, 3, 'John')""")
          session.execute( s"""INSERT INTO $ks1.test2 (a, b, c, name) VALUES (3, 1, 3, 'Jack')""")
          session.execute( s"""INSERT INTO $ks1.test2 (a, b, c, name) VALUES (3, 2, 3, 'Hank')""")
          session.execute( s"""INSERT INTO $ks1.test2 (a, b, c, name) VALUES (3, 3, 3, 'Dug')""")
        },
        Future {
          session.execute(
            s"""CREATE TABLE $ks1.test3 (a INT, b INT, c INT, PRIMARY KEY (a,
               |b))""".stripMargin)
        },
        Future {
          skipIfProtocolVersionLT(DefaultProtocolVersion.V4) {
            session.execute(s"""
            | CREATE TABLE $ks1.test_data_type (
            | a ASCII,
            | b INT,
            | c FLOAT,
            | d DOUBLE,
            | e BIGINT,
            | f BOOLEAN,
            | g DECIMAL,
            | h INET,
            | i TEXT,
            | j TIMESTAMP,
            | k UUID,
            | l VARINT,
            | m SMALLINT,
            | n TINYINT,
            | PRIMARY KEY ((a), b, c)
            |)""".stripMargin)
            session.execute(s"""
            | INSERT INTO $ks1.test_data_type (a, b, c, d, e, f, g, h, i, j, k, l, m, n)
            | VALUES (
            | 'ascii',
            | 10,
            | 12.34,
            | 12.3456789,
            | 123344556,
            | true,
            | 12.36,
            | '74.125.239.135',
            | 'text',
            | '2011-02-03 04:05+0000',
            | 123e4567-e89b-12d3-a456-426655440000,
            | 123456,
            | 22,
            | 4
            |)""".
                stripMargin)}
        },
        Future {
          skipIfProtocolVersionLT(DefaultProtocolVersion.V4){
          session.execute(s"""
              | CREATE TABLE $ks1.test_data_type1 (
              | a ASCII,
              | b INT,
              | c FLOAT,
              | d DOUBLE,
              | e BIGINT,
              | f BOOLEAN,
              | g DECIMAL,
              | h INET,
              | i TEXT,
              | j TIMESTAMP,
              | k UUID,
              | l VARINT,
              | m SMALLINT,
              | n TINYINT,
              | PRIMARY KEY ((a), b, c)
              |)""".
              stripMargin)}
        },
        Future {
          session.execute(s"""
               | CREATE TABLE $ks1.test_collection (
               |  a INT,
               |  b SET<TIMESTAMP>,
               |  c MAP<TIMESTAMP, TIMESTAMP>,
               |  d List<TIMESTAMP>,
               |  PRIMARY KEY (a)
               |)"""
            .stripMargin)
          session.execute(s"""
               | INSERT INTO $ks1.test_collection (a, b, c, d)
               | VALUES (
               |  1,
               |  {'2011-02-03','2011-02-04'},
               |  {'2011-02-03':'2011-02-04', '2011-02-06':'2011-02-07'},
               |  ['2011-02-03','2011-02-04']
               |)"""
            .stripMargin)
        },
        Future {
          session.execute(s"""
            | CREATE TYPE $ks1.address (street text, city text, zip int, date TIMESTAMP)"""
            .stripMargin)
          session.execute(s"""
              | CREATE TABLE $ks1.udts (key INT PRIMARY KEY, name text, addr frozen<address>)"""
            .stripMargin)
          session.execute (s"""
              | INSERT INTO $ks1.udts (key, name, addr)
              | VALUES (1, 'name', {street: 'Some Street', city: 'Paris', zip: 11120})"""
            .stripMargin)
        },
        Future {
          session.execute(s"""
               | CREATE TYPE $ks1.category_metadata (
               |  category_id text,
               |  metric_descriptors list <text>
               |)"""
            .stripMargin)
          session.execute(s"""
               | CREATE TYPE $ks1.object_metadata (
               |  name text,
               |  category_metadata frozen<category_metadata>,
               |  bucket_size int
               |)"""
            .stripMargin)
          session.execute(s"""
               | CREATE TYPE $ks1.relation (
               |  type text,
               |  object_type text,
               |  related_to text,
               |  obj_id text
               |)""".
              stripMargin)
          session.execute(s"""
               | CREATE TABLE $ks1.objects (
               |  obj_id text,
               |  metadata  frozen<object_metadata>,
               |  relations list<frozen<relation>>,
               |  ts timestamp, PRIMARY KEY(obj_id)
               |)"""
            .stripMargin)
          session.execute(s"""
               | INSERT INTO $ks1.objects (obj_id, ts, metadata, relations)
               | values (
               |  '123', '2015-06-16 15:53:23-0400',
               |  {
               |    name: 'foo',
               |    category_metadata: {
               |      category_id: 'thermostat',
               |      metric_descriptors: []
               |    },
               |    bucket_size: 0
               |  },
               |  [
               |    {
               |      type: 'a',
               |      object_type: 'b',
               |      related_to: 'c',
               |      obj_id: 'd'
               |    },
               |    {
               |      type: 'a1',
               |      object_type: 'b1',
               |      related_to: 'c1',
               |      obj_id: 'd1'
               |    }
               |  ]
               | )
               |"""
            .stripMargin)
          session.execute(s"""
               | CREATE TABLE $ks1.objects_copy (
               |  obj_id text,
               |  metadata  frozen<object_metadata>,
               |  relations list<frozen<relation>>,
               |  ts timestamp, PRIMARY KEY(obj_id)
               |)"""
            .stripMargin)
        },
        Future {
          session.execute(s"""
               |CREATE TABLE $ks1.export_table (
               |  objectid int,
               |  utcstamp timestamp,
               |  service_location_id int,
               |  service_location_name text,
               |  meterid int,
               |  primary key(meterid, utcstamp)
               |)"""
            .stripMargin)
        },
        Future {
          session.execute(s"create table $ks1.timestamp_conversion_bug (k int, v int, d timestamp, primary key(k,v))")
          session.execute(s"insert into $ks1.timestamp_conversion_bug (k, v, d) values (1, 1, '2015-01-03 15:13')")
          session.execute(s"insert into $ks1.timestamp_conversion_bug (k, v, d) values (1, 2, '2015-01-03 16:13')")
          session.execute(s"insert into $ks1.timestamp_conversion_bug (k, v, d) values (1, 3, '2015-01-03 17:13')")
          session.execute(s"insert into $ks1.timestamp_conversion_bug (k, v, d) values (1, 4, '2015-01-03 18:13')")
        },
        Future {
          session.execute (s"create table $ks1.uuid_inet_type (a UUID, b INET, c INT, primary key(a,b))")
          session.execute(s"insert into $ks1.uuid_inet_type (a, b, c) values (123e4567-e89b-12d3-a456-426655440000,'74.125.239.135', 1)")
          session.execute(s"insert into $ks1.uuid_inet_type (a, b, c) values (067e6162-3b6f-4ae2-a171-2470b63dff00, '74.125.239.136', 2)")
        },
        Future {
          session.execute(s"""
               | CREATE TABLE $ks1.varint_test(
               |  id varint,
               |  series varint,
               |  rollup_minutes varint,
               |  event text,
               |  PRIMARY KEY ((id, series, rollup_minutes), event)
               |)"""
            .stripMargin)
          session.execute(s"""
               | INSERT INTO $ks1.varint_test(id, series, rollup_minutes, event)
               | VALUES(1234567891234, 1234567891235, 1234567891236, 'event')
               |"""
            .stripMargin)
        },
        Future {
          session.execute(s"CREATE TABLE $ks1.tuple_test1 (id int PRIMARY KEY, t Tuple<text, int>)")
          session.execute(s"INSERT INTO $ks1.tuple_test1 (id, t) VALUES (1, ('xyz', 2))")
          session.execute(s"CREATE TABLE $ks1.tuple_test1_copy (id int PRIMARY KEY, t Tuple<text, int>)")
        },

        Future {
          session.execute(s"""
               |CREATE TABLE IF NOT EXISTS $ks1 .index_test (
               |  ipk1 int,
               |  pk2 int,
               |  id1 int,
               |  d2 int,
               |  PRIMARY KEY ((ipk1, pk2))
               |)"""
            .stripMargin)
          session.execute(s"CREATE INDEX IF NOT EXISTS idx_ipk1_index_test on $ks1.index_test(ipk1)")
          session.execute(s"CREATE INDEX IF NOT EXISTS idx_id1_index_test on $ks1.index_test(id1)")

          session.execute(s"INSERT INTO $ks1.index_test (ipk1, pk2, id1, d2) VALUES (1, 1, 1, 1)")
          session.execute(s"INSERT INTO $ks1.index_test (ipk1, pk2, id1, d2) VALUES (2, 2, 2, 2)")
        },
        Future {
          session.execute( s"""CREATE TABLE $ks2.test3 (a INT, b INT, c INT, PRIMARY KEY (a, b))""")
        },
        Future {
          session.execute( s"""CREATE TABLE $ks2.test2 (a INT, b INT, c INT, PRIMARY KEY (a, b))""")
          session.execute( s"""INSERT INTO $ks2.test2 (a, b, c) VALUES (1, 1, 1)""")
          session.execute( s"""INSERT INTO $ks2.test2 (a, b, c) VALUES (1, 2, 3)""")
          session.execute( s"""INSERT INTO $ks2.test2 (a, b, c) VALUES (1, 3, 3)""")
          session.execute( s"""INSERT INTO $ks2.test2 (a, b, c) VALUES (2, 1, 3)""")
          session.execute( s"""INSERT INTO $ks2.test2 (a, b, c) VALUES (2, 2, 3)""")
          session.execute( s"""INSERT INTO $ks2.test2 (a, b, c) VALUES (2, 3, 3)""")
          session.execute( s"""INSERT INTO $ks2.test2 (a, b, c) VALUES (3, 1, 3)""")
          session.execute( s"""INSERT INTO $ks2.test2 (a, b, c) VALUES (3, 2, 3)""")
          session.execute( s"""INSERT INTO $ks2.test2 (a, b, c) VALUES (3, 3, 3)""")
        }
      )
    }

    Seq("index_test", "varint_test", "timestamp_conversion_bug", "uuid_inet_type", "export_table",
      "objects_copy", "udts", "test_collection",  "tuple_test1",
      "test3", "test2", "test1")
        .foreach(t => spark.read.cassandraFormat(t, ks1).load().createOrReplaceTempView(s"ks1_$t"))
    Seq("test3", "test2")
        .foreach(t => spark.read.cassandraFormat(t, ks2).load().createOrReplaceTempView(s"ks2_$t"))
    skipIfProtocolVersionLT(DefaultProtocolVersion.V4) {
      Seq("test_data_type1", "test_data_type")
       .foreach(t => spark.read.cassandraFormat(t, ks1).load().createOrReplaceTempView(s"ks1_$t"))
    }
  }

  "SqlContext" should "allow to select all rows" in {
    val result = spark.sql(s"SELECT * FROM ks1_test1").collect()
    result should have length 8
  }

  it should "allow to select rows with index columns" in {
    val result = spark.sql(s"SELECT * FROM ks1_test1 WHERE g = 2").collect()
    result should have length 4
  }

  it should "allow to select rows with indexed columns that do not belong to partition key" in {
    val result = spark.sql(s"SELECT * FROM ks1_index_test WHERE id1 = 2").collect()
    result should have length 1
  }

  it should "allow to select rows with indexed columns that belong to partition key" in {
    val result = spark.sql(s"SELECT * FROM ks1_index_test WHERE ipk1 = 2").collect()
    result should have length 1
  }

  it should "allow to select rows with indexed partition and regular columns" in {
    val result = spark.sql(s"SELECT * FROM ks1_index_test WHERE ipk1 = 2 and id1 = 2").collect()
    result should have length 1
  }

  it should "allow to select rows with >= clause" in {
    val result = spark.sql(s"SELECT * FROM ks1_test1 WHERE b >= 2").collect()
    result should have length 4
  }

  it should "allow to select rows with > clause" in {
    val result = spark.sql(s"SELECT * FROM ks1_test1 WHERE b > 2").collect()
    result should have length 0
  }

  it should "allow to select rows with < clause" in {
    val result = spark.sql(s"SELECT * FROM ks1_test1 WHERE b < 2").collect()
    result should have length 4
  }

  it should "allow to select rows with <= clause" in {
    val result = spark.sql(s"SELECT * FROM ks1_test1 WHERE b <= 2").collect()
    result should have length 8
  }

  it should "allow to select rows with in clause" in {
    val result = spark.sql(s"SELECT * FROM ks1_test1 WHERE b in (1,2)").collect()
    result should have length 8
  }

  it should "allow to select rows with in clause pushed down" in {
    val query = spark.sql(s"SELECT * FROM ks1_test2 WHERE a in (1,2)")
    query.queryExecution.sparkPlan.toString should not include ("Filter (") // No Spark Filter Step
    val result = query.collect()
    result should have length 6
  }

  it should "allow to select rows with or clause" in {
    val result = spark.sql(s"SELECT * FROM ks1_test1 WHERE b = 2 or b = 1").collect()
    result should have length 8
  }

  it should "allow to select rows with != clause" in {
    val result = spark.sql(s"SELECT * FROM ks1_test1 WHERE b != 2").collect()
    result should have length 4
  }

  it should "allow to select rows with <> clause" in {
    val result = spark.sql(s"SELECT * FROM ks1_test1 WHERE b <> 2").collect()
    result should have length 4
  }

  it should "allow to select rows with not in clause" in {
    val result = spark.sql(s"SELECT * FROM ks1_test1 WHERE b not in (1,2)").collect()
    result should have length 0
  }

  it should "allow to select rows with is not null clause" in {
    val result = spark.sql(s"SELECT * FROM ks1_test1 WHERE b is not null").collect()
    result should have length 8
  }

  it should "allow to select rows with like clause" in {
    val result = spark.sql(s"SELECT * FROM ks1_test2 WHERE name LIKE '%om' ").collect()
    result should have length 1
  }

  it should "allow to select rows with between clause" in {
    val result = spark.sql(s"SELECT * FROM ks1_test2 WHERE a BETWEEN 1 AND 2 ").collect()
    result should have length 6
  }

  it should "allow to select rows with alias" in {
    val result = spark.sql(s"SELECT a AS a_column, b AS b_column FROM ks1_test2").collect()
    result should have length 9
  }

  it should "allow to select rows with distinct column" in {
    val result = spark.sql(s"SELECT DISTINCT a FROM ks1_test2").collect()
    result should have length 3
  }

  it should "allow to select rows with limit clause" in {
    val result = spark.sql(s"SELECT * FROM ks1_test1 limit 2").collect()
    result should have length 2
  }

  it should "allow to select rows with order by clause" in {
    val result = spark.sql(s"SELECT * FROM ks1_test1 order by d").collect()
    result should have length 8
  }

  it should "allow to select rows with group by clause" in {
    val result = spark.sql(s"SELECT b, count(*) FROM ks1_test1 GROUP BY b order by b").collect()
    result(0).getInt(0) should be (1)
    result(1).getInt(0) should be (2)
    result(0).getLong(1) should be (4L)
    result(1).getLong(1) should be (4L)
    result should have length 2
  }

  it should "return correct count(*)" in {
    val result = spark.sql(s"SELECT count(*) FROM ks1_test1").collect()
    result(0).getLong(0) should be (8L)
  }

  it should "return correct count(1)" in {
    val result = spark.sql(s"SELECT count(1) FROM ks1_test1").collect()
    result(0).getLong(0) should be (8L)
  }

  it should "allow to select rows with union clause" in {
    val result = spark.sql(s"SELECT test1.a FROM ks1_test1 AS test1 UNION DISTINCT SELECT test2.a FROM ks1_test2 AS test2").collect()
    result should have length 3
  }

  it should "allow to select rows with union distinct clause" in {
    val result = spark.sql(s"SELECT test1.a FROM ks1_test1 AS test1 UNION DISTINCT SELECT test2.a FROM ks1_test2 AS test2").collect()
    result should have length 3
  }

  it should "allow to select rows with union all clause" in {
    val result = spark.sql(s"SELECT test1.a FROM ks1_test1 AS test1 UNION ALL SELECT test2.a FROM ks1_test2 AS test2").collect()
    result should have length 17
  }

  it should "allow to select rows with having clause" in {
    val result = spark.sql(s"SELECT count(*) FROM ks1_test1 GROUP BY b HAVING count(b) > 4").collect()
    result should have length 0
  }

  it should "allow to select rows with partition column clause" in {
    val result = spark.sql(s"SELECT * FROM ks1_test1 WHERE a = 1 and b = 1 and c = 1").collect()
    result should have length 4
  }

  it should "allow to select rows with partition column and cluster column clause" in {
    val result = spark.sql(s"SELECT * FROM ks1_test1 WHERE a = 1 and b = 1 and c = 1 and d = 1 and e = 1").collect()
    result should have length 1
  }

  it should "allow to insert into another table" in {
    val result = spark.sql(s"INSERT INTO TABLE ks1_test3 SELECT a, b, c FROM ks1_test2").collect()
    val result2 = spark.sql(s"SELECT a, b, c FROM ks1_test3").collect()
    result2 should have length 9
  }

  it should "allow to insert into another table in different keyspace" in {
    val result = spark.sql(s"INSERT INTO TABLE ks2_test3 SELECT test2.a, test2.b, test2.c FROM ks1_test2 as test2").collect()
    val result2 = spark.sql(s"SELECT test3.a, test3.b, test3.c FROM ks2_test3 as test3").collect()
    result2 should have length 9
  }

  it should "allow to join two tables" in {
    val result = spark.sql(s"SELECT test1.a, test1.b, test1.c, test2.a FROM ks1_test1 AS test1 " +
      s"JOIN ks1_test2 AS test2 ON test1.a = test2.a AND test1.b = test2.b AND test1.c = test2.c").collect()
    result should have length 4
  }

  it should "allow to join two tables from different keyspaces" in {
    val result = spark.sql(s"SELECT test1.a, test1.b, test1.c, test2.a FROM ks1_test1 AS test1 " +
      s"JOIN ks2_test2 AS test2 ON test1.a = test2.a AND test1.b = test2.b AND test1.c = test2.c").collect()
    result should have length 4
  }

  it should "allow to inner join two tables" in {
    val result = spark.sql(s"SELECT test1.a, test1.b, test1.c, test2.a FROM ks1_test1 AS test1 " +
      s"INNER JOIN ks1_test2 AS test2 ON test1.a = test2.a AND test1.b = test2.b AND test1.c = test2.c").collect()
    result should have length 4
  }

  it should "allow to left join two tables" in {
    val result = spark.sql(s"SELECT test1.a, test1.b, test1.c, test1.d, test1.e, test1.f FROM ks1_test1 AS test1 " +
      s"LEFT JOIN ks1_test2 AS test2 ON test1.a = test2.a AND test1.b = test2.b AND test1.c = test2.c").collect()
    result should have length 8
  }

  it should "allow to left outer join two tables" in {
    val result = spark.sql(s"SELECT test1.a, test1.b, test1.c, test1.d, test1.e, test1.f FROM ks1_test1 AS test1 " +
      s"LEFT OUTER JOIN ks1_test2 AS test2 ON test1.a = test2.a AND test1.b = test2.b AND test1.c = test2.c").collect()
    result should have length 8
  }

  it should "allow to right join two tables" in {
    val result = spark.sql(s"SELECT test2.a, test2.b, test2.c FROM ks1_test1 AS test1 " +
      s"RIGHT JOIN ks1_test2 AS test2 ON test1.a = test2.a AND test1.b = test2.b AND test1.c = test2.c").collect()
    result should have length 12
  }

  it should "allow to right outer join two tables" in {
    val result = spark.sql(s"SELECT test2.a, test2.b, test2.c FROM ks1_test1 AS test1 " +
      s"RIGHT OUTER JOIN ks1_test2 AS test2 ON test1.a = test2.a AND test1.b = test2.b AND test1.c = test2.c").collect()
    result should have length 12
  }

  it should "allow to full join two tables" in {
    val result = spark.sql(s"SELECT test2.a, test2.b, test2.c FROM ks1_test1 AS test1 " +
      s"FULL JOIN ks1_test2 AS test2 ON test1.a = test2.a AND test1.b = test2.b AND test1.c = test2.c").collect()
    result should have length 16
  }

  it should "allow to select rows for collection columns" in {
    val result = spark.sql(s"SELECT * FROM ks1_test_collection").collect()
    result should have length 1
  }

  it should "allow to select rows for data types of ASCII, INT, FLOAT, DOUBLE, BIGINT, BOOLEAN, DECIMAL, INET, TEXT, TIMESTAMP, UUID, VARINT, SMALLINT" in skipIfProtocolVersionLT(DefaultProtocolVersion.V4) {
    val result = spark.sql(s"SELECT * FROM ks1_test_data_type").collect()
    result should have length 1
  }

  it should "allow to insert rows for data types of ASCII, INT, FLOAT, DOUBLE, BIGINT, BOOLEAN, DECIMAL, INET, TEXT, TIMESTAMP, UUID, VARINT, SMALLINT" in skipIfProtocolVersionLT(DefaultProtocolVersion.V4) {
    val result = spark.sql(s"INSERT INTO TABLE ks1_test_data_type1 SELECT * FROM ks1_test_data_type").collect()
    val result1 = spark.sql(s"SELECT * FROM ks1_test_data_type1").collect()
    result1 should have length 1
  }

  it should "allow to select specified non-UDT columns from a table containing some UDT columns" in {
    val result = spark.sql(s"SELECT key, name FROM ks1_udts").collect()
    result should have length 1
    val row = result.head
    row.getInt(0) should be(1)
    row.getString(1) should be("name")
  }

  //TODO: SPARK-9269 is opened to address Set matching issue. I change the Set data type to List for now
  it should "allow to select UDT collection column and nested UDT column" in {
    val result = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(
        Map(
          "table" -> "objects",
          "keyspace" -> ks1
        )
      )
      .load()
      .collect()
    result should have length 1
  }

  it should "allow writing UDTs to C* tables" in {
    spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(
        Map(
          "table" -> "objects",
          "keyspace" -> ks1
        )
      )
      .load()
      .write
      .format("org.apache.spark.sql.cassandra")
      .mode("APPEND")
      .options(
        Map(
          "table" -> "objects_copy",
          "keyspace" -> ks1
        )
      ).save()
  }

  // Regression test for #454: java.util.NoSuchElementException thrown when accessing timestamp field using CassandraSQLContext
  it should "allow to restrict a clustering timestamp column value" in {
    spark.sql("select objectid, meterid, utcstamp  from ks1_export_table where meterid = 4317 and utcstamp > '2013-07-26 20:30:00-0700'").collect()
  }

  it should "allow to min/max timestamp column" in {
    spark.sql("select k, min(d), max(d) from ks1_timestamp_conversion_bug group by k").collect()
  }

  it should "be able to push down filter on UUID and Inet columns" in {
    val result = spark.sql(
      "select * " +
        "from ks1_uuid_inet_type " +
        "where b > '74.125.239.135'").collect()
    result should have length 1
    val result1 = spark.sql(
      "select * " +
        "from ks1_uuid_inet_type " +
        "where a < '123e4567-e89b-12d3-a456-426655440000'").collect()
    result1 should have length 1
    val result2 = spark.sql(
      "select * " +
        "from ks1_uuid_inet_type " +
        "where a = '123e4567-e89b-12d3-a456-426655440000' and b = '74.125.239.135'").collect()
    result2 should have length 1
  }

  it should "be able to push down filter on varint columns" in {
    spark.sql(
      s"""
         |SELECT * FROM ks1_varint_test
         |WHERE id = 1234567891234
         | AND series = 1234567891235
         | AND rollup_minutes = 1234567891236
      """.stripMargin.replaceAll("\n", " ")
    ).collect() should have length 1
  }

  it should "read C* Tuple using sql" in {
    val df = spark.sql(s"SELECT * FROM ks1_tuple_test1")

    df.count should be (1)
    df.first.getStruct(1).getString(0) should be ("xyz")
    df.first.getStruct(1).getInt(1) should be (2)
  }

  it should "write C* Tuple using sql" in {
    spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(
        Map(
          "table" -> "tuple_test1",
          "keyspace" -> ks1
        )
      )
      .load()
      .write
      .format("org.apache.spark.sql.cassandra")
      .mode("APPEND")
      .options(
        Map(
          "table" -> "tuple_test1_copy",
          "keyspace" -> ks1
        )
      ).save()
  }

  it should "handle keyspace with quoted name" in {
    spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "test", "keyspace" -> "sqlQuotedKeyspace"))
      .load()
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "test_copy", "keyspace" -> "sqlQuotedKeyspace"))
      .mode("append")
      .save()
  }
}
