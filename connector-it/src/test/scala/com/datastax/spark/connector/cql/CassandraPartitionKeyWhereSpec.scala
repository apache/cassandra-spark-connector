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

import scala.concurrent.Future
import com.datastax.spark.connector._
import com.datastax.spark.connector.cluster.DefaultCluster

class CassandraPartitionKeyWhereSpec extends SparkCassandraITFlatSpecBase with DefaultCluster {

  override lazy val conn = CassandraConnector(defaultConf)

  override def beforeClass {
    conn.withSessionDo { session =>
      createKeyspace(session)

      awaitAll(
        Future {
          session.execute(s"CREATE TABLE $ks.key_value (key INT, group BIGINT, value TEXT, PRIMARY KEY (key, group))")
          session.execute(s"INSERT INTO $ks.key_value (key, group, value) VALUES (1, 100, '0001')")
          session.execute(s"INSERT INTO $ks.key_value (key, group, value) VALUES (2, 200, '0002')")
          session.execute(s"INSERT INTO $ks.key_value (key, group, value) VALUES (3, 300, '0003')")
        },
        Future {
          session.execute(s"""CREATE TABLE $ks.ckey_value (key1 INT, "Key2" BIGINT, group INT, value TEXT, PRIMARY KEY ((key1, "Key2"), group))""")
          session.execute(s"""INSERT INTO $ks.ckey_value (key1, "Key2", group, value) VALUES (1, 100, 1000, '0001')""")
          session.execute(s"""INSERT INTO $ks.ckey_value (key1, "Key2", group, value) VALUES (2, 200, 2000, '0002')""")
          session.execute(s"""INSERT INTO $ks.ckey_value (key1, "Key2", group, value) VALUES (3, 300, 3000, '0003')""")
        }
      )
    }
  }

  "A CassandraRDD" should "allow partition key eq in where" in {
    val rdd = sc.cassandraTable(ks, "key_value").where("key = ?", 1)
    val result =  rdd.collect()
    result should have length 1
    result.head.getInt("key") should be (1)
  }

  it should "allow partition key 'in' in where" in {
    val result = sc.cassandraTable(ks, "key_value").where("key in (?, ?)", 2,3).collect()
    result should have length 2
    result.head.getInt("key") should (be (2) or be (3))
  }

  it should "allow cluster key 'in' in where" in {
    val result = sc.cassandraTable(ks, "key_value").where("group in (?, ?)", 200,300).collect()
    result should have length 2
    result.head.getInt("key") should (be (2) or be (3))
  }

  it should "work with composite keys in" in {
    val result = sc.cassandraTable(ks, "ckey_value").where("key1 = 1 and \"Key2\" in (?, ?)", 100,200).collect()
    result should have length 1
    result.head.getInt("key1") should be (1)
  }

  it should "work with composite keys eq" in {
    val result = sc.cassandraTable(ks, "ckey_value").where("key1 = ? and \"Key2\" = ?", 1,100).collect()
    result should have length 1
    result.head.getInt("key1") should be (1)
  }

  it should "work with composite keys in2" in {
    val result = sc.cassandraTable(ks, "ckey_value").where("\"Key2\" in (?, ?) and key1 = 1", 100,200).collect()
    result should have length 1
    result.head.getInt("key1") should be (1)
  }
}
