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

package com.datastax.spark.connector.rdd.partitioner

import java.net.InetAddress

import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cluster.DefaultCluster
import com.datastax.spark.connector.cql.{CassandraConnector, Schema, TableDef}
import com.datastax.spark.connector.rdd.partitioner.dht.TokenFactory
import com.datastax.spark.connector.util.schemaFromCassandra

class CassandraPartitionGeneratorSpec extends SparkCassandraITFlatSpecBase with DefaultCluster {

  override lazy val conn = CassandraConnector(defaultConf)
  implicit val tokenFactory = TokenFactory.forSystemLocalPartitioner(conn)

  override def beforeClass {
    conn.withSessionDo { session =>
      createKeyspace(session)
      session.execute(s"CREATE TABLE $ks.empty(key INT PRIMARY KEY)")
    }
  }

  // TODO: Currently CassandraPartitionGenerator uses a size-based algorithm that doesn't guarantee exact
  // split count, so we are only checking if the split count is "close enough" to the desired value.
  // Should be improved in the future.
  private def testPartitionCount(numPartitions: Int, min: Int, max: Int): Unit = {
    val table = schemaFromCassandra(conn, Some(ks), Some("empty")).tables.head
    val partitioner = CassandraPartitionGenerator(conn, table, numPartitions)
    val partitions = partitioner.partitions
    partitions.length should be >= min
    partitions.length should be <= max
  }


  class MockCassandraPartitionGenerator(
      connector: CassandraConnector,
      tableDef: TableDef,
      splitCount: Int) extends CassandraPartitionGenerator(connector, tableDef, splitCount) {


    //emulate 12 node cluster
    val endPoints = (1 to 12).map("127.0.0." + _).map(InetAddress.getByName)

    // with 36 RF3 ranges
    override private[partitioner] def describeRing: Seq[TokenRange] = {
      (endPoints ++ endPoints.slice(0, 2)).sliding(3, 1).toSeq.zipWithIndex
        .flatMap { case (endpoints, i) =>
          Seq(
            tokenRange(i * 1000, (i + 1) * 1000, endpoints),
            tokenRange(i * 1000, (i + 1) * 1000, endpoints),
            tokenRange(i * 1000, (i + 1) * 1000, endpoints)
          )
        }
    }

    private def tokenRange(start: Long, end: Long, replicas: Seq[InetAddress]): TokenRange = {
      val startToken = tokenFactory.tokenFromString(start.toString)
      val endToken = tokenFactory.tokenFromString(end.toString)
      new TokenRange(startToken, endToken, replicas.toSet, tokenFactory)
    }
  }

  "CassandraPartitionGenerator" should "create 1 partition if splitCount == 1" in {
    testPartitionCount(1, 1, 1)
  }

  // we won't run it on a 10000 node cluster, so we don't need to check node count
  it should "create about 10000 partitions when splitCount == 10000" in {
    testPartitionCount(10000, 9000, 11000)
  }

  it should "round robin partition with different endpoints" in {
    val table = schemaFromCassandra(conn, Some(ks), Some("empty")).tables.head
    val partitioner = new MockCassandraPartitionGenerator(conn, table, 12)
    val partitions = partitioner.partitions
    print(s"Partitions: $partitions")
    partitions.length should be(12)
    // endpoint are distributed by round robin algorithm and there is no equals endpoints near by.
    partitions.sliding(3, 1).foreach(group => {
      group(0).endpoints should not be group(1).endpoints
      group(1).endpoints should not be group(2).endpoints
      group(2).endpoints should not be group(0).endpoints
    })
  }
}
