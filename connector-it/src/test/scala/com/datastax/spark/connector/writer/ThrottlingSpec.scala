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

import com.datastax.spark.connector._
import com.datastax.spark.connector.cluster.DefaultCluster
import com.datastax.spark.connector.{SomeColumns, SparkCassandraITFlatSpecBase}
import com.datastax.spark.connector.cql.CassandraConnector

import scala.concurrent.Future

class ThrottlingSpec extends SparkCassandraITFlatSpecBase with DefaultCluster {

  override lazy val conn = CassandraConnector(defaultConf)

  override def beforeClass {
    conn.withSessionDo { session =>
      createKeyspace(session)

      awaitAll(
        Future {
          session.execute( s"""CREATE TABLE $ks.key_value (key INT, group BIGINT, value TEXT, PRIMARY KEY (key, group))""")
        })
    }
  }

  //TODO FIX This after we figure out profiles
  "Throttling" should "prevent failures based on driver max queue size while writing" in {
    conn.withSessionDo{session =>
      val poolProfile = session.getContext.getConfig.getDefaultProfile
//      poolingOptions.setMaxQueueSize(1)
//      poolingOptions.setConnectionsPerHost(com.datastax.driver.core.HostDistance.LOCAL, 1, 1)
    }
    val rows = (1 to 10000).map(x => (x, x.toLong, x.toString))
    sc.parallelize(rows).saveToCassandra(ks, "key_value")
    sc.cassandraTable(ks, "key_value").cassandraCount() should be(10000)
  }

  it should "prevent failures based on driver pooling limits while joining" in {
 /* TODO:
      conn.withClusterDo{cluster =>
      val poolingOptions = cluster.getConfiguration.getPoolingOptions
      poolingOptions.setMaxQueueSize(1)
      poolingOptions.setConnectionsPerHost(com.datastax.driver.core.HostDistance.LOCAL, 1, 1)
    }*/

    val rows = (1 to 100000).map(Tuple1(_))
    val results = sc.parallelize(rows).joinWithCassandraTable(ks, "key_value")
    results.count should be(10000)
  }
}
