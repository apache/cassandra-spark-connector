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

package io.teknek.arizona.spark.connector.cql

import java.io.IOException

import com.datastax.oss.driver.api.core.config.DefaultDriverOption
import com.datastax.oss.driver.api.core.cql.SimpleStatement
import com.datastax.oss.driver.api.core.{ProtocolVersion, Version}
import _root_.io.teknek.arizona.spark.connector.cluster.DefaultCluster
import _root_.io.teknek.arizona.spark.connector.{SparkCassandraITFlatSpecBase, _}
import org.scalatest.BeforeAndAfterEach

case class KeyValue(key: Int, group: Long, value: String)
case class KeyValueWithConversion(key: String, group: Int, value: Long)

class CassandraConnectorSpec extends SparkCassandraITFlatSpecBase with DefaultCluster with BeforeAndAfterEach {

  override lazy val conn = CassandraConnector(defaultConf)

  val createKeyspaceCql = keyspaceCql(ks)

  "A CassandraConnector" should "connect to Cassandra with native protocol" in {
    conn.withSessionDo { session =>
      assert(session.isClosed === false)
      assert(session !== null)
    }
  }

  it should "give access to cluster metadata" in {
    conn.withSessionDo { session =>
      assert(session.getMetadata.getNodes.size > 0)
    }
  }

  it should "have the default max hosts in pooling options" in {
    val conf = conn.withSessionDo(_.getContext.getConfig.getDefaultProfile)
    conf.getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE) should be >= 1
    conf.getInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE) should be >= 1
  }

  it should "have larger max hosts if set" in {
    val alteredConnector = CassandraConnector(
      defaultConf
          .set(CassandraConnectorConf.LocalConnectionsPerExecutorParam.name, "12")
          .set(CassandraConnectorConf.RemoteConnectionsPerExecutorParam.name, "14"))

    // *ConnectionsPerExecutorsParam are not taken in account when retrieving session objects from global cache.
    // This results in a possibility of grabbing a session that does not have the parameters set.
    // https://github.com/riptano/bdp/pull/11359/files#diff-a866d6dfb859aa080a01d9a55ae1e5c0R43
    CassandraConnector.evictCache()

    val conf = alteredConnector.withSessionDo(_.getContext.getConfig.getDefaultProfile)
    conf.getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE) should be (12)
    conf.getInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE) should be (14)
  }

  it should "run queries" in {
    conn.withSessionDo { session =>
      session.execute(createKeyspaceCql)
      session.execute(s"DROP TABLE IF EXISTS $ks.simple_query")
      session.execute(s"CREATE TABLE $ks.simple_query (key INT PRIMARY KEY, value TEXT)")
      session.execute(s"INSERT INTO $ks.simple_query(key, value) VALUES (1, 'value')")
      val result = session.execute(
        SimpleStatement.newInstance(s"SELECT * FROM $ks.simple_query WHERE key = 1"))

      assert(result.one().getString("value") === "value")
    }
  }

  it should "disconnect from the session after use" in {
    val session = conn.withSessionDo { session => session }
    Thread.sleep(
      sc.getConf.getInt(
        "spark.cassandra.connection.keepAliveMS",
        CassandraConnectorConf.KeepAliveMillisParam.default) * 2)
    assert(session.isClosed === true)
  }

  it should "share internal Cluster and Session object between multiple logical sessions" in {
    val session1 = conn.openSession()
    val threadCount1 = Thread.activeCount()
    val session2 = conn.openSession()
    val threadCount2 = Thread.activeCount()

    session1.getName shouldEqual session2.getName
    // Unfortunately we don't have a way to obtain an internal Session object; all we got here are proxies.
    // Instead, we try to figure out whether a new Session was opened by counting active threads.
    // Opening internal Session creates new threads, so if none was created, the thread count would not change.
    threadCount1 shouldEqual threadCount2
    session1.close()
    session1.isClosed shouldEqual true
    session2.isClosed shouldEqual false
    session2.close()
    session2.isClosed shouldEqual true
  }

  it should "share internal Cluster object between multiple logical sessions created by different connectors to the same cluster" in {
    val conn2 = CassandraConnector(sc.getConf)
    val session1 = conn.openSession()
    val threadCount1 = Thread.activeCount()
    val session2 = conn2.openSession()
    val threadCount2 = Thread.activeCount()
    threadCount1 shouldEqual threadCount2

    session1.getName shouldEqual session2.getName
    session1.close()
    session1.isClosed shouldEqual true
    session2.isClosed shouldEqual false
    session2.close()
    session2.isClosed shouldEqual true
  }

  it should "cache session objects for reuse" in {
    CassandraConnector(sc.getConf).withSessionDo(x => {})
    CassandraConnector(sc.getConf).withSessionDo(x => {})
    val sessionCache = CassandraConnector.sessionCache
    sessionCache.contains(CassandraConnectorConf(sc.getConf)) should be (true)
    sessionCache.cache.size should be (1)
  }

  it should "not make multiple clusters when writing multiple RDDs" in {
    val sessionCache = CassandraConnector.sessionCache
    val originalSize = sessionCache.cache.size

    CassandraConnector(sc.getConf).withSessionDo { session =>
      session.execute(createKeyspaceCql)
      session.execute(s"CREATE TABLE IF NOT EXISTS $ks.pair (x int, y int, PRIMARY KEY (x))")
    }
    for (trial <- 1 to 4) {
      val rdd = sc.parallelize(1 to 100).map(x => (x, x)).saveToCassandra(ks, "pair")
    }

    sessionCache.contains(CassandraConnectorConf(sc.getConf)) should be(true)
    sessionCache.cache.size should be <= (originalSize + 1)
  }


  it should "be configurable from SparkConf" in {
    val conf = sc.getConf

    // would throw exception if connection unsuccessful
    val conn2 = CassandraConnector(conf)
    conn2.withSessionDo { session => }
  }

  it should "accept multiple hostnames in spark.cassandra.connection.host property" in {
    val goodHost = cluster.getConnectionHost
    val invalidHost = "192.168.254.254"
    // let's connect to two addresses, of which the first one is deliberately invalid
    val conf = sc.getConf
    conf.set(CassandraConnectorConf.ConnectionHostParam.name, invalidHost + "," + goodHost)

    // would throw exception if connection unsuccessful
    val conn2 = CassandraConnector(conf)
    conn2.withSessionDo { session => }
  }

  it should "accept multiple hostnames with ports in spark.cassandra.connection.host property" in {
    val goodHost = s"${cluster.getConnectionHost}:9042"
    val invalidHost = s"${cluster.getConnectionHost}:32"
    // let's connect to two addresses, of which the first one is deliberately invalid
    val conf = sc.getConf
    conf.set(CassandraConnectorConf.ConnectionHostParam.name, invalidHost + "," + goodHost)

    // would throw exception if connection unsuccessful
    val conn2 = CassandraConnector(conf)
    conn2.withSessionDo { session => }
  }

  it should "use SNAPPY compression when configured" in skipIfProtocolVersionGTE(ProtocolVersion.V5) {
    val conf = sc.getConf
      .set(CassandraConnectorConf.CompressionParam.name, "SNAPPY")

    val conn = CassandraConnector(conf)
    conn.withSessionDo { session =>
      session
        .getContext
        .getConfig
        .getDefaultProfile.getString(DefaultDriverOption.PROTOCOL_COMPRESSION) shouldBe "snappy"
    }
  }

  // https://github.com/apache/cassandra/blob/cassandra-4.0.0/doc/native_protocol_v5.spec#L194-L195
  it should "use LZ4 compression when configured" in skipIfProtocolVersionLT(ProtocolVersion.V5) {
    val conf = sc.getConf
      .set(CassandraConnectorConf.CompressionParam.name, "LZ4")

    val conn = CassandraConnector(conf)
    conn.withSessionDo { session =>
      session
        .getContext
        .getConfig
        .getDefaultProfile.getString(DefaultDriverOption.PROTOCOL_COMPRESSION) shouldBe "lz4"
    }
  }

  /*
  Check to see whether a parameter set in our "test-conf" is actually read, we do this by trying to
  connect using the file and making sure the invalid ip is used.
   */
  it should "accept a driver profile file added to --files" in {
    sc.addFile(ClassLoader.getSystemResource("test.conf").getFile)
    val conf = sc.getConf
      .set(CassandraConnectorConf.ProfileFileBasedConfigurationParam.name, "test.conf")
    val conn = CassandraConnector(conf)

    val exception = intercept[IOException] {
      conn.withSessionDo(session => session.getContext)
    }
    exception.getMessage should include ("test.conf")
    exception.getMessage should include ("6.6.6.6:9042")
  }

  it should "use a driver profile file accessed via URL" in {
    val url = ClassLoader.getSystemResource("test.conf")

    val conf = sc.getConf
      .set(CassandraConnectorConf.ProfileFileBasedConfigurationParam.name, url.toString)
    val conn = CassandraConnector(conf)

    val exception = intercept[IOException] {
      conn.withSessionDo(session => session.getContext)
    }
    exception.getMessage should include ("test.conf")
    exception.getMessage should include ("6.6.6.6:9042")
  }

  /**
    * Once again we don't have a good way of testing the cloud connection either, so we are just
    * testing to make sure that it is properly in the config. To this end we are going to use an
    * invalid bundle and just make sure it is loaded.
    */
  it should "use a cloud connect bundle added to --files" in {
    sc.addFile(ClassLoader.getSystemResource("test.conf").getFile)

    val conf = sc.getConf
      .set(CassandraConnectorConf.CloudBasedConfigurationParam.name, "test.conf")
    val conn = CassandraConnector(conf)

    val exception = intercept[IOException] {
      conn.withSessionDo(session => session.getContext)
    }

    exception.getMessage should include ("Invalid bundle")
    exception.getMessage should include ("missing file config.json")
  }


  it should "use a cloud connect bundle added to URL" in {
    val url = ClassLoader.getSystemResource("test.conf")

    val conf = sc.getConf
      .set(CassandraConnectorConf.CloudBasedConfigurationParam.name, url.toString)
    val conn = CassandraConnector(conf)

    val exception = intercept[IOException] {
      conn.withSessionDo(session => session.getContext)
    }

    exception.getMessage should include ("Invalid bundle")
    exception.getMessage should include ("missing file config.json")
  }


}


