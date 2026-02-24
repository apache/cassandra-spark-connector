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

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance
import com.datastax.spark.connector.types.TypeAdapters.{ValueByNameAdapter, ValuesSeqAdapter}
import com.datastax.spark.connector.types.{NullableTypeConverter, TypeConverter}
import com.datastax.spark.connector.util.ConfigCheck.ConnectorConfigurationException
import com.datastax.spark.connector.util.DriverUtil.toAddress
import com.datastax.spark.connector.util.{Logging, SerialShutdownHooks}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.{SparkConf, SparkContext}

import java.io.IOException
import java.net.{InetAddress, InetSocketAddress}
import scala.jdk.CollectionConverters._
import scala.language.reflectiveCalls

/** Provides and manages connections to Cassandra.
  *
  * A `CassandraConnector` instance is serializable and
  * can be safely sent over network,
  * because it automatically reestablishes the connection
  * to the same cluster after deserialization. Internally it saves
  * a list of all nodes in the cluster, so a connection can be established
  * even if the host given in the initial config is down.
  *
  * Multiple `CassandraConnector`s in the same JVM connected to the same
  * Cassandra cluster will share a single underlying `Cluster` object.
  * `CassandraConnector` will close the underlying `Cluster` object automatically
  * whenever it is not used i.e. no `Session` or `Cluster` is open for longer
  * than `spark.cassandra.connection.keepAliveMS` property value.
  *
  * A `CassandraConnector` object is configured from [[CassandraConnectorConf]] object which
  * can be either given explicitly or automatically configured from [[org.apache.spark.SparkConf SparkConf]].
  * The connection options are:
  *   - `spark.cassandra.connection.host`:               contact points to connect to the Cassandra cluster, defaults to spark master host
  *   - `spark.cassandra.connection.port`:               Cassandra native port, defaults to 9042
  *   - `spark.cassandra.connection.factory`:            name of a Scala module or class implementing [[CassandraConnectionFactory]] that allows to plugin custom code for connecting to Cassandra
  *   - `spark.cassandra.connection.keepAliveMS`:      how long to keep unused connection before closing it (default 250 ms)
  *   - `spark.cassandra.connection.timeoutMS`:         how long to wait for connection to the Cassandra cluster (default 5 s)
  *   - `spark.cassandra.connection.reconnectionDelayMS.min`: initial delay determining how often to try to reconnect to a dead node (default 1 s)
  *   - `spark.cassandra.connection.reconnectionDelayMS.max`: final delay determining how often to try to reconnect to a dead node (default 60 s)
  *   - `spark.cassandra.auth.username`:                        login for password authentication
  *   - `spark.cassandra.auth.password`:                        password for password authentication
  *   - `spark.cassandra.auth.conf.factory`:                    name of a Scala module or class implementing [[AuthConfFactory]] that allows to plugin custom authentication configuration
  *   - `spark.cassandra.query.retry.count`:                    how many times to reattempt a failed query (default 10)
  *   - `spark.cassandra.read.timeoutMS`:                      maximum period of time to wait for a read to return
  *   - `spark.cassandra.connection.ssl.enabled`:               enable secure connection to Cassandra cluster
  *   - `spark.cassandra.connection.ssl.trustStore.path`:      path for the trust store being used
  *   - `spark.cassandra.connection.ssl.trustStore.password`:  trust store password
  *   - `spark.cassandra.connection.ssl.trustStore.type`:      trust store type (default JKS)
  *   - `spark.cassandra.connection.ssl.protocol`:              SSL protocol (default TLS)
  *   - `spark.cassandra.connection.ssl.enabledAlgorithms`:         SSL cipher suites (default TLS_RSA_WITH_AES_128_CBC_SHA, TLS_RSA_WITH_AES_256_CBC_SHA)
  */
class CassandraConnector(val conf: CassandraConnectorConf)
  extends Serializable with Logging {

  import com.datastax.spark.connector.cql.CassandraConnector._

  private[this] var _config = conf

  /** Known cluster hosts in the connected datacenter.*/
  def hosts: Set[InetSocketAddress] =
    // wrapped in a session, so we get full lists of hosts,
    // not only those explicitly passed in the conf
    withSessionDo { session =>
      dataCenterNodes(session)
    }

  private[connector] def hostAddresses: Set[InetAddress] = hosts.map(_.getAddress)

  /** Connection configurator */
  def connectionFactory = _config.connectionFactory

  /** Returns a shared session to Cassandra and increases the internal open
    * reference counter. It does not release the session automatically,
    * so please remember to close it after use. Closing a shared session
    * decreases the session reference counter. If the reference count drops to zero,
    * the session may be physically closed. */
  def openSession() = {
    val session = sessionCache.acquire(_config)
    try {
      // We need a separate SessionProxy here to protect against double closing the session.
      // Closing SessionProxy is not really closing the session, because sessions are shared.
      // Instead, refcount is decreased. But double closing the same Session reference must not
      // decrease refcount twice. There is a guard in SessionProxy
      // so any subsequent close calls on the same SessionProxy are no-ops.
      SessionProxy.wrapWithCloseAction(session)(sessionCache.release(_, _config.keepAliveMillis))
    }
    catch {
      case e: Throwable =>
        sessionCache.release(session, 0)
        throw e
    }
  }

  /** Allows to use Cassandra `Session` in a safe way without
    * risk of forgetting to close it. The `Session` object obtained through this method
    * is a proxy to a shared, single `Session` associated with the cluster.
    * Internally, the shared underlying `Session` will be closed shortly after all the proxies
    * are closed. */
  def withSessionDo[T](code: CqlSession => T): T = {
    closeResourceAfterUse(openSession()) { session =>
      code(session)
    }
  }

  /**
    * Allows use of a Cassandra `Session` in a safe way without
    * risk of forgetting to close it from java. See `withSessionDo`
    *
    * We should not need this in scala 2.12
    */
  def jWithSessionDo[T](code: java.util.function.Function[CqlSession, T]): T = {
    withSessionDo(session => code.apply(session))
  }

  /** Automatically closes resource after use. Handy for closing streams, files, sessions etc.
    * Similar to try-with-resources in Java 7. */
  def closeResourceAfterUse[T, C <: { def close(): Unit }](closeable: C)(code: C => T): T =
    try code(closeable) finally {
      closeable.close()
    }

}

object CassandraConnector extends Logging {

  import scala.reflect.runtime.universe._

  TypeConverter.registerConverter(new GenericRowWithSchemeToValuesByNameConverter())
  TypeConverter.registerConverter(new GenericRowWithSchemeToValuesSeqConverter())

  private class GenericRowWithSchemeToValuesByNameConverter extends NullableTypeConverter[ValueByNameAdapter] {

    class GenericRowWithSchemaAdapter(row: GenericRowWithSchema) extends ValueByNameAdapter {
      override def getByName(name: String): Any = row.get(row.fieldIndex(name))
    }

    override def targetTypeTag = implicitly[TypeTag[ValueByNameAdapter]]

    override def convertPF = {
      case row: GenericRowWithSchema => new GenericRowWithSchemaAdapter(row)
    }
  }

  private class GenericRowWithSchemeToValuesSeqConverter extends NullableTypeConverter[ValuesSeqAdapter] {

    class GenericRowWithSchemaAdapter(row: GenericRowWithSchema) extends ValuesSeqAdapter {
      override def toSeq(): Seq[Any] = row.toSeq
    }

    override def targetTypeTag = implicitly[TypeTag[ValuesSeqAdapter]]

    override def convertPF = {
      case row: GenericRowWithSchema => new GenericRowWithSchemaAdapter(row)
    }
  }

  private[cql] val sessionCache = new RefCountedCache[CassandraConnectorConf, CqlSession](
    createSession, destroySession, alternativeConnectionConfigs)

  private def createSession(conf: CassandraConnectorConf): CqlSession = {
    lazy val endpointsStr = conf.contactInfo.endPointStr()
    logDebug(s"Attempting to open native connection to Cassandra with $endpointsStr")
    try {
      val session = conf.connectionFactory.createSession(conf)
      logInfo(s"Connected to Cassandra cluster.")
      session
    }
    catch {
      case e: Throwable =>
        throw new IOException(s"Failed to open native connection to Cassandra at $endpointsStr :: ${e.getLocalizedMessage}", e)
    }
  }

  private def destroySession(session: CqlSession): Unit = {
    session.close()
    logInfo(s"Disconnected from Cassandra cluster.")
  }

  // LocalNodeFirstLoadBalancingPolicy assigns LOCAL or REMOTE (i.e. non-IGNORED) distance to local DC nodes
  private def dataCenterNodes(session: CqlSession): Set[InetSocketAddress] = {
    val allNodes = session.getMetadata.getNodes.asScala.values.toSet
    val nodes = allNodes
      .filter(_.getDistance != NodeDistance.IGNORED)
      .flatMap(toAddress)
    if (nodes.isEmpty) {
      throw new ConnectorConfigurationException(s"Could not determine suitable nodes in local DC for known nodes: " +
        s"${allNodes.map(n => (n.getHostId, toAddress(n))).mkString(", ")}")
    }
    nodes
  }

  // This is to ensure the Cluster can be found by requesting for any of its hosts, or all hosts together. We can only do this for IP defined contactInfo
  private def alternativeConnectionConfigs(conf: CassandraConnectorConf, session: CqlSession): Set[CassandraConnectorConf] = {
    conf.contactInfo match {
      case ipConf: IpBasedContactInfo =>
        val nodes = dataCenterNodes(session)
        nodes.map(n => conf.copy(contactInfo = ipConf.copy(hosts = Set(n)))) + conf.copy(contactInfo = ipConf.copy(hosts = nodes))
      case _ => Set.empty
    }
  }

  SerialShutdownHooks.add("Clearing session cache for C* connector", 200)(() => {
    val classLoaderCheckName = "com.datastax.spark.connector.util.ClassLoaderCheck"
    try {
      val check = sessionCache.getClass.getClassLoader.loadClass(classLoaderCheckName).getSimpleName
      logDebug("Class loader check: " + check)

      sessionCache.shutdown()
    } catch {
      case exc: ClassNotFoundException if exc.getMessage.contains(classLoaderCheckName) =>
        // a temp workaround for spark-sql classloader problem, see SPARKC-620 PR for details
        logDebug(s"Couldn't shutdown the session cache.")
    }
  })

  def apply(conf: CassandraConnectorConf) = {
    new CassandraConnector(conf)
  }

  /** Returns a CassandraConnector created from properties found in the [[org.apache.spark.SparkConf SparkConf]] object */
  def apply(conf: SparkConf): CassandraConnector = {
    CassandraConnector(CassandraConnectorConf(conf))
  }

  /** Returns a CassandraConnector with runtime Cluster Environment information. This can set remoteConnectionsPerExecutor
    * based on cores and executors in use
    */
  def apply(sc: SparkContext): CassandraConnector = {
    CassandraConnector(sc.getConf)
  }

  /** Returns a CassandraConnector created from explicitly given connection configuration. */
  def apply(contactInfo: ContactInfo,
            localDC: Option[String] = None,
            keepAliveMillis: Int = CassandraConnectorConf.KeepAliveMillisParam.default,
            minReconnectionDelayMillis: Int = CassandraConnectorConf.MinReconnectionDelayParam.default,
            maxReconnectionDelayMillis: Int = CassandraConnectorConf.MaxReconnectionDelayParam.default,
            queryRetryCount: Int = CassandraConnectorConf.QueryRetryParam.default,
            connectTimeoutMillis: Int = CassandraConnectorConf.ConnectionTimeoutParam.default,
            readTimeoutMillis: Int = CassandraConnectorConf.ReadTimeoutParam.default,
            connectionFactory: CassandraConnectionFactory = DefaultConnectionFactory) = {

    val config = CassandraConnectorConf(
      contactInfo,
      localDC = localDC,
      keepAliveMillis = keepAliveMillis,
      minReconnectionDelayMillis = minReconnectionDelayMillis,
      maxReconnectionDelayMillis = maxReconnectionDelayMillis,
      queryRetryCount = queryRetryCount,
      connectTimeoutMillis = connectTimeoutMillis,
      readTimeoutMillis = readTimeoutMillis,
      connectionFactory = connectionFactory
    )

    new CassandraConnector(config)
  }

  def evictCache(): Unit = {
    sessionCache.evict()
  }

}
