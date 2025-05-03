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

import java.net.{InetAddress, InetSocketAddress}

import scala.language.postfixOps
import scala.util.control.NonFatal
import org.apache.commons.lang3.builder.{EqualsBuilder, HashCodeBuilder}
import org.apache.spark.SparkConf
import com.datastax.spark.connector.util.{ConfigCheck, ConfigParameter, DeprecatedConfigParameter, Logging}


//All of these classes must be serializable
sealed trait ContactInfo {
  def endPointStr(): String
};

case class IpBasedContactInfo (
  hosts: Set[InetSocketAddress],
  authConf: AuthConf = NoAuthConf,
  cassandraSSLConf: CassandraConnectorConf.CassandraSSLConf = CassandraConnectorConf.DefaultCassandraSSLConf) extends ContactInfo {

  override def equals(obj: Any): Boolean = obj match {
    case that: IpBasedContactInfo =>
      (this.hosts == that.hosts  && this.authConf == that.authConf)
    case _ => false
  }

  def apply(
    hosts: Set[InetAddress],
    port: Int = CassandraConnectorConf.ConnectionPortParam.default,
    authConf: AuthConf,
    cassandraSSLConf: CassandraConnectorConf.CassandraSSLConf = CassandraConnectorConf.DefaultCassandraSSLConf): IpBasedContactInfo = {

    IpBasedContactInfo(hosts.map( host => new InetSocketAddress(host, port)), authConf, cassandraSSLConf)
  }

  override def endPointStr() = hosts.map(i => s"${i.getHostString}:${i.getPort}").mkString("{", ", ", "}")
}

case class CloudBasedContactInfo(path: String, authConf: AuthConf ) extends ContactInfo {
  override def endPointStr(): String = s"Cloud File Based Config at $path"
}

case class ProfileFileBasedContactInfo(path: String) extends ContactInfo {
  override def endPointStr(): String = s"Profile based config at $path"
}

/** Stores configuration of a connection to Cassandra.
  * Provides information about cluster nodes, ports and optional credentials for authentication. */
case class CassandraConnectorConf(
  contactInfo: ContactInfo,
  localDC: Option[String] = CassandraConnectorConf.LocalDCParam.default,
  keepAliveMillis: Int = CassandraConnectorConf.KeepAliveMillisParam.default,
  minReconnectionDelayMillis: Int = CassandraConnectorConf.MinReconnectionDelayParam.default,
  maxReconnectionDelayMillis: Int = CassandraConnectorConf.MaxReconnectionDelayParam.default,
  localConnectionsPerExecutor: Option[Int] = CassandraConnectorConf.LocalConnectionsPerExecutorParam.default,
  remoteConnectionsPerExecutor: Option[Int] = CassandraConnectorConf.RemoteConnectionsPerExecutorParam.default,
  compression: String = CassandraConnectorConf.CompressionParam.default,
  queryRetryCount: Int = CassandraConnectorConf.QueryRetryParam.default,
  connectTimeoutMillis: Int = CassandraConnectorConf.ConnectionTimeoutParam.default,
  readTimeoutMillis: Int = CassandraConnectorConf.ReadTimeoutParam.default,
  connectionFactory: CassandraConnectionFactory = DefaultConnectionFactory,
  quietPeriodBeforeCloseMillis: Int = CassandraConnectorConf.QuietPeriodBeforeCloseParam.default,
  timeoutBeforeCloseMillis: Int = CassandraConnectorConf.TimeoutBeforeCloseParam.default,
  resolveContactPoints: Boolean = CassandraConnectorConf.ResolveContactPoints.default,
  maxFrameLengthInMB: Int = CassandraConnectorConf.MaxFrameLengthInMB.default,
) {

  override def hashCode: Int = HashCodeBuilder.reflectionHashCode(this, false)

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: CassandraConnectorConf if hashCode == that.hashCode =>
        EqualsBuilder.reflectionEquals(this, that, false)
      case _ => false
    }
  }
}

/** A factory for [[CassandraConnectorConf]] objects.
  * Allows for manually setting connection properties or reading them from [[org.apache.spark.SparkConf SparkConf]]
  * object. By embedding connection information in [[org.apache.spark.SparkConf SparkConf]],
  * [[org.apache.spark.SparkContext SparkContext]] can offer Cassandra specific methods which require establishing
  * connections to a Cassandra cluster. */
object CassandraConnectorConf extends Logging {

  case class CassandraSSLConf(
    enabled: Boolean = false,
    trustStorePath: Option[String] = None,
    trustStorePassword: Option[String] = None,
    trustStoreType: String = "JKS",
    protocol: String = "TLS",
    enabledAlgorithms: Set[String] = Set("TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA"),
    clientAuthEnabled: Boolean = false,
    keyStorePath: Option[String] = None,
    keyStorePassword: Option[String] = None,
    keyStoreType: String = "JKS")

  val ReferenceSection = "Cassandra Connection Parameters"

  val ConnectionPortParam = ConfigParameter[Int](
    name = "spark.cassandra.connection.port",
    section = ReferenceSection,
    default = 9042,
    description = """Cassandra native connection port, will be set to all hosts if no individual ports are given""")

  val ConnectionHostParam = ConfigParameter[String](
    name = "spark.cassandra.connection.host",
    section = ReferenceSection,
    default = "localhost",
    description =
      s"""Contact point to connect to the Cassandra cluster. A comma separated list
        |may also be used. Ports may be provided but are optional. If Ports are missing ${ConnectionPortParam.name} will
        | be used ("127.0.0.1:9042,192.168.0.1:9051")
      """.stripMargin)

  val LocalDCParam = ConfigParameter[Option[String]](
    name = "spark.cassandra.connection.localDC",
    section = ReferenceSection,
    default = None,
    description = """The local DC to connect to (other nodes will be ignored)""")

  val DeprecatedLocalDCParam = DeprecatedConfigParameter(
    name = "spark.cassandra.connection.local_dc",
    replacementParameter = Some(LocalDCParam),
    deprecatedSince = "DSE 6.0.0"
  )

  val ConnectionTimeoutParam = ConfigParameter[Int](
    name = "spark.cassandra.connection.timeoutMS",
    section = ReferenceSection,
    default = 5000,
    description = """Maximum period of time to attempt connecting to a node""")

  val DeprecatedConnectionTimeoutParam = DeprecatedConfigParameter(
    name = "spark.cassandra.connection.timeout_ms",
    replacementParameter = Some(ConnectionTimeoutParam),
    deprecatedSince = "DSE 6.0.0"
  )

  val KeepAliveMillisParam = ConfigParameter[Int](
    name = "spark.cassandra.connection.keepAliveMS",
    section = ReferenceSection,
    default = 3600000,
    description = """Period of time to keep unused connections open""")

  val DeprecatedKeepAliveMillisParam = DeprecatedConfigParameter(
    name = "spark.cassandra.connection.keep_alive_ms",
    replacementParameter = Some(KeepAliveMillisParam),
    deprecatedSince = "DSE 6.0.0"
  )

  val MinReconnectionDelayParam = ConfigParameter[Int](
    name = "spark.cassandra.connection.reconnectionDelayMS.min",
    section = ReferenceSection,
    default = 1000,
    description = """Minimum period of time to wait before reconnecting to a dead node""")

  val DeprecatedMinReconnectionDelayParam = DeprecatedConfigParameter(
    name = "spark.cassandra.connection.reconnection_delay_ms.min",
    replacementParameter = Some(MinReconnectionDelayParam),
    deprecatedSince = "DSE 6.0.0"
  )

  val MaxReconnectionDelayParam = ConfigParameter[Int](
    name = "spark.cassandra.connection.reconnectionDelayMS.max",
    section = ReferenceSection,
    default = 60000,
    description = """Maximum period of time to wait before reconnecting to a dead node""")

  val LocalConnectionsPerExecutorParam = ConfigParameter[Option[Int]](
    name = "spark.cassandra.connection.localConnectionsPerExecutor",
    section = ReferenceSection,
    default = None,
    description =
        """Number of local connections set on each Executor JVM. Defaults to the number
          | of available CPU cores on the local node if not specified and not in a Spark Env""".stripMargin
  )

  val DeprecatedMaxReconnectionDelayParam = DeprecatedConfigParameter(
    name = "spark.cassandra.connection.reconnection_delay_ms.max",
    replacementParameter = Some(MaxReconnectionDelayParam),
    deprecatedSince = "DSE 6.0.0"
  )

  val RemoteConnectionsPerExecutorParam = ConfigParameter[Option[Int]](
    name = "spark.cassandra.connection.remoteConnectionsPerExecutor",
    section = ReferenceSection,
    default = None,
    description =
        """Minimum number of remote connections per Host set on each Executor JVM. Default value is
          | estimated automatically based on the total number of executors in the cluster""".stripMargin
  )

  val CompressionParam = ConfigParameter[String](
    name = "spark.cassandra.connection.compression",
    section = ReferenceSection,
    default = "NONE",
    description = """Compression to use (LZ4, SNAPPY or NONE)""")

  val QuietPeriodBeforeCloseParam = ConfigParameter[Int](
    name = "spark.cassandra.connection.quietPeriodBeforeCloseMS",
    section = ReferenceSection,
    default = 0,
    description = "The time in seconds that must pass without any additional request after requesting connection close (see Netty quiet period)")

  val TimeoutBeforeCloseParam = ConfigParameter[Int](
    name = "spark.cassandra.connection.timeoutBeforeCloseMS",
    section = ReferenceSection,
    default = 15000,
    description = "The time in seconds for all in-flight connections to finish after requesting connection close")

  val QueryRetryParam = ConfigParameter[Int](
    name = "spark.cassandra.query.retry.count",
    section = ReferenceSection,
    default = 60,
    description =
      """Number of times to retry a timed-out query
        |Setting this to -1 means unlimited retries
      """.stripMargin)

  val ReadTimeoutParam = ConfigParameter[Int](
    name = "spark.cassandra.read.timeoutMS",
    section = ReferenceSection,
    default = 120000,
    description = """Maximum period of time to wait for a read to return """)

  val DeprecatedReadTimeoutParam = DeprecatedConfigParameter(
    name = "spark.cassandra.read.timeout_ms",
    replacementParameter = Some(ReadTimeoutParam),
    deprecatedSince = "DSE 6.0.0"
  )

  val ResolveContactPoints = ConfigParameter[Boolean](
    name = "spark.cassandra.connection.resolveContactPoints",
    section = ReferenceSection,
    default = true,
    description =
      """Controls, if we need to resolve contact points at start (true), or at reconnection (false).
        |Helpful for usage with Kubernetes or other systems with dynamic endpoints which may change
        |while the application is running.""".stripMargin)

  val ReferenceSectionAlternativeConnection = "Alternative Connection Configuration Options"

  val CloudBasedConfigurationParam = ConfigParameter[Option[String]](
    name = "spark.cassandra.connection.config.cloud.path",
    section = ReferenceSectionAlternativeConnection,
    default = None,
    description =
      """Path to Secure Connect Bundle to be used for this connection. Accepts URLs and references to files
        |distributed via spark.files (--files) setting.<br/>
        |Provided URL must by accessible from each executor.<br/>
        |Using spark.files is recommended as it relies on Spark to distribute the bundle to every executor and
        |leverages Spark capabilities to access files located in distributed file systems like HDFS, S3, etc.
        |For example, to use a bundle located in HDFS in spark-shell:
        |
        |    spark-shell --conf spark.files=hdfs:///some_dir/bundle.zip \
        |       --conf spark.cassandra.connection.config.cloud.path=bundle.zip \
        |       --conf spark.cassandra.auth.username=<name> \
        |       --conf spark.cassandra.auth.password=<pass> ...
        |
        |""".stripMargin
  )

  val ProfileFileBasedConfigurationParam = ConfigParameter[Option[String]](
    name = "spark.cassandra.connection.config.profile.path",
    section = ReferenceSectionAlternativeConnection,
    default = None,
    description = """Specifies a default Java Driver 4.0 Profile file to be used for this connection. Accepts
                    |URLs and references to files distributed via spark.files (--files) setting.""".stripMargin
  )

  val ReferenceSectionSSL = "Cassandra SSL Connection Options"
  val DefaultCassandraSSLConf = CassandraSSLConf()

  val SSLEnabledParam = ConfigParameter[Boolean](
    name = "spark.cassandra.connection.ssl.enabled",
    section = ReferenceSectionSSL,
    default = DefaultCassandraSSLConf.enabled,
    description = """Enable secure connection to Cassandra cluster""")

  val SSLTrustStorePathParam = ConfigParameter[Option[String]](
    name = "spark.cassandra.connection.ssl.trustStore.path",
    section = ReferenceSectionSSL,
    default = DefaultCassandraSSLConf.trustStorePath,
    description = """Path for the trust store being used""")

  val SSLTrustStorePasswordParam = ConfigParameter[Option[String]](
    name = "spark.cassandra.connection.ssl.trustStore.password",
    section = ReferenceSectionSSL,
    default = DefaultCassandraSSLConf.trustStorePassword,
    description = """Trust store password""")

  val SSLTrustStoreTypeParam = ConfigParameter[String](
    name = "spark.cassandra.connection.ssl.trustStore.type",
    section = ReferenceSectionSSL,
    default = DefaultCassandraSSLConf.trustStoreType,
    description = """Trust store type""")

  val SSLProtocolParam = ConfigParameter[String](
    name = "spark.cassandra.connection.ssl.protocol",
    section = ReferenceSectionSSL,
    default = DefaultCassandraSSLConf.protocol,
    description = """SSL protocol""")

  val SSLEnabledAlgorithmsParam = ConfigParameter[Set[String]](
    name = "spark.cassandra.connection.ssl.enabledAlgorithms",
    section = ReferenceSectionSSL,
    default = DefaultCassandraSSLConf.enabledAlgorithms,
    description = """SSL cipher suites""")

  val SSLClientAuthEnabledParam = ConfigParameter[Boolean](
    name = "spark.cassandra.connection.ssl.clientAuth.enabled",
    section = ReferenceSectionSSL,
    default = DefaultCassandraSSLConf.clientAuthEnabled,
    description = """Enable 2-way secure connection to Cassandra cluster""")

  val SSLKeyStorePathParam = ConfigParameter[Option[String]](
    name = "spark.cassandra.connection.ssl.keyStore.path",
    section = ReferenceSectionSSL,
    default = DefaultCassandraSSLConf.keyStorePath,
    description = """Path for the key store being used""")

  val SSLKeyStorePasswordParam = ConfigParameter[Option[String]](
    name = "spark.cassandra.connection.ssl.keyStore.password",
    section = ReferenceSectionSSL,
    default = DefaultCassandraSSLConf.keyStorePassword,
    description = """Key store password""")

  val SSLKeyStoreTypeParam = ConfigParameter[String](
    name = "spark.cassandra.connection.ssl.keyStore.type",
    section = ReferenceSectionSSL,
    default = DefaultCassandraSSLConf.keyStoreType,
    description = """Key store type""")

  val MaxFrameLengthInMB = ConfigParameter[Int](
    name = "spark.cassandra.protocol.max-frame-length-mb",
    section = ReferenceSection,
    default = 256,
    description = """The maximum length, in MB, of the frames supported by the driver. """)

  private def maybeResolveHostAndPort(hostAndPort: String, defaultPort: Int,
                                      resolveContactPoints: Boolean): Option[InetSocketAddress] = {
    val (hostName, port) = if (hostAndPort.contains(":")) {
      val splitStr = hostAndPort.split(":")
      if (splitStr.length!= 2) throw new IllegalArgumentException(s"Couldn't parse host $hostAndPort")
      (splitStr(0), splitStr(1).toInt)
    } else {
      (hostAndPort, defaultPort)
    }

    if (resolveContactPoints) {
      try Some(new InetSocketAddress(InetAddress.getByName(hostName), port))
      catch {
        case NonFatal(e) =>
          logError(s"Unknown host '$hostName'", e)
          None
      }
    } else {
      Some(InetSocketAddress.createUnresolved(hostName, port))
    }
  }

  def apply(conf: SparkConf): CassandraConnectorConf = {
    fromSparkConf(conf)
  }

  /**
    * Determine how we should be connecting to Cassandra for this configuration
    */
  def getContactInfoFromSparkConf(conf: SparkConf): ContactInfo = {
    Seq(
      conf.getOption(CloudBasedConfigurationParam.name).map(url => CloudBasedContactInfo(url, AuthConf.fromSparkConf(conf))),
      conf.getOption(ProfileFileBasedConfigurationParam.name).map(path => ProfileFileBasedContactInfo(path)),
      Some(getIpBasedContactInfoFromSparkConf(conf))
    ).collectFirst{ case Some(contactPoint) => contactPoint}.get
  }

  private def getIpBasedContactInfoFromSparkConf(conf: SparkConf): IpBasedContactInfo = {
    val resolveContactPoints = conf.getBoolean(ResolveContactPoints.name, ResolveContactPoints.default)
    val port = conf.getInt(ConnectionPortParam.name, ConnectionPortParam.default)

    val hostsStr = conf.get(ConnectionHostParam.name, ConnectionHostParam.default)
    val hosts = for {
      hostName <- hostsStr.split(",").toSet[String]
      hostAddress <- maybeResolveHostAndPort(hostName.trim, port, resolveContactPoints)
    } yield hostAddress

    val authConf = AuthConf.fromSparkConf(conf)
    val sslEnabled = conf.getBoolean(SSLEnabledParam.name, SSLEnabledParam.default)
    val sslTrustStorePath = conf.getOption(SSLTrustStorePathParam.name).orElse(SSLTrustStorePathParam.default)
    val sslTrustStorePassword = conf.getOption(SSLTrustStorePasswordParam.name).orElse(SSLTrustStorePasswordParam.default)
    val sslTrustStoreType = conf.get(SSLTrustStoreTypeParam.name, SSLTrustStoreTypeParam.default)
    val sslProtocol = conf.get(SSLProtocolParam.name, SSLProtocolParam.default)
    val sslEnabledAlgorithms = conf.getOption(SSLEnabledAlgorithmsParam.name)
      .map(_.split(",").map(_.trim).toSet).getOrElse(SSLEnabledAlgorithmsParam.default)
    val sslClientAuthEnabled = conf.getBoolean(SSLClientAuthEnabledParam.name, SSLClientAuthEnabledParam.default)
    val sslKeyStorePath = conf.getOption(SSLKeyStorePathParam.name).orElse(SSLKeyStorePathParam.default)
    val sslKeyStorePassword = conf.getOption(SSLKeyStorePasswordParam.name).orElse(SSLKeyStorePasswordParam.default)
    val sslKeyStoreType = conf.get(SSLKeyStoreTypeParam.name, SSLKeyStoreTypeParam.default)

    val cassandraSSLConf = CassandraSSLConf(
      enabled = sslEnabled,
      trustStorePath = sslTrustStorePath,
      trustStorePassword = sslTrustStorePassword,
      trustStoreType = sslTrustStoreType,
      clientAuthEnabled = sslClientAuthEnabled,
      keyStorePath = sslKeyStorePath,
      keyStorePassword = sslKeyStorePassword,
      keyStoreType = sslKeyStoreType,
      protocol = sslProtocol,
      enabledAlgorithms = sslEnabledAlgorithms
    )

    IpBasedContactInfo(hosts, authConf, cassandraSSLConf)
  }

  def fromSparkConf(conf: SparkConf) = {

    ConfigCheck.checkConfig(conf)

    val localDC = conf.getOption(LocalDCParam.name)
    val keepAlive = conf.getInt(KeepAliveMillisParam.name, KeepAliveMillisParam.default)
    val minReconnectionDelay = conf.getInt(MinReconnectionDelayParam.name, MinReconnectionDelayParam.default)
    val maxReconnectionDelay = conf.getInt(MaxReconnectionDelayParam.name, MaxReconnectionDelayParam.default)
    val localConnections = conf.getOption(LocalConnectionsPerExecutorParam.name).map(_.toInt)
    val remoteConnections = conf.getOption(RemoteConnectionsPerExecutorParam.name).map(_.toInt)
    val queryRetryCount = conf.getInt(QueryRetryParam.name, QueryRetryParam.default)
    val connectTimeout = conf.getInt(ConnectionTimeoutParam.name, ConnectionTimeoutParam.default)
    val readTimeout = conf.getInt(ReadTimeoutParam.name, ReadTimeoutParam.default)
    val quietPeriodBeforeClose = conf.getInt(QuietPeriodBeforeCloseParam.name, QuietPeriodBeforeCloseParam.default)
    val timeoutBeforeClose = conf.getInt(TimeoutBeforeCloseParam.name, TimeoutBeforeCloseParam.default)
    val resolveContactPoints = conf.getBoolean(ResolveContactPoints.name, ResolveContactPoints.default)

    val compression = conf.getOption(CompressionParam.name).getOrElse(CompressionParam.default)

    val connectionFactory = CassandraConnectionFactory.fromSparkConf(conf)

    val maxFrameLengthInMB = conf.getInt(MaxFrameLengthInMB.name, MaxFrameLengthInMB.default)

    CassandraConnectorConf(
      contactInfo = getContactInfoFromSparkConf(conf),
      localDC = localDC,
      keepAliveMillis = keepAlive,
      minReconnectionDelayMillis = minReconnectionDelay,
      maxReconnectionDelayMillis = maxReconnectionDelay,
      localConnectionsPerExecutor = localConnections,
      remoteConnectionsPerExecutor = remoteConnections,
      compression = compression,
      queryRetryCount = queryRetryCount,
      connectTimeoutMillis = connectTimeout,
      readTimeoutMillis = readTimeout,
      connectionFactory = connectionFactory,
      quietPeriodBeforeCloseMillis = quietPeriodBeforeClose,
      timeoutBeforeCloseMillis = timeoutBeforeClose,
      resolveContactPoints = resolveContactPoints,
      maxFrameLengthInMB = maxFrameLengthInMB
    )
  }

  private def fromConnectionParams(conf: SparkConf, params: Map[String, String]): CassandraConnectorConf = {
    apply(conf.clone().setAll(
      params.collect {
        case (k, v) if ConfigCheck.validStaticPropertyNames.contains(ConfigCheck.Prefix + k) => (ConfigCheck.Prefix + k) -> v
        case (k, v) if k.startsWith("spark.") => k -> v
      }))
  }

  // used by DSE
  def fromConnectionParams(params: Map[String, String]): CassandraConnectorConf = {
    fromConnectionParams(new SparkConf(loadDefaults = false), params)
  }
}
