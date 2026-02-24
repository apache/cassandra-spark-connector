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

package com.datastax.spark.connector.ccm

import java.io.{File, IOException, InputStream}
import java.net.InetSocketAddress
import java.nio.file.{Files, Paths, StandardCopyOption}

import com.datastax.oss.driver.api.core.Version
import com.datastax.spark.connector.ccm.CcmConfig._
import org.slf4j.{Logger, LoggerFactory}

case class CcmConfig(
    nodes: Seq[Int] = Seq(1),
    // disable auto_snapshot by default to reduce disk usage when destroying schema.
    cassandraConfiguration: Map[String, Object] = Map("auto_snapshot" -> "false"),
    dseConfiguration: Map[String, Object] = Map(),
    dseRawYaml: Seq[String] = List(),
    jvmArgs: Seq[String] = List(),
    ipPrefix: String = "127.0.0.",
    createOptions: List[String] = List(),
    dseWorkloads: List[String] = List(),
    jmxPortOffset: Int = 0,
    rawVersion: String = System.getProperty("ccm.version", "5.0-beta1"),
    installDirectory: Option[String] = Option(System.getProperty("ccm.directory")),
    installBranch: Option[String] = Option(System.getProperty("ccm.branch")),
    dseEnabled: Boolean = Option(System.getProperty("ccm.dse")).exists(_.toLowerCase == "true"),
    scyllaEnabled: Boolean = Option(System.getProperty("ccm.scylla")).exists(_.toLowerCase == "true"),
    javaVersion: Option[Int] = None,
    mode: ClusterMode = ClusterModes.fromEnvVar) {

  lazy val version: Version = {
    if (scyllaEnabled) {
      resolveScyllaVersion(rawVersion)
    } else {
      Version.parse(rawVersion)
    }
  }

  private def resolveScyllaVersion(versionStr: String): Version = {
    // Check if it's in the format "release:x.x.x"
    if (versionStr.startsWith("release:")) {
      val extractedVersion = versionStr.substring(8) // Remove "release:" prefix
      logger.info(s"Extracted version from release format: $extractedVersion")
      try {
        Version.parse(extractedVersion)
      } catch {
        case _: IllegalArgumentException =>
          getVersionFromCcm()
      }
    } else {
      // For non-standard version strings, get version from CCM
      getVersionFromCcm()
    }
  }

  private def getVersionFromCcm(): Version = {
    CcmConfig.getCachedVersionFromCcm(rawVersion)
  }

  /** Enables SSL with JKS keystore format (for Cassandra). */
  def withSsl(keystorePath: String, keystorePassword: String): CcmConfig = {
    copy(cassandraConfiguration = cassandraConfiguration +
      ("client_encryption_options.enabled" -> "true") +
      ("client_encryption_options.keystore" -> keystorePath) +
      ("client_encryption_options.keystore_password" -> keystorePassword)
    )
  }

  /** Enables SSL with PEM certificate format (for Scylla). */
  def withSslPem(certPath: String, keyPath: String): CcmConfig = {
    copy(cassandraConfiguration = cassandraConfiguration +
      ("client_encryption_options.enabled" -> "true") +
      ("client_encryption_options.certificate" -> certPath) +
      ("client_encryption_options.keyfile" -> keyPath)
    )
  }

  /** Enables client authentication with JKS format. This also enables encryption with `withSsl(...)`. */
  def withSslAuth(
     keystorePath: String,
     keystorePassword: String,
     truststorePath: String,
     truststorePassword: String): CcmConfig = {
    val ssl = withSsl(keystorePath, keystorePassword)
    ssl.copy(cassandraConfiguration = ssl.cassandraConfiguration +
      ("client_encryption_options.require_client_auth" -> "true") +
      ("client_encryption_options.truststore" -> truststorePath) +
      ("client_encryption_options.truststore_password" -> truststorePassword)
    )
  }

  /** Enables client authentication with PEM format (for Scylla). */
  def withSslAuthPem(
      certPath: String,
      keyPath: String,
      truststorePath: String): CcmConfig = {
    val ssl = withSslPem(certPath, keyPath)
    ssl.copy(cassandraConfiguration = ssl.cassandraConfiguration +
      ("client_encryption_options.require_client_auth" -> "true") +
      ("client_encryption_options.truststore" -> truststorePath)
    )
  }

  def getDseVersion: Option[Version] = {
    if (dseEnabled) Option(version) else None
  }

  def getCassandraVersion: Version = {
    if (dseEnabled) {
      val stableVersion = version.nextStable()
      if (stableVersion.compareTo(DSE_V6_0_0) >= 0) {
        Version.V4_0_0
      } else if (stableVersion.compareTo(DSE_V5_1_0) >= 0) {
        V3_10
      } else if (stableVersion.compareTo(DSE_V5_0_0) >= 0) {
        V3_0_15
      } else {
        V2_1_19
      }
    } else if (scyllaEnabled) {
      V3_10
    } else {
      version
    }
  }

  def ipOfNode(n: Int): String = {
    ipPrefix + n
  }

  def jmxPort(n: Int): Integer = {
    7100 + jmxPortOffset + n
  }

  def addressOfNode(n: Int): InetSocketAddress = {
    new InetSocketAddress(ipOfNode(n), 9042)
  }

  def nodeAddresses(): Seq[InetSocketAddress] = {
    nodes.map(addressOfNode)
  }
}

object CcmConfig {

  val logger: Logger = LoggerFactory.getLogger(classOf[CcmConfig])

  // Cache for resolved Scylla versions to avoid expensive CCM cluster creation
  private val versionCache = new java.util.concurrent.ConcurrentHashMap[String, Version]()

  private[ccm] def getCachedVersionFromCcm(rawVersion: String): Version = {
    versionCache.computeIfAbsent(rawVersion, { version =>
      import scala.sys.process._

      val tmpDir = s"/tmp/get-version-${System.currentTimeMillis()}"
      val createCmd = s"""ccm create ccm_1 -i 127.0.254. -n 1:0 -v "$version" --scylla --config-dir=$tmpDir"""
      val versionCmd = s"ccm node1 versionfrombuild --config-dir=$tmpDir"

      try {
        logger.info(s"Creating temporary CCM cluster to resolve version for: $version")
        createCmd.!!
        val resolvedVersion = versionCmd.!!.trim
        logger.info(s"Resolved Scylla version: $resolvedVersion")
        Version.parse(resolvedVersion)
      } catch {
        case e: Exception =>
          throw new RuntimeException("failed to resolve scylla version", e)
      } finally {
        // Clean up the temporary cluster
        try {
          s"ccm remove --config-dir=$tmpDir".!
        } catch {
          case _: Exception => // Ignore cleanup errors
        }
      }
    })
  }

  // TLS certificate paths - files are generated by scripts/generate-test-certs.sh
  val DEFAULT_CLIENT_TRUSTSTORE_PASSWORD: String = "cassandra1sfun"
  val DEFAULT_CLIENT_TRUSTSTORE_PATH: String = "/tls/client.truststore"

  val DEFAULT_CLIENT_TRUSTSTORE_FILE: File =
    createTempStore(DEFAULT_CLIENT_TRUSTSTORE_PATH)

  val DEFAULT_CLIENT_KEYSTORE_PASSWORD: String = "cassandra1sfun"
  val DEFAULT_CLIENT_KEYSTORE_PATH: String = "/tls/client.keystore"

  val DEFAULT_CLIENT_KEYSTORE_FILE: File =
    createTempStore(DEFAULT_CLIENT_KEYSTORE_PATH)

  // Contains the same keypair as the client keystore, but in format usable by OpenSSL
  val DEFAULT_CLIENT_PRIVATE_KEY_FILE: File = createTempStore("/tls/client.key")
  val DEFAULT_CLIENT_CERT_CHAIN_FILE: File = createTempStore("/tls/client.crt")

  val DEFAULT_SERVER_TRUSTSTORE_PASSWORD: String = "cassandra1sfun"
  val DEFAULT_SERVER_TRUSTSTORE_PATH: String = "/tls/server.truststore"

  val DEFAULT_SERVER_KEYSTORE_PASSWORD: String = "cassandra1sfun"
  val DEFAULT_SERVER_KEYSTORE_PATH: String = "/tls/server.keystore"

  // Server PEM files for Scylla SSL configuration
  val DEFAULT_SERVER_CERT_PATH: String = "/tls/server.crt"
  val DEFAULT_SERVER_KEY_PATH: String = "/tls/server.key"
  val DEFAULT_SERVER_TRUSTSTORE_PEM_PATH: String = "/tls/server_truststore.pem"

  // DSE versions
  val DSE_V6_8_5: Version = Version.parse("6.8.5")
  val DSE_V6_8_3: Version = Version.parse("6.8.3")
  val DSE_V6_7_0: Version = Version.parse("6.7.0")
  val DSE_V6_0_0: Version = Version.parse("6.0.0")
  val DSE_V5_1_0: Version = Version.parse("5.1.0")
  val DSE_V5_0_0: Version = Version.parse("5.0.0")

  // C* versions
  val V5_0_0: Version = Version.parse("5.0-alpha1")
  val V4_1_0: Version = Version.parse("4.1.0")
  val V3_6_0: Version = Version.parse("3.6.0")
  val V3_10: Version = Version.parse("3.10")
  val V3_0_15: Version = Version.parse("3.0.15")
  val V2_1_19: Version = Version.parse("2.1.19")

  // artificial estimation of maximum number of nodes for this test group, may be bumped anytime.
  val MAX_NUMBER_OF_NODES: Integer = 2

  private def store(resource: String, target: File): File = {
    val resourceStream: InputStream = this.getClass.getResourceAsStream(resource)
    if (resourceStream != null) {
      Files.copy(resourceStream, target.toPath, StandardCopyOption.REPLACE_EXISTING)
      target
    } else {
      throw new IllegalStateException("Resource path not found: " + resource)
    }
  }

  /**
    * Extracts a keystore from the classpath into a temporary file.
    *
    * <p>This is needed as the keystore could be part of a built test jar used by other projects, and
    * they need to be extracted to a file system so cassandra may use them.
    *
    * @param storePath Path in classpath where the keystore exists.
    * @return The generated File.
    */
  private def createTempStore(storePath: String): File = {
    try {
      val file = File.createTempFile("client", ".store")
      file.deleteOnExit()
      store(storePath, file)
    } catch {
      case e: IOException =>
        logger.error("Failure to write keystore, SSL-enabled servers may fail to start.", e)
        throw e
    }
  }

  /** Stores the given resource in `/tmp/ccm_resources` directory. Stored file name is prefixed with the given prefix */
  def storeResource(prefix: String, resource: String): String = {
    val resourceName = Paths.get(resource).getFileName.toString
    val file = Paths.get("/tmp/ccm_resources").resolve(s"${prefix}_${resourceName}").toFile
    file.getParentFile.mkdirs()

    store(resource, file).getAbsolutePath
  }
}