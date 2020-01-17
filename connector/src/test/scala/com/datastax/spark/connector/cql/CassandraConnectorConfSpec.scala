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
import scala.reflect.runtime.universe._
import org.apache.commons.lang3.SerializationUtils
import org.apache.spark.SparkConf
import org.scalatest.{FlatSpec, Matchers}

//import com.datastax.bdp.test.ng.{DataGenerator, ToString}
//import com.datastax.spark.connector.rdd.DummyFactory
import com.datastax.spark.connector.util.CustomConnectionFactory

class CassandraConnectorConfSpec extends FlatSpec with Matchers {

  it should "be serializable" in {
    val conf = CassandraConnectorConf(new SparkConf)
    SerializationUtils.roundtrip(conf)
  }

  it should "match a conf with the same settings" in {
    val conf_a = CassandraConnectorConf(new SparkConf)
    val conf_1 = CassandraConnectorConf(new SparkConf)

    conf_a should equal(conf_1)
  }

  it should "resolve default SSL settings correctly" in {
    val sparkConf = new SparkConf(loadDefaults = false)

    val connConf = CassandraConnectorConf(sparkConf)
      .contactInfo
      .asInstanceOf[IpBasedContactInfo]

    connConf.cassandraSSLConf.enabled shouldBe false
    connConf.cassandraSSLConf.trustStorePath shouldBe empty
    connConf.cassandraSSLConf.trustStorePassword shouldBe empty
    connConf.cassandraSSLConf.trustStoreType shouldBe "JKS"
    connConf.cassandraSSLConf.protocol shouldBe "TLS"
    connConf.cassandraSSLConf.enabledAlgorithms should contain theSameElementsAs Set("TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA")
    connConf.cassandraSSLConf.clientAuthEnabled shouldBe false
    connConf.cassandraSSLConf.keyStorePath shouldBe empty
    connConf.cassandraSSLConf.keyStorePassword shouldBe empty
    connConf.cassandraSSLConf.keyStoreType shouldBe "JKS"
  }

  it should "resolve provided SSL settings correctly" in {
    val sparkConf = new SparkConf(loadDefaults = false)
    sparkConf.set(CassandraConnectorConf.SSLEnabledParam.name, "true")
    sparkConf.set(CassandraConnectorConf.SSLTrustStorePathParam.name, "/etc/keys/.truststore")
    sparkConf.set(CassandraConnectorConf.SSLTrustStorePasswordParam.name, "secret")
    sparkConf.set(CassandraConnectorConf.SSLTrustStoreTypeParam.name, "JCEKS")
    sparkConf.set(CassandraConnectorConf.SSLProtocolParam.name, "SSLv3")
    sparkConf.set(CassandraConnectorConf.SSLEnabledAlgorithmsParam.name, "TLS_ECDH_RSA_WITH_AES_256_CBC_SHA384,TLS_DHE_RSA_WITH_AES_256_CBC_SHA256")
    sparkConf.set(CassandraConnectorConf.SSLClientAuthEnabledParam.name, "true")
    sparkConf.set(CassandraConnectorConf.SSLKeyStorePathParam.name, "/etc/keys/.keystore")
    sparkConf.set(CassandraConnectorConf.SSLKeyStorePasswordParam.name, "secret")
    sparkConf.set(CassandraConnectorConf.SSLKeyStoreTypeParam.name, "JCEKS")

    val connConf = CassandraConnectorConf(sparkConf)
      .contactInfo
      .asInstanceOf[IpBasedContactInfo]
    connConf.cassandraSSLConf.enabled shouldBe true
    connConf.cassandraSSLConf.trustStorePath shouldBe Some("/etc/keys/.truststore")
    connConf.cassandraSSLConf.trustStorePassword shouldBe Some("secret")
    connConf.cassandraSSLConf.trustStoreType shouldBe "JCEKS"
    connConf.cassandraSSLConf.protocol shouldBe "SSLv3"
    connConf.cassandraSSLConf.enabledAlgorithms should contain theSameElementsAs Seq("TLS_ECDH_RSA_WITH_AES_256_CBC_SHA384", "TLS_DHE_RSA_WITH_AES_256_CBC_SHA256")
    connConf.cassandraSSLConf.clientAuthEnabled shouldBe true
    connConf.cassandraSSLConf.keyStorePath shouldBe Some("/etc/keys/.keystore")
    connConf.cassandraSSLConf.keyStorePassword shouldBe Some("secret")
    connConf.cassandraSSLConf.keyStoreType shouldBe "JCEKS"
  }

  it should "produce serializedConfString that disregards names in hosts" in {
    val addressAndHost = InetAddress.getLocalHost
    val addressOnly = InetAddress.getByAddress(addressAndHost.getAddress)

    val addressOnlyConf = CassandraConnectorConf(contactInfo = IpBasedContactInfo(Set(new InetSocketAddress(addressOnly, 9042))))
    val addressAndHostConf = CassandraConnectorConf(contactInfo = IpBasedContactInfo(Set(new InetSocketAddress(addressAndHost, 9042))))

    addressOnlyConf should be(addressAndHostConf)
  }

  val addr1 = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9042)
  val addr2 = new InetSocketAddress(InetAddress.getByName("127.0.0.2"), 9042)
  val addr3 = new InetSocketAddress(InetAddress.getByName("127.0.0.3"), 9042)

  it should "be equal to a conf with the same settings and hosts" in {
    val conf1 = CassandraConnectorConf(contactInfo = IpBasedContactInfo(Set(addr1, addr2)))
    val conf2 = CassandraConnectorConf(contactInfo = IpBasedContactInfo(Set(addr1, addr2)))

    conf1 should equal(conf2)

  }

  it should "be not equal to a conf with the same settings and different hosts" in {
    val conf1 = CassandraConnectorConf(contactInfo = IpBasedContactInfo(Set(addr1, addr2)))
    val conf2 = CassandraConnectorConf(contactInfo = IpBasedContactInfo(Set(addr2, addr3)))

    conf1 shouldNot equal(conf2)
  }

  it should "be equal to a conf with the same hosts in different order" in {
    val conf1 = CassandraConnectorConf(contactInfo = IpBasedContactInfo(Set(addr1, addr2)))
    val conf2 = CassandraConnectorConf(contactInfo = IpBasedContactInfo(Set(addr2, addr1)))

    conf1 should equal(conf2)
  }

  it should "be not equal to a conf with the same hosts but different settings" in {
    val conf1 = CassandraConnectorConf(contactInfo = IpBasedContactInfo(Set(addr1, addr2)))
    val conf2 = CassandraConnectorConf(contactInfo = IpBasedContactInfo(Set(addr1, addr2)), compression = "FunPression")

    conf1 shouldNot equal(conf2)
  }

  it should "not be equal to a conf with the same settings and different hosts if no host is common" in {
    val conf1 = CassandraConnectorConf(contactInfo = IpBasedContactInfo(Set(addr1, addr2)))
    val conf2 = CassandraConnectorConf(contactInfo = IpBasedContactInfo(Set(addr3)))

    conf1 shouldNot equal(conf2)
  }

  it should "enable JMX reporting by default" in {
    val conf = CassandraConnectorConf(new SparkConf(loadDefaults = false))
    conf.jmxEnabled shouldBe true
  }

  it should "allow disabling JMX reporting through configuration" in {
    val sparkConf = new SparkConf(loadDefaults = false)
      .set(CassandraConnectorConf.JmxEnabledParam.name, "false")
    val conf = CassandraConnectorConf(sparkConf)
    conf.jmxEnabled shouldBe false
  }

  /*
  TODO:
  it should "be equals for the same settings" in {
    val gen = new DataGenerator().registerCustomCasesGeneators {
      case (_, t) if t =:= typeOf[AuthConf] => gen => Iterator(NoAuthConf) ++ gen.generate[PasswordAuthConf]()
      case (_, t) if t =:= typeOf[CassandraConnectionFactory] => gen => Iterator(CustomConnectionFactory, DefaultConnectionFactory, DummyFactory)
    }
    val cases = gen.generate[CassandraConnectorConf]()
    for (c <- cases) {
      withClue(s"Comparing ${ToString.toStringWithNames(c)} failed") {
        val duplicate = SerializationUtils.roundtrip(c)
        duplicate shouldBe c
      }
    }
  }
  */
}
