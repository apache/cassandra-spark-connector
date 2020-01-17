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

import java.io.IOException

import com.datastax.oss.driver.api.core.config.{DefaultDriverOption, DriverConfigLoader}
import org.apache.spark.{SparkConf, SparkEnv}
import org.mockito.Mockito
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import org.scalatestplus.mockito.MockitoSugar

class DefaultConnectionFactoryTest extends FlatSpec with MockitoSugar {

  /** DefaultConnectionFactory relies on a non-null SparkEnv */
  private def mockedSparkEnv[T](code: => T): T = {
    val original = SparkEnv.get
    val sparkEnv = Mockito.mock(classOf[SparkEnv], new Answer[Option[String]] {
      override def answer(invocation: InvocationOnMock): Option[String] = None
    })
    SparkEnv.set(sparkEnv)
    try {
      code
    } finally {
      SparkEnv.set(original)
    }
  }

  it should "complain when a malformed URL is provided" in mockedSparkEnv {
    intercept[IOException] {
      DefaultConnectionFactory.maybeGetLocalFile("secure-bundle.zip")
    }
  }

  it should "complain when an URL with unrecognized scheme is provided" in mockedSparkEnv {
    intercept[IOException] {
      DefaultConnectionFactory.maybeGetLocalFile("hdfs:///secure-bundle.zip")
    }
  }

  it should "use NoopMetricsFactory when JMX reporting is disabled" in {
    val sparkConf = new SparkConf(loadDefaults = false)
      .set(CassandraConnectorConf.JmxEnabledParam.name, "false")
    val conf = CassandraConnectorConf(sparkConf)
    val profile = DefaultConnectionFactory
      .connectorConfigBuilder(conf, DriverConfigLoader.programmaticBuilder())
      .build()
      .getInitialConfig
      .getDefaultProfile
    profile.getString(DefaultDriverOption.METRICS_FACTORY_CLASS) shouldBe "NoopMetricsFactory"
  }

}
