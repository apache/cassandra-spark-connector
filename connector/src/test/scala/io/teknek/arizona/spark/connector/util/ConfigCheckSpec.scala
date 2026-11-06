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

package io.teknek.arizona.spark.connector.util

import org.apache.spark.SparkConf
import org.scalatest.{FlatSpec, Matchers}
import _root_.io.teknek.arizona.spark.connector.cql.{AuthConfFactory, CassandraConnectionFactory, CassandraConnectorConf}
import _root_.io.teknek.arizona.spark.connector.util.ConfigCheck.ConnectorConfigurationException

object CustomConnectionFactory extends CassandraConnectionFactory {
  val CustomProperty = "spark.cassandra.connection.custom.property"
  override def properties = Set(CustomProperty)
  override def createSession(conf: CassandraConnectorConf) = ???
}

object CustomAuthConfFactory extends AuthConfFactory {
  val CustomProperty = "spark.cassandra.connection.auth.custom.credentials"
  override def properties = Set(CustomProperty)
  override def authConf(conf: SparkConf) = ???
}

class ConfigCheckSpec extends FlatSpec with Matchers  {

  "ConfigCheck" should "throw an exception when the configuration contains a invalid spark.cassandra prop" in {
    val sparkConf = new SparkConf().set("spark.cassandra.foo.bar", "foobar")
    val exception = the [ConnectorConfigurationException] thrownBy ConfigCheck.checkConfig(sparkConf)
    exception.getMessage should include ("spark.cassandra.foo.bar")
  }

  it should "suggest alternatives if you have a slight misspelling " in {
    val sparkConf = new SparkConf()
      .set("spark.cassandra.output.batch.siz.bytez", "40")
      .set("spark.cassandra.output.batch.size.row","10")
      .set("spark.cassandra.connect.host", "123.231.123.231")

    val exception = the[ConnectorConfigurationException] thrownBy ConfigCheck.checkConfig(sparkConf)
    exception.getMessage should include("spark.cassandra.output.batch.size.bytes")
    exception.getMessage should include("spark.cassandra.output.batch.size.rows")
    exception.getMessage should include("spark.cassandra.connection.host")
  }

  it should "suggest alternatives if you miss a word " in {
    val sparkConf = new SparkConf()
      .set("spark.cassandra.output.batch.bytez", "40")
      .set("spark.cassandra.output.size.row","10")
      .set("spark.cassandra.host", "123.231.123.231")

    val exception = the[ConnectorConfigurationException] thrownBy ConfigCheck.checkConfig(sparkConf)
    exception.getMessage should include("spark.cassandra.output.batch.size.bytes")
    exception.getMessage should include("spark.cassandra.output.batch.size.rows")
    exception.getMessage should include("spark.cassandra.connection.host")
  }

  it should "not throw an exception if you have a random variable not in the spark.cassandra space" in {
    val sparkConf = new SparkConf()
      .set("my.own.var", "40")
      .set("spark.cassandraOther.var","42")
    ConfigCheck.checkConfig(sparkConf)
  }

  it should "not list all options as suggestions " in {
     val sparkConf = new SparkConf()
      .set("spark.cassandra.output.batch.bytez", "40")
    val exception = the[ConnectorConfigurationException] thrownBy ConfigCheck.checkConfig(sparkConf)
    exception.getMessage shouldNot include ("connection")
    exception.getMessage shouldNot include ("input")
  }

  it should "not give suggestions when the variable is very strange " in {
    val sparkConf = new SparkConf().set("spark.cassandra.foo.bar", "foobar")
    val exception = the [ConnectorConfigurationException] thrownBy ConfigCheck.checkConfig(sparkConf)
    exception.getMessage shouldNot include ("Possible matches")
  }

  it should "accept custom ConnectionFactory properties" in {
    val sparkConf = new SparkConf()
      .set(CassandraConnectionFactory.FactoryParam.name,
        "io.teknek.arizona.spark.connector.util.CustomConnectionFactory")
      .set(CustomConnectionFactory.CustomProperty, "foobar")

    ConfigCheck.checkConfig(sparkConf)
  }

  it should "accept custom AuthConfFactory properties" in {
    val sparkConf = new SparkConf()
      .set(AuthConfFactory.FactoryParam.name,
        "io.teknek.arizona.spark.connector.util.CustomAuthConfFactory")
      .set(CustomAuthConfFactory.CustomProperty, "foobar")

    ConfigCheck.checkConfig(sparkConf)
  }

}
