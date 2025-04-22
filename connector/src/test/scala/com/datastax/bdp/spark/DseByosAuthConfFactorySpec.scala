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

package com.datastax.bdp.spark
/*
TODO:
import scala.jdk.CollectionConverters._
import scala.reflect.runtime.universe._

import org.apache.spark.SparkConf
import org.scalatest.{FlatSpec, Matchers}

import com.datastax.bdp.config.ClientConfiguration
import com.datastax.bdp.spark.DseByosAuthConfFactory.ByosAuthConf
import com.datastax.bdp.test.ng.{DataGenerator, DseScalaTestBase, ToString}
import com.datastax.bdp.transport.client.MapBasedClientConfiguration

class DseByosAuthConfFactorySpec extends FlatSpec with Matchers with DseScalaTestBase {

  "DseByosAuthConfFactory" should "provide same result instance for identical input" in {
    val first = DseByosAuthConfFactory.authConf(new SparkConf())
    for (_ <- 0 to 10) {
      DseByosAuthConfFactory.authConf(new SparkConf()) should be(first)
    }
  }

  it should "produce comparable auth configurations" in {
    val mapBasedCC1 :: mapBasedCC2 :: Nil = Iterator.continually(new MapBasedClientConfiguration(Map("a" -> "1", "b" -> "2").asJava, "abc")).take(2).toList
    val gen = new DataGenerator().registerCustomCasesGeneators {
      case (_, t) if t =:= typeOf[ClientConfiguration] => gen => Iterator(mapBasedCC1)
    }
    val cases = gen.generate[ByosAuthConf]()
    for (c <- cases) {
      withClue(s"Comparing ${ToString.toStringWithNames(c)} failed") {
        val duplicate = ByosAuthConf(mapBasedCC2, c.tokenStr, c.credentials)
        duplicate shouldBe c
      }
    }
  }

  it should "obtain cassandra hosts from spark.cassandra.connection.host" in {
    val conf = new SparkConf()
    conf.set("spark.cassandra.connection.host", "127.0.0.1,127.0.0.2")
    val dseByosClientConfiguration = DseByosAuthConfFactory.getDseByosClientConfiguration(conf)
    dseByosClientConfiguration.get("spark.cassandra.connection.host") shouldBe "127.0.0.1,127.0.0.2"
    dseByosClientConfiguration.getCassandraHosts.size shouldBe 2
  }
}

 */
