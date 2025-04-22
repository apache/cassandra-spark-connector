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
/**
TODO:
import org.apache.commons.lang3.SerializationUtils
import org.apache.spark.SparkConf
import org.scalatest.{FlatSpec, Matchers}

import com.datastax.bdp.config.{ClientConfigurationFactory, YamlClientConfiguration}
import com.datastax.bdp.spark.DseAuthConfFactory.DsePasswordAuthConf
import com.datastax.bdp.test.ng.{DataGenerator, DseScalaTestBase, ToString, YamlProvider}

class DseAuthConfFactorySpec extends FlatSpec with Matchers with DseScalaTestBase {
  
  beforeClass {
    YamlProvider.provideDefaultYamls()
    YamlClientConfiguration.setAsClientConfigurationImpl()
  }
  
  it should "produce equivalent AuthConf instances for the same SparkConf" in {
    def genAuthConf = DseAuthConfFactory.authConf(new SparkConf())

    genAuthConf shouldBe genAuthConf
  }

  it should "produce comparable DsePasswordAuthConf instances" in {
    val gen = new DataGenerator()
    val cases = gen.generate[DsePasswordAuthConf]()
    for (c <- cases) {
      withClue(s"Comparing ${ToString.toStringWithNames(c)} failed") {
        val duplicate = SerializationUtils.roundtrip(c)
        duplicate shouldBe c
      }
    }
  }
}
  **/
