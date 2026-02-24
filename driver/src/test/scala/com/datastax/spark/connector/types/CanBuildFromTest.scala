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

package com.datastax.spark.connector.types

import org.apache.commons.lang3.SerializationUtils
import org.junit.Assert._
import org.junit.Test

class CanBuildFromTest {

  @Test
  def testBuild(): Unit = {
    val bf = CanBuildFrom.setCanBuildFrom[Int]
    val builder = bf.apply()
    builder += 1
    builder += 2
    builder += 3
    assertEquals(Set(1,2,3), builder.result())
  }

  @Test
  def testSerializeAndBuild(): Unit = {
    val bf = CanBuildFrom.setCanBuildFrom[Int]
    val bf2 = SerializationUtils.roundtrip(bf)
    val builder = bf2.apply()
    builder += 1
    builder += 2
    builder += 3
    assertEquals(Set(1,2,3), builder.result())
  }

  @Test
  def testSerializeAndBuildWithOrdering(): Unit = {
    val bf = CanBuildFrom.treeSetCanBuildFrom[Int]
    val bf2 = SerializationUtils.roundtrip(bf)
    val builder = bf2.apply()
    builder += 1
    builder += 2
    builder += 3
    assertEquals(Set(1,2,3), builder.result())
  }


}
