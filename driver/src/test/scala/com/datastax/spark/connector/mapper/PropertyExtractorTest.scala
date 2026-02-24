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

package com.datastax.spark.connector.mapper

import org.junit.Assert._
import org.junit.Test

class PropertyExtractorTest {

  class TestClass(val field1: String, val field2: Int)

  @Test
  def testSimpleExtraction(): Unit = {
    val testObject = new TestClass("a", 1)
    val propertyExtractor = new PropertyExtractor(classOf[TestClass], Seq("field1", "field2"))
    val result = propertyExtractor.extract(testObject)
    assertEquals(2, result.length)
    assertEquals("a", result(0))
    assertEquals(1, result(1))
  }

  @Test
  def testAvailableProperties(): Unit = {
    val triedProperties = Seq("field1", "foo", "bar")
    val availableProperties = PropertyExtractor.availablePropertyNames(classOf[TestClass], triedProperties)
    assertEquals(Seq("field1"), availableProperties)
  }

  @Test(expected = classOf[NoSuchMethodException])
  def testWrongPropertyName(): Unit = {
    val testObject = new TestClass("a", 1)
    val propertyExtractor = new PropertyExtractor(classOf[TestClass], Seq("foo"))
    propertyExtractor.extract(testObject)
  }

}
