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

class TypeSerializationTest {

  private def testSerialization(t: ColumnType[_]): Unit = {
    assertEquals(t, SerializationUtils.roundtrip(t))
  }

  @Test
  def testSerializationOfPrimitiveTypes(): Unit = {
    testSerialization(AsciiType)
    testSerialization(TextType)
    testSerialization(IntType)
    testSerialization(BigIntType)
    testSerialization(DoubleType)
    testSerialization(FloatType)
    testSerialization(BooleanType)
    testSerialization(UUIDType)
    testSerialization(TimeUUIDType)
    testSerialization(TimestampType)
    testSerialization(DecimalType)
    testSerialization(BigIntType)
    testSerialization(InetType)
    testSerialization(CounterType)
    testSerialization(SmallIntType)
    testSerialization(TinyIntType)
    testSerialization(DateType)
  }

  @Test
  def testSerializationOfCollectionTypes(): Unit = {
    testSerialization(ListType(IntType))
    testSerialization(ListType(ListType(IntType)))
    testSerialization(SetType(TextType))
    testSerialization(MapType(BigIntType, TimestampType))
  }


}
