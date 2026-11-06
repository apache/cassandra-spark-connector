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

package io.teknek.arizona.spark.connector.types

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.{Date, UUID}

import com.datastax.oss.driver.api.core.{DefaultProtocolVersion, ProtocolVersion}

import scala.reflect.runtime.universe._
import org.scalatest.{GivenWhenThen, Matchers, WordSpec}

class ColumnTypeSpec extends WordSpec with Matchers with GivenWhenThen {

  "A ColumnType companion object" should {

    "throw InvalidArgumentException if given unsupported type" in {
      an [IllegalArgumentException] should be thrownBy ColumnType.fromScalaType(typeOf[ColumnTypeSpec])
    }

    "allow to obtain a proper ColumnType" when {

      "given a Boolean should return BooleanType" in {
        assert (ColumnType.fromScalaType(typeOf[Boolean]) === BooleanType)
      }
      "given a java.lang.Boolean should return BooleanType" in {
        assert (ColumnType.fromScalaType(typeOf[java.lang.Boolean]) === BooleanType)
      }
      "given an Int should return IntType" in {
        assert (ColumnType.fromScalaType(typeOf[Int]) === IntType)
      }
      "given an java.lang.Integer should return IntType" in {
        assert (ColumnType.fromScalaType(typeOf[java.lang.Integer]) === IntType)
      }
      "given a Long should return BigIntType" in {
        assert (ColumnType.fromScalaType(typeOf[Long]) === BigIntType)
      }
      "given a java.lang.Long should return BigIntType" in {
        assert (ColumnType.fromScalaType(typeOf[java.lang.Long]) === BigIntType)
      }
      "given a Float should return FloatType" in {
        assert (ColumnType.fromScalaType(typeOf[Float]) === FloatType)
      }
      "given a java.lang.Float should return FloatType" in {
        assert (ColumnType.fromScalaType(typeOf[java.lang.Float]) === FloatType)
      }
      "given a Double should return DoubleType" in {
        assert (ColumnType.fromScalaType(typeOf[Double]) === DoubleType)
      }
      "given a java.lang.Double should return DoubleType" in {
        assert (ColumnType.fromScalaType(typeOf[java.lang.Double]) === DoubleType)
      }
      "given a String should return VarcharType" in {
        assert (ColumnType.fromScalaType(typeOf[String]) === VarCharType)
      }
      "given a java.lang.Short should return a SmallIntType" in {
        assert (ColumnType.fromScalaType(typeOf[java.lang.Short]) === SmallIntType)
      }
      "given a Short should return a SmallIntType" in {
        assert (ColumnType.fromScalaType(typeOf[Short]) === SmallIntType)
      }
      "given a Byte should return a TinyIntType" in {
        assert (ColumnType.fromScalaType(typeOf[Byte]) === TinyIntType)
      }
      "given a java.lang.Byte should return a TinyIntType" in {
        assert (ColumnType.fromScalaType(typeOf[java.lang.Byte]) === TinyIntType)
      }
      "given a java.lang.Short and PV3 should return a IntType" in {
        assert (ColumnType.fromScalaType(typeOf[java.lang.Short], DefaultProtocolVersion.V3) === IntType)
      }
      "given a Short and PV3 should return a IntType" in {
        assert (ColumnType.fromScalaType(typeOf[Short], DefaultProtocolVersion.V3) === IntType)
      }
      "given a Byte and PV3 should return a IntType" in {
        assert (ColumnType.fromScalaType(typeOf[Byte], DefaultProtocolVersion.V3) === IntType)
      }
      "given a java.lang.Byte and PV3 should return a IntType" in {
        assert (ColumnType.fromScalaType(typeOf[java.lang.Byte], DefaultProtocolVersion.V3) === IntType)
      }
      "given a java.util.Date should return TimestampType" in {
        assert (ColumnType.fromScalaType(typeOf[java.util.Date]) === TimestampType)
      }
      "given a java.sql.Date should return DateType" in {
        assert (ColumnType.fromScalaType(typeOf[java.sql.Date]) === DateType)
      }
      "given a java.sql.Date and a pre V4 Protcol version should return a TimestampType" in {
        assert (ColumnType.fromScalaType(typeOf[java.sql.Date], DefaultProtocolVersion.V3) === TimestampType)
      }
      "given a ByteBuffer should return BlobType" in {
        assert (ColumnType.fromScalaType(typeOf[ByteBuffer]) === BlobType)
      }
      "given an Array[Byte] should return BlobType" in {
        assert (ColumnType.fromScalaType(typeOf[Array[Byte]]) === BlobType)
      }
      "given an UUID should return UUIDType" in {
        assert (ColumnType.fromScalaType(typeOf[UUID]) === UUIDType)
      }
      "given a List[String] should return ListType(VarcharType)" in {
        assert (ColumnType.fromScalaType(typeOf[List[String]]) === ListType(VarCharType))
      }
      "given a Set[InetAddress] should return SetType(InetType)" in {
        assert (ColumnType.fromScalaType(typeOf[Set[InetAddress]]) === SetType(InetType))
      }
      "given a Map[Int, Date] should return MapType(IntType, TimestampType)" in {
        assert (ColumnType.fromScalaType(typeOf[Map[Int, Date]]) === MapType(IntType, TimestampType))
      }
      "given an Option[Int] should return IntType" in {
        assert (ColumnType.fromScalaType(typeOf[Option[Int]]) === IntType)
      }
      "given an Option[Vector[Int]] should return ListType(IntType)" in {
        assert (ColumnType.fromScalaType(typeOf[Option[Vector[Int]]]) === ListType(IntType))
      }
    }
  }

}
