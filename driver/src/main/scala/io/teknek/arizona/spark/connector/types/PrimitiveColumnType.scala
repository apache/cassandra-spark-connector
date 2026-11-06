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
import java.time.{Instant, LocalDate, LocalTime}
import java.util.UUID

import com.datastax.dse.driver.api.core.data.geometry._
import com.datastax.dse.driver.api.core.data.time.DateRange
import com.datastax.oss.driver.api.core.data.CqlDuration
import _root_.io.teknek.arizona.spark.connector.types.TypeConverter.OptionToNullConverter

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.TypeTag

trait PrimitiveColumnType[T] extends ColumnType[T] {
  def isCollection = false
}

case object TextType extends PrimitiveColumnType[String] {
  def scalaTypeTag = implicitly[TypeTag[String]]
  def cqlTypeName = "text"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[String])
}

case object AsciiType extends PrimitiveColumnType[String] {
  def scalaTypeTag = implicitly[TypeTag[String]]
  def cqlTypeName = "ascii"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[String])
}

case object VarCharType extends PrimitiveColumnType[String] {
  def scalaTypeTag = implicitly[TypeTag[String]]
  def cqlTypeName = "varchar"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[String])
}

case object IntType extends PrimitiveColumnType[Int] {
  def scalaTypeTag = implicitly[TypeTag[Int]]
  def cqlTypeName = "int"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[java.lang.Integer])
}

case object BigIntType extends PrimitiveColumnType[Long] {
  def scalaTypeTag = implicitly[TypeTag[Long]]
  def cqlTypeName = "bigint"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[java.lang.Long])
}

case object SmallIntType extends PrimitiveColumnType[Short] {
  def scalaTypeTag = implicitly[TypeTag[Short]]
  def cqlTypeName = "smallint"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[java.lang.Short])
}

case object TinyIntType extends PrimitiveColumnType[Byte] {
  def scalaTypeTag = { implicitly[TypeTag[Byte]]}
  def cqlTypeName = "tinyint"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[java.lang.Byte])
}

case object FloatType extends PrimitiveColumnType[Float] {
  def scalaTypeTag = implicitly[TypeTag[Float]]
  def cqlTypeName = "float"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[java.lang.Float])
}

case object DoubleType extends PrimitiveColumnType[Double] {
  def scalaTypeTag = implicitly[TypeTag[Double]]
  def cqlTypeName = "double"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[java.lang.Double])
}

case object BooleanType extends PrimitiveColumnType[Boolean] {
  def scalaTypeTag = implicitly[TypeTag[Boolean]]
  def cqlTypeName = "boolean"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[java.lang.Boolean])
}

case object VarIntType extends PrimitiveColumnType[BigInt] {
  def scalaTypeTag = implicitly[TypeTag[BigInt]]
  def cqlTypeName = "varint"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[java.math.BigInteger])
}

case object DecimalType extends PrimitiveColumnType[BigDecimal] {
  def scalaTypeTag = implicitly[TypeTag[BigDecimal]]
  def cqlTypeName = "decimal"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[java.math.BigDecimal])
}

case object TimestampType extends PrimitiveColumnType[Instant] {
  def scalaTypeTag = implicitly[TypeTag[Instant]]
  def cqlTypeName = "timestamp"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[java.time.Instant])
}

case object InetType extends PrimitiveColumnType[InetAddress] {
  def scalaTypeTag = implicitly[TypeTag[InetAddress]]
  def cqlTypeName = "inet"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[InetAddress])
}

case object UUIDType extends PrimitiveColumnType[UUID] {
  def scalaTypeTag = implicitly[TypeTag[UUID]]
  def cqlTypeName = "uuid"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[UUID])
}

case object TimeUUIDType extends PrimitiveColumnType[UUID] {
  def scalaTypeTag = implicitly[TypeTag[UUID]]
  def cqlTypeName = "timeuuid"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[UUID])
}

case object CounterType extends PrimitiveColumnType[Long] {
  def scalaTypeTag = implicitly[TypeTag[Long]]
  def cqlTypeName = "counter"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[java.lang.Long])
}

case object BlobType extends PrimitiveColumnType[ByteBuffer] {
  def scalaTypeTag = implicitly[TypeTag[ByteBuffer]]
  def cqlTypeName = "blob"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[java.nio.ByteBuffer])
}

case object DateType extends PrimitiveColumnType[LocalDate] {
  def scalaTypeTag = implicitly[TypeTag[LocalDate]]
  def cqlTypeName = "date"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[java.time.LocalDate])
}

case object TimeType extends PrimitiveColumnType[LocalTime] {
  def scalaTypeTag = { implicitly[TypeTag[LocalTime]]}
  def cqlTypeName = "time"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.JavaLocalTimeConverter)
}

case object DurationType extends PrimitiveColumnType[CqlDuration] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[CqlDuration]] }
  def cqlTypeName = "DurationType"
  def converterToCassandra =
    new OptionToNullConverter(TypeConverter.forType[CqlDuration])
}

case object PointType extends PrimitiveColumnType[Point] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[Point]] }
  def cqlTypeName = "PointType"
  def converterToCassandra =
    new OptionToNullConverter(TypeConverter.forType[Point])
}

case object PolygonType extends PrimitiveColumnType[Polygon] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[Polygon]] }
  def cqlTypeName = "PolygonType"
  def converterToCassandra =
    new OptionToNullConverter(TypeConverter.forType[Polygon])
}

case object LineStringType extends PrimitiveColumnType[LineString] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[LineString]] }
  def cqlTypeName = "LineStringType"
  def converterToCassandra =
    new OptionToNullConverter(TypeConverter.forType[LineString])
}

case object DateRangeType extends PrimitiveColumnType[DateRange] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[DateRange]] }
  def cqlTypeName = "DateRangeType"
  def converterToCassandra =
    new OptionToNullConverter(TypeConverter.forType[DateRange])
}

