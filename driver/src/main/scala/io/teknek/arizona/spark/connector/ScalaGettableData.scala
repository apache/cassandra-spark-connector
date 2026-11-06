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

package io.teknek.arizona.spark.connector

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.{UUID, Date}

import _root_.io.teknek.arizona.spark.connector.types.TypeConverter
import _root_.io.teknek.arizona.spark.connector.types.TypeConverter.StringConverter

trait ScalaGettableData extends ScalaGettableByIndexData with GettableData {

  /** Converts this row to a Map */
  def toMap: Map[String, Any] =
    metaData.columnNames.zip(columnValues).toMap

  /** Generic getter for getting columns of any type.
    * Looks the column up by column name. Column names are case-sensitive.*/
  def get[T](name: String)(implicit c: TypeConverter[T]): T =
    get[T](metaData.indexOfOrThrow(name))

  /** Returns a `bool` column value. Besides working with `bool` Cassandra type, it can also read
    * numbers and strings. Non-zero numbers are converted to `true`, zero is converted to `false`.
    * Strings are converted using `String#toBoolean` method.*/
  def getBoolean(name: String) = get[Boolean](name)
  def getBooleanOption(name: String) = get[Option[Boolean]](name)

  def getByte(name: String) = get[Byte](name)
  def getByteOption(name: String) = get[Option[Byte]](name)

  def getShort(name: String) = get[Short](name)
  def getShortOption(name: String) = get[Option[Short]](name)

  /** Returns a column value as a 32-bit integer number.
    * Besides working with `int` Cassandra type, it can also read
    * other integer numbers as `bigint` or `varint` and strings.
    * The string must represent a valid integer number.
    * The number must be within 32-bit integer range or the `TypeConversionException` will be thrown.*/
  def getInt(name: String) = get[Int](name)
  def getIntOption(name: String) = get[Option[Int]](name)

  /** Returns a column value as a 64-bit integer number.
    * Recommended to use with `bigint` and `counter` CQL types
    * It can also read other column types as `int`, `varint`, `timestamp` and `string`.
    * The string must represent a valid integer number.
    * The number must be within 64-bit integer range or [[io.teknek.arizona.spark.connector.types.TypeConversionException]]
    * will be thrown. When used with timestamps, returns a number of milliseconds since epoch.*/
  def getLong(name: String) = get[Long](name)
  def getLongOption(name: String) = get[Option[Long]](name)

  /** Returns a column value as Float.
    * Recommended to use with `float` CQL type.
    * This method can be also used to read a `double` or `decimal` column, with some loss of precision.*/
  def getFloat(name: String) = get[Float](name)
  def getFloatOption(name: String) = get[Option[Float]](name)

  /** Returns a column value as Double.
    * Recommended to use with `float` and `double` CQL types.
    * This method can be also used to read a `decimal` column, with some loss of precision.*/
  def getDouble(name: String) = get[Double](name)
  def getDoubleOption(name: String) = get[Option[Double]](name)

  /** Returns the column value converted to a `String` acceptable by CQL.
    * All data types that have human readable text representations can be converted.
    * Note, this is not the same as calling `getAny(index).toString` which works differently e.g. for dates.*/
  def getString(name: String) = get[String](name)
  def getStringOption(name: String) = get[Option[String]](name)

  /** Returns a `blob` column value as ByteBuffer.
    * This method is not suitable for reading other types of columns.
    * Columns of type `blob` can be also read as Array[Byte] with the generic `get` method. */
  def getBytes(name: String) = get[ByteBuffer](name)
  def getBytesOption(name: String) = get[Option[ByteBuffer]](name)

  /** Returns a `timestamp` or `timeuuid` column value as `java.util.Date`.
    * To convert a timestamp to one of other supported date types, use the generic `get` method,
    * for example:
    * {{{
    *   row.get[java.sql.Date](0)
    * }}}*/
  def getDate(name: String) = get[Date](name)
  def getDateOption(name: String) = get[Option[Date]](name)

  /** Returns a `varint` column value.
    * Can be used with all other integer types as well as
    * with strings containing a valid integer number of arbitrary size. */
  def getVarInt(name: String) = get[BigInt](name)
  def getVarIntOption(name: String) = get[Option[BigInt]](name)

  /** Returns a `decimal` column value.
    * Can be used with all other floating point types as well as
    * with strings containing a valid floating point number of arbitrary precision. */
  def getDecimal(name: String) = get[BigDecimal](name)
  def getDecimalOption(name: String) = get[Option[BigDecimal]](name)

  /** Returns an `uuid` column value.
    * Can be used to read a string containing a valid UUID.*/
  def getUUID(name: String) = get[UUID](name)
  def getUUIDOption(name: String) = get[Option[UUID]](name)

  /** Returns an `inet` column value.
    * Can be used to read a string containing a valid
    * Internet address, given either as a host name or IP address.*/
  def getInet(name: String) = get[InetAddress](name)
  def getInetOption(name: String) = get[Option[InetAddress]](name)

  /** Returns a column value of User Defined Type */
  def getUDTValue(name: String) = get[UDTValue](name)
  def getUDTValueOption(name: String) = get[Option[UDTValue]](name)

  /** Returns a column value of cassandra tuple type */
  def getTupleValue(name: String) = get[TupleValue](name)
  def getTupleValueOption(name: String) = get[Option[TupleValue]](name)

  /** Reads a `list` column value and returns it as Scala `Vector`.
    * A null list is converted to an empty collection.
    * Items of the list are converted to the given type.
    * This method can be also used to read `set` and `map` column types.
    * For `map`, the list items are converted to key-value pairs.
    * @tparam T type of the list item, must be given explicitly. */
  def getList[T : TypeConverter](name: String) =
    get[Vector[T]](name)

  /** Reads a `set` column value.
    * A null set is converted to an empty collection.
    * Items of the set are converted to the given type.
    * This method can be also used to read `list` and `map` column types.
    * For `map`, the set items are converted to key-value pairs.
    * @tparam T type of the set item, must be given explicitly. */
  def getSet[T : TypeConverter](name: String) =
    get[Set[T]](name)

  /** Reads a `map` column value.
    * A null map is converted to an empty collection.
    * Keys and values of the map are converted to the given types.
    * @tparam K type of keys, must be given explicitly.
    * @tparam V type of values, must be given explicitly.*/
  def getMap[K : TypeConverter, V : TypeConverter](name: String) =
    get[Map[K, V]](name)

  override def copy: ScalaGettableData = this  // this class is immutable
}
