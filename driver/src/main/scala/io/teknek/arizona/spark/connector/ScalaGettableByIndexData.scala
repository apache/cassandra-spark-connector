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

trait ScalaGettableByIndexData extends GettableByIndexData {

  /** Generic getter for getting columns of any type.
    * Looks the column up by its index. First column starts at index 0. */
  def get[T](index: Int)(implicit c: TypeConverter[T]): T =
    c.convert(columnValues(index)) match {
      case null => throw new NullPointerException(
        "Unexpected null value of column " + index + ". Use get[Option[...]] to receive null values.")
      case notNull => notNull
    }

  /** Returns a `bool` column value. Besides working with `bool` Cassandra type, it can also read
    * numbers and strings. Non-zero numbers are converted to `true`, zero is converted to `false`.
    * Strings are converted using `String#toBoolean` method.*/
  def getBoolean(index: Int) = get[Boolean](index)
  def getBooleanOption(index: Int) = get[Option[Boolean]](index)

  def getByte(index: Int) = get[Byte](index)
  def getByteOption(index: Int) = get[Option[Byte]](index)

  def getShort(index: Int) = get[Short](index)
  def getShortOption(index: Int) = get[Option[Short]](index)

  /** Returns a column value as a 32-bit integer number.
    * Besides working with `int` Cassandra type, it can also read
    * other integer numbers as `bigint` or `varint` and strings.
    * The string must represent a valid integer number.
    * The number must be within 32-bit integer range or the `TypeConversionException` will be thrown.*/
  def getInt(index: Int) = get[Int](index)
  def getIntOption(index: Int) = get[Option[Int]](index)

  /** Returns a column value as a 64-bit integer number.
    * Recommended to use with `bigint` and `counter` CQL types
    * It can also read other column types as `int`, `varint`, `timestamp` and `string`.
    * The string must represent a valid integer number.
    * The number must be within 64-bit integer range or [[io.teknek.arizona.spark.connector.types.TypeConversionException]]
    * will be thrown. When used with timestamps, returns a number of milliseconds since epoch.*/
  def getLong(index: Int) = get[Long](index)
  def getLongOption(index: Int) = get[Option[Long]](index)

  /** Returns a column value as Float.
    * Recommended to use with `float` CQL type.
    * This method can be also used to read a `double` or `decimal` column, with some loss of precision.*/
  def getFloat(index: Int) = get[Float](index)
  def getFloatOption(index: Int) = get[Option[Float]](index)

  /** Returns a column value as Double.
    * Recommended to use with `float` and `double` CQL types.
    * This method can be also used to read a `decimal` column, with some loss of precision.*/
  def getDouble(index: Int) = get[Double](index)
  def getDoubleOption(index: Int) = get[Option[Double]](index)

  /** Returns the column value converted to a `String` acceptable by CQL.
    * All data types that have human readable text representations can be converted.
    * Note, this is not the same as calling `getAny(index).toString` which works differently e.g. for dates.*/
  def getString(index: Int) = get[String](index)
  def getStringOption(index: Int) = get[Option[String]](index)

  /** Returns a `blob` column value as ByteBuffer.
    * This method is not suitable for reading other types of columns.
    * Columns of type `blob` can be also read as Array[Byte] with the generic `get` method. */
  def getBytes(index: Int) = get[ByteBuffer](index)
  def getBytesOption(index: Int) = get[Option[ByteBuffer]](index)

  /** Returns a `timestamp` or `timeuuid` column value as `java.util.Date`.
    * To convert a timestamp to one of other supported date types, use the generic `get` method,
    * for example:
    * {{{
    *   row.get[java.sql.Date](0)
    * }}}*/
  def getDate(index: Int) = get[Date](index)
  def getDateOption(index: Int) = get[Option[Date]](index)

  /** Returns a `varint` column value.
    * Can be used with all other integer types as well as
    * with strings containing a valid integer number of arbitrary size. */
  def getVarInt(index: Int) = get[BigInt](index)
  def getVarIntOption(index: Int) = get[Option[BigInt]](index)

  /** Returns a `decimal` column value.
    * Can be used with all other floating point types as well as
    * with strings containing a valid floating point number of arbitrary precision. */
  def getDecimal(index: Int) = get[BigDecimal](index)
  def getDecimalOption(index: Int) = get[Option[BigDecimal]](index)

  /** Returns an `uuid` column value.
    * Can be used to read a string containing a valid UUID.*/
  def getUUID(index: Int) = get[UUID](index)
  def getUUIDOption(index: Int) = get[Option[UUID]](index)

  /** Returns an `inet` column value.
    * Can be used to read a string containing a valid
    * Internet address, given either as a host name or IP address.*/
  def getInet(index: Int) = get[InetAddress](index)
  def getInetOption(index: Int) = get[Option[InetAddress]](index)

  /** Returns a column value of User Defined Type */
  def getUDTValue(index: Int) = get[UDTValue](index)
  def getUDTValueOption(index: Int) = get[Option[UDTValue]](index)

  /** Returns a column value of cassandra tuple type */
  def getTupleValue(index: Int) = get[TupleValue](index)
  def getTupleValueOption(index: Int) = get[Option[TupleValue]](index)

  /** Reads a `list` column value and returns it as Scala `Vector`.
    * A null list is converted to an empty collection.
    * Items of the list are converted to the given type.
    * This method can be also used to read `set` and `map` column types.
    * For `map`, the list items are converted to key-value pairs.
    * @tparam T type of the list item, must be given explicitly. */
  def getList[T : TypeConverter](index: Int) =
    get[Vector[T]](index)

  /** Reads a `set` column value.
    * A null set is converted to an empty collection.
    * Items of the set are converted to the given type.
    * This method can be also used to read `list` and `map` column types.
    * For `map`, the set items are converted to key-value pairs.
    * @tparam T type of the set item, must be given explicitly. */
  def getSet[T : TypeConverter](index: Int) =
    get[Set[T]](index)

  /** Reads a `map` column value.
    * A null map is converted to an empty collection.
    * Keys and values of the map are converted to the given types.
    * @tparam K type of keys, must be given explicitly.
    * @tparam V type of values, must be given explicitly.*/
  def getMap[K : TypeConverter, V : TypeConverter](index: Int) =
    get[Map[K, V]](index)

  def copy: ScalaGettableByIndexData = this  // this class is immutable

  def iterator = columnValues.iterator
}
