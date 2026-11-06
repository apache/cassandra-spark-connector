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

package io.teknek.arizona.spark.connector.japi

import java.lang.{Boolean => JBoolean, Byte => JByte, Double => JDouble, Float => JFloat, Long => JLong, Short => JShort}
import java.math.{BigInteger, BigDecimal => JBigDecimal}
import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.{Date, UUID, List => JList, Map => JMap, Set => JSet}

import _root_.io.teknek.arizona.spark.connector.GettableByIndexData
import _root_.io.teknek.arizona.spark.connector.types.TypeConverter

trait JavaGettableByIndexData extends GettableByIndexData {

  /** Generic getter for getting columns of any type.
    * Looks the column up by its index. First column starts at index 0. */
  def get[T <: AnyRef](index: Int, tc: TypeConverter[T]): T =
    _get(index)(tc)


  /** Generic getter for getting columns of any type.
    * Looks the column up by its index. First column starts at index 0. */
  private def _get[T <: AnyRef](index: Int)(implicit tc: TypeConverter[T]): T =
    tc.convert(columnValues(index))

  /** Equivalent to `getAny` */
  def apply(index: Int): AnyRef = getObject(index)

  /** Returns a column value by index without applying any conversion.
    * The underlying type is the same as the type returned by the low-level Cassandra driver. */
  def getObject(index: Int) = _get[Object](index)

  /** Returns a `bool` column value. Besides working with `bool` Cassandra type, it can also read
    * numbers and strings. Non-zero numbers are converted to `true`, zero is converted to `false`.
    * Strings are converted using `String#toBoolean` method.*/
  def getBoolean(index: Int) = _get[JBoolean](index)

  def getByte(index: Int) = _get[JByte](index)

  def getShort(index: Int) = _get[JShort](index)

  /** Returns a column value as a 32-bit integer number.
    * Besides working with `int` Cassandra type, it can also read
    * other integer numbers as `bigint` or `varint` and strings.
    * The string must represent a valid integer number.
    * The number must be within 32-bit integer range or the `TypeConversionException` will be thrown.*/
  def getInt(index: Int) = _get[Integer](index)

  /** Returns a column value as a 64-bit integer number.
    * Recommended to use with `bigint` and `counter` CQL types
    * It can also read other column types as `int`, `varint`, `timestamp` and `string`.
    * The string must represent a valid integer number.
    * The number must be within 64-bit integer range or
    * `io.teknek.arizona.spark.connector.types.TypeConversionException`
    * will be thrown. When used with timestamps, returns a number of milliseconds since epoch.*/
  def getLong(index: Int) = _get[JLong](index)

  /** Returns a column value as Float.
    * Recommended to use with `float` CQL type.
    * This method can be also used to read a `double` or `decimal` column, with some loss of precision.*/
  def getFloat(index: Int) = _get[JFloat](index)

  /** Returns a column value as Double.
    * Recommended to use with `float` and `double` CQL types.
    * This method can be also used to read a `decimal` column, with some loss of precision.*/
  def getDouble(index: Int) = _get[JDouble](index)

  /** Returns the column value converted to a `String` acceptable by CQL.
    * All data types that have human readable text representations can be converted.
    * Note, this is not the same as calling `getAny(index).toString` which works differently e.g. for dates.*/
  def getString(index: Int) = _get[String](index)

  /** Returns a `blob` column value as ByteBuffer.
    * This method is not suitable for reading other types of columns.
    * Columns of type `blob` can be also read as Array[Byte] with the generic `get` method. */
  def getBytes(index: Int) = _get[ByteBuffer](index)

  /** Returns a `timestamp` or `timeuuid` column value as `java.util.Date`.
    * To convert a timestamp to one of other supported date types, use the generic `get` method,
    * for example:
    * {{{
    *   row.get[java.sql.Date](0)
    * }}}*/
  def getDate(index: Int) = _get[Date](index)

  /** Returns a `varint` column value.
    * Can be used with all other integer types as well as
    * with strings containing a valid integer number of arbitrary size. */
  def getVarInt(index: Int) = _get[BigInteger](index)

  /** Returns a `decimal` column value.
    * Can be used with all other floating point types as well as
    * with strings containing a valid floating point number of arbitrary precision. */
  def getDecimal(index: Int) = _get[JBigDecimal](index)

  /** Returns an `uuid` column value.
    * Can be used to read a string containing a valid UUID.*/
  def getUUID(index: Int) = _get[UUID](index)

  /** Returns an `inet` column value.
    * Can be used to read a string containing a valid
    * Internet address, given either as a host name or IP address.*/
  def getInet(index: Int) = _get[InetAddress](index)

  /** Returns a column value of User Defined Type */
  def getUDTValue(index: Int) = _get[UDTValue](index)

  /** Returns a column value of tuple type */
  def getTupleValue(index: Int) = _get[TupleValue](index)

  /** Reads a `list` column value and returns it as Scala `Vector`.
    * A null list is converted to an empty collection.
    * Items of the list are converted to the given type.
    * This method can be also used to read `set` and `map` column types.
    * For `map`, the list items are converted to key-value pairs.*/
  def getList(index: Int) = _get[JList[AnyRef]](index)

  def getList[T](index: Int)(implicit converter: TypeConverter[T]) = _get[JList[T]](index)

  /** Reads a `set` column value.
    * A null set is converted to an empty collection.
    * Items of the set are converted to the given type.
    * This method can be also used to read `list` and `map` column types.
    * For `map`, the set items are converted to key-value pairs. */
  def getSet(index: Int) = _get[JSet[AnyRef]](index)

  def getSet[T](index: Int)(implicit converter: TypeConverter[T]) = _get[JSet[T]](index)

  /** Reads a `map` column value.
    * A null map is converted to an empty collection.
    * Keys and values of the map are converted to the given types. */
  def getMap(index: Int) = _get[JMap[AnyRef, AnyRef]](index)

  /** Reads a `map` column value.
    * A null map is converted to an empty collection.
    * Keys and values of the map are converted to the given types.
    * @tparam K type of keys, must be given explicitly.
    * @tparam V type of values, must be given explicitly.*/
  def getMap[K, V](index: Int)(implicit keyConverter: TypeConverter[K], valueConverter: TypeConverter[V]) =
    _get[JMap[K, V]](index)
}
