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

import java.nio.ByteBuffer

import com.datastax.oss.driver.api.core.cql.Row

import com.datastax.oss.driver.api.core.`type`.codec.TypeCodec
import com.datastax.oss.driver.api.core.data.{TupleValue => DriverTupleValue, UdtValue => DriverUDTValue}
import _root_.io.teknek.arizona.spark.connector.types.TypeConverter.StringConverter
import _root_.io.teknek.arizona.spark.connector.util.ByteBufferUtil
import scala.jdk.CollectionConverters._

trait GettableData extends GettableByIndexData {

  def metaData: CassandraRowMetadata

  /** Returns a column value by aliased name without applying any conversion.
    * The underlying type is the same as the type returned by the low-level Cassandra driver,
    * is implementation defined and may change in the future.
    * Cassandra nulls are returned as Scala nulls. */
  def getRaw(name: String): AnyRef = columnValues(metaData.indexOfOrThrow(name))

  /**
    * Returns a column value by cql Name
    * @param name
    * @return
    */
  def getRawCql(name: String): AnyRef = columnValues(metaData.indexOfCqlColumnOrThrow(name))


  /** Returns true if column value is Cassandra null */
  def isNullAt(name: String): Boolean = {
    columnValues(metaData.indexOfOrThrow(name)) == null
  }

  /** Returns index of column with given name or -1 if column not found */
  def indexOf(name: String): Int =
    metaData.namesToIndex(name)

  /** Returns the name of the i-th column. */
  def nameOf(index: Int): String =
    metaData.columnNames(index)

  /** Returns true if column with given name is defined and has an
    * entry in the underlying value array, i.e. was requested in the result set.
    * For columns having null value, returns true. */
  def contains(name: String): Boolean =
    metaData.namesToIndex(name) != -1

  /** Displays the content in human readable form, including the names and values of the columns */
  override def dataAsString =
    metaData.columnNames
      .zip(columnValues)
      .map(kv => kv._1 + ": " + StringConverter.convert(kv._2))
      .mkString("{", ", ", "}")

  override def toString = dataAsString

  override def equals(o: Any) = o match {
    case o: GettableData if
        this.metaData == o.metaData &&
        this.columnValues == o.columnValues => true
    case _ => false
  }

  override def hashCode =
    metaData.hashCode * 31 + columnValues.hashCode
}

object GettableData {

  /* ByteBuffers are not serializable, so we need to convert them to something that is serializable.
     Array[Byte] seems reasonable candidate. Additionally converts Java collections to Scala ones. */
  private[connector] def convert(obj: Any): AnyRef = {
    obj match {
      case bb: ByteBuffer => ByteBufferUtil.toArray(bb)
      case list: java.util.List[_] => list.asScala.view.map(convert).toList
      case set: java.util.Set[_] => set.asScala.view.map(convert).toSet
      case map: java.util.Map[_, _] => map.asScala.view.map { case (k, v) => (convert(k), convert(v))}.toMap
      case udtValue: DriverUDTValue => UDTValue.fromJavaDriverUDTValue(udtValue)
      case tupleValue: DriverTupleValue => TupleValue.fromJavaDriverTupleValue(tupleValue)
      case other => other.asInstanceOf[AnyRef]
    }
  }

  /** Deserializes given field from the DataStax Java Driver `Row` into appropriate Java type.
    * If the field is null, returns null (not Scala Option). */
  def get(row: Row, index: Int): AnyRef = {
    val data = row.getObject(index)
    if (data != null)
      convert(data)
    else
      null
  }


  /** Deserializes given field from the DataStax Java Driver `Row` into appropriate Java type by using predefined codec
    * If the field is null, returns null (not Scala Option). */
  def get(row: Row, index: Int, codec: TypeCodec[AnyRef]): AnyRef = {
    val data = row.get(index, codec)
    if (data != null)
      convert(data)
    else
      null
  }

  def get(row: Row, name: String): AnyRef = {
    val index = row.getColumnDefinitions.firstIndexOf(name)
    require(index >= 0, s"Column not found in Java driver Row: $name")
    get(row, index)
  }

  def get(row: Row, name: String, codec: TypeCodec[AnyRef]): AnyRef = {
    val index = row.getColumnDefinitions.firstIndexOf(name)
    require(index >= 0, s"Column not found in Java driver Row: $name")
    get(row, index, codec)
  }

  def get(value: DriverUDTValue, name: String): AnyRef = {
    val quotedName = "\"" + name + "\""
    val data = value.getObject(quotedName)
    if (data != null)
      convert(data)
    else
      null
  }

  def get(value: DriverTupleValue, index: Int): AnyRef = {
    val data = value.getObject(index)
    if (data != null)
      convert(data)
    else
      null
  }

}

/** Thrown when the requested column does not exist in the result set. */
class ColumnNotFoundException(message: String) extends Exception(message)
