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

import java.io.ObjectOutputStream

import com.datastax.oss.driver.api.core.`type`.{DataType, UserDefinedType => DriverUserDefinedType}
import com.datastax.oss.driver.api.core.`type`.codec.registry.CodecRegistry
import com.datastax.oss.driver.api.core.data.{UdtValue => DriverUDTValue}
import _root_.io.teknek.arizona.spark.connector.cql.{FieldDef, StructDef}
import _root_.io.teknek.arizona.spark.connector.types.ColumnType.fromDriverType
import _root_.io.teknek.arizona.spark.connector.types.TypeAdapters.ValueByNameAdapter
import _root_.io.teknek.arizona.spark.connector.{CassandraRowMetadata, ColumnName, UDTValue}

import scala.reflect.runtime.universe._
import scala.jdk.CollectionConverters._

/** A Cassandra user defined type field metadata. It consists of a name and an associated column type.
  * The word `column` instead of `field` is used in member names because we want to treat UDT field
  * entries in the same way as table columns, so that they are mappable to case classes.
  * This is also the reason why this class extends `FieldDef`*/
case class UDTFieldDef(columnName: String, columnType: ColumnType[_]) extends FieldDef {
  override lazy val ref = ColumnName(columnName)
}

/** A Cassandra user defined type metadata.
  * A UDT consists of a sequence of ordered fields, called `columns`. */
case class UserDefinedType(
  name: String,
  columns: IndexedSeq[UDTFieldDef],
  override val isFrozen: Boolean = false)
  extends StructDef with ColumnType[UDTValue] {

  override type Column = FieldDef

  override def isMultiCell: Boolean = !isFrozen

  def isCollection = false
  def scalaTypeTag = implicitly[TypeTag[UDTValue]]
  def cqlTypeName = name

  val fieldConvereters = columnTypes.map(_.converterToCassandra)
  private lazy val metadata = CassandraRowMetadata.fromColumnNames(columnNames)

  private lazy val valueByNameConverter = scala.util.Try(TypeConverter.forType[ValueByNameAdapter]).toOption

  override def converterToCassandra = new NullableTypeConverter[UDTValue] {
    override def targetTypeTag = UDTValue.TypeTag
    override def convertPF = {
      case udtValue: UDTValue =>
        val columnValues =
          for (i <- columns.indices) yield {
            val columnName = columnNames(i)
            val columnConverter = fieldConvereters(i)
            val columnValue = columnConverter.convert(udtValue.getRaw(columnName))
            columnValue
          }
        new UDTValue(metadata, columnValues)
      case value if valueByNameConverter.exists(_.convertPF.isDefinedAt(value)) =>
        val valuesByName = valueByNameConverter.get.convert(value)
        val columnValues =
          for (i <- columns.indices) yield {
            val columnName = columnNames(i)
            val columnConverter = fieldConvereters(i)
            val columnValue = columnConverter.convert(valuesByName.getByName(columnName))
            columnValue
          }
        new UDTValue(metadata, columnValues)
    }
  }

  override type ValueRepr = UDTValue

  override def newInstance(columnValues: Any*): UDTValue = {
    UDTValue(metadata, columnValues.map(_.asInstanceOf[AnyRef]).toIndexedSeq)
  }
}

object UserDefinedType {

  /** Converts connector's UDTValue to Cassandra Java Driver UDTValue.
    * Used when saving data to Cassandra.  */
  class DriverUDTValueConverter(dataType: DriverUserDefinedType)
    extends TypeConverter[DriverUDTValue] {

    val fieldNames = dataType.getFieldNames.asScala.toIndexedSeq
    val fieldTypes = dataType.getFieldTypes.asScala.toIndexedSeq
    val fieldConverters = fieldTypes.map(ColumnType.converterToCassandra)

    override def targetTypeTag = implicitly[TypeTag[DriverUDTValue]]

    override def convertPF = {
      case udtValue: UDTValue =>
        val toSave = dataType.newValue()
        for (i <- fieldNames.indices) {
          val fieldName = fieldNames(i)
          val fieldConverter = fieldConverters(i)
          val fieldValue = fieldConverter.convert(udtValue.getRaw(fieldName.asInternal()))
          if (fieldValue == null) {
            toSave.setToNull(i)
          } else {
            toSave.set(i, fieldValue, CodecRegistry.DEFAULT.codecFor(fieldTypes(i), fieldValue))
          }
        }
        toSave
    }

    // Fortunately we ain't gonna need serialization, because this TypeConverter is used only on the
    // write side and instantiated separately on each executor node.
    private def writeObject(oos: ObjectOutputStream): Unit =
      throw new UnsupportedOperationException(
        this.getClass.getName + " does not support serialization, because the " +
        "required underlying " + classOf[DataType].getName + " is not Serializable.")

  }

  private def fields(dataType: DriverUserDefinedType): IndexedSeq[UDTFieldDef] = unlazify {
    for ((fieldName, fieldType) <- dataType.getFieldNames.asScala.zip(dataType.getFieldTypes.asScala).toIndexedSeq) yield
      UDTFieldDef(fieldName.asInternal(), fromDriverType(fieldType))
  }

  def apply(javaUserType: DriverUserDefinedType): UserDefinedType = {
    UserDefinedType(javaUserType.getName.asInternal(), fields(javaUserType), javaUserType.isFrozen)
  }
}

