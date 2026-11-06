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

import com.datastax.oss.driver.api.core.data.{UdtValue => DriverUDTValue}
import _root_.io.teknek.arizona.spark.connector.types.NullableTypeConverter
import _root_.io.teknek.arizona.spark.connector.util.DriverUtil.toName

import scala.reflect.runtime.universe._
import scala.jdk.CollectionConverters._

final case class UDTValue(metaData: CassandraRowMetadata, columnValues: IndexedSeq[AnyRef])
  extends ScalaGettableData {

  def this(columnNames: IndexedSeq[String], columnValues: IndexedSeq[AnyRef]) =
    this(CassandraRowMetadata.fromColumnNames(columnNames), columnValues)

  def columnNames: IndexedSeq[String] = metaData.columnNames

  override def productArity: Int = columnValues.size
  override def productElement(i: Int) = columnValues(i)

  def unapply(t: UDTValue): Some[(IndexedSeq[String],IndexedSeq[AnyRef])] =
    Some((t.metaData.columnNames,t.columnValues))
}

object UDTValue {

  def fromJavaDriverUDTValue(value: DriverUDTValue): UDTValue = {
    val fields = value.getType.getFieldNames.asScala.map(f => toName(f)).toIndexedSeq
    val values = fields.map(GettableData.get(value, _))
    UDTValue(fields, values)
  }

  def fromMap(map: Map[String, Any]): UDTValue =
    new UDTValue(map.keys.toIndexedSeq, map.values.map(_.asInstanceOf[AnyRef]).toIndexedSeq)

  val TypeTag = implicitly[TypeTag[UDTValue]]
  val Symbol = typeOf[UDTValue].asInstanceOf[TypeRef].sym

  implicit object UDTValueConverter extends NullableTypeConverter[UDTValue] {
    def targetTypeTag = TypeTag
    def convertPF = {
      case x: UDTValue => x
    }
  }

  def apply(columnNames: IndexedSeq[String], columnValues: IndexedSeq[AnyRef]): UDTValue =
    new UDTValue(columnNames, columnValues)
}
