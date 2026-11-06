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

import com.datastax.oss.driver.api.core.data.{TupleValue => DriverTupleValue}
import _root_.io.teknek.arizona.spark.connector.types.NullableTypeConverter

import scala.reflect.runtime.universe._

final case class TupleValue(values: Any*) extends ScalaGettableByIndexData with Product {
  override def columnValues = values.toIndexedSeq.map(_.asInstanceOf[AnyRef])

  override def productArity: Int = columnValues.size

  override def productElement(n: Int): Any = getRaw(n)
}

object TupleValue {

  def fromJavaDriverTupleValue
      (value: DriverTupleValue)
      : TupleValue = {
    val values =
      for (i <- 0 until value.getType.getComponentTypes.size()) yield
        GettableData.get(value, i)
    new TupleValue(values: _*)
  }

  val TypeTag = typeTag[TupleValue]
  val Symbol = typeOf[TupleValue].asInstanceOf[TypeRef].sym

  implicit object TupleValueConverter extends NullableTypeConverter[TupleValue] {
    def targetTypeTag = TypeTag
    def convertPF = {
      case x: TupleValue => x
    }
  }
}