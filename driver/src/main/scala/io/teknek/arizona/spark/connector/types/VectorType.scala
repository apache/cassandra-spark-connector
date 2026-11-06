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

import scala.language.existentials
import scala.reflect.runtime.universe._

case class VectorType[T](elemType: ColumnType[T], dimension: Int) extends ColumnType[Seq[T]] {

  override def isCollection: Boolean = false

  @transient
  lazy val scalaTypeTag = {
    implicit val elemTypeTag = elemType.scalaTypeTag
    implicitly[TypeTag[Seq[T]]]
  }

  def cqlTypeName = s"vector<${elemType.cqlTypeName}, ${dimension}>"

  override def converterToCassandra: TypeConverter[_ <: AnyRef] =
    new TypeConverter.OptionToNullConverter(TypeConverter.seqConverter(elemType.converterToCassandra))
}
