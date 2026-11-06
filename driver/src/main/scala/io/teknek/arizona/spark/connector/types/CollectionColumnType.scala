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


trait CollectionColumnType[T] extends ColumnType[T] {
  def isCollection = true

  protected def nestedElementTypeName(elemType: ColumnType[_]): String = elemType match {
    case _: UserDefinedType => s"frozen<${elemType.cqlTypeName}>" // non-frozen user types are only supported at top-level
    case _ => s"${elemType.cqlTypeName}"
  }
}

case class ListType[T](
  elemType: ColumnType[T],
  override val isFrozen: Boolean = false) extends CollectionColumnType[Vector[T]] {

  @transient
  lazy val scalaTypeTag = {
    implicit val elemTypeTag = elemType.scalaTypeTag
    implicitly[TypeTag[Vector[T]]]
  }

  def cqlTypeName = s"list<${nestedElementTypeName(elemType)}>"

  override def converterToCassandra: TypeConverter[_ <: AnyRef] =
    new TypeConverter.OptionToNullConverter(TypeConverter.listConverter(elemType.converterToCassandra))
}

case class SetType[T](
  elemType: ColumnType[T],
  override val isFrozen: Boolean = false) extends CollectionColumnType[Set[T]] {

  @transient
  lazy val scalaTypeTag = {
    implicit val elemTypeTag = elemType.scalaTypeTag
    implicitly[TypeTag[Set[T]]]
  }

  def cqlTypeName = s"set<${nestedElementTypeName(elemType)}>"

  override def converterToCassandra: TypeConverter[_ <: AnyRef] =
    new TypeConverter.OptionToNullConverter(TypeConverter.setConverter(elemType.converterToCassandra))
}

case class MapType[K, V](
  keyType: ColumnType[K],
  valueType: ColumnType[V],
  override val isFrozen: Boolean = false) extends CollectionColumnType[Map[K, V]] {

  @transient
  lazy val scalaTypeTag = {
    implicit val keyTypeTag = keyType.scalaTypeTag
    implicit val valueTypeTag = valueType.scalaTypeTag
    implicitly[TypeTag[Map[K, V]]]
  }

  def cqlTypeName = s"map<${nestedElementTypeName(keyType)}, ${nestedElementTypeName(valueType)}>"

  override def converterToCassandra: TypeConverter[_ <: AnyRef] =
    new TypeConverter.OptionToNullConverter(
      TypeConverter.mapConverter(keyType.converterToCassandra, valueType.converterToCassandra))
}

