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

package com.datastax.spark.connector.mapper

import java.lang.reflect.Method

import com.datastax.oss.driver.api.core.ProtocolVersion
import com.datastax.spark.connector.ColumnRef
import com.datastax.spark.connector.cql.TableDef

import scala.reflect.ClassTag

class JavaBeanColumnMapper[T : ClassTag](columnNameOverride: Map[String, String] = Map.empty)
  extends ReflectionColumnMapper[T] {

  import com.datastax.spark.connector.mapper.JavaBeanColumnMapper._

  private def propertyName(accessorName: String) = {
    val AccessorRegex(_, strippedName) = accessorName
    val fieldName = s"${strippedName(0).toLower}${strippedName.substring(1)}"
    // For Java Beans, we need to figure out if there is
    // an equivalent name on the annotation if it has one
    annotationForFieldName(fieldName) getOrElse fieldName
  }

  override protected def isGetter(method: Method): Boolean =
    GetterRegex.findFirstMatchIn(method.getName).isDefined &&
    method.getParameterTypes.isEmpty &&
    method.getReturnType != Void.TYPE

  override protected def isSetter(method: Method): Boolean =
    SetterRegex.findFirstMatchIn(method.getName).isDefined &&
    method.getParameterTypes.length == 1 &&
    method.getReturnType == Void.TYPE

  private def resolve(name: String, columns: Map[String, ColumnRef]): Option[ColumnRef] = {
    val overridenName = columnNameOverride.getOrElse(name, name)
    ColumnMapperConvention.columnForProperty(overridenName, columns)
  }

  override protected def getterToColumnName(getterName: String, columns: Map[String, ColumnRef]) = {
    val p = propertyName(getterName)
    resolve(p, columns)
  }

  override protected def setterToColumnName(setterName: String, columns: Map[String, ColumnRef]) = {
    val p = propertyName(setterName)
    resolve(p, columns)
  }

  override protected def constructorParamToColumnName(
      paramName: String,
      columns: Map[String, ColumnRef]) = {
    resolve(paramName, columns)
  }

  /** Java Beans allow nulls in property values */
  override protected def allowsNull = true

  // TODO: Implement
  override def newTable(
    keyspaceName: String,
    tableName: String,
    protocolVersion: ProtocolVersion = ProtocolVersion.DEFAULT): TableDef = ???
}

object JavaBeanColumnMapper {
  val GetterRegex = "^(get|is)(.+)$".r
  val SetterRegex = "^(set)(.+)$".r
  val AccessorRegex = "^(get|is|set)(.+)$".r
}