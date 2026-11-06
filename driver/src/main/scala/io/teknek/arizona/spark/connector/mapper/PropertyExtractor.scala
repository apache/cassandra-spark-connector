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

package io.teknek.arizona.spark.connector.mapper

import java.lang.reflect.Method

import scala.util.Try

/** Extracts values from fields of an object. */
class PropertyExtractor[T](val cls: Class[T], val propertyNames: Seq[String]) extends Serializable {

  private def getter(name: String) =
    cls.getMethod(name)

  @transient
  private lazy val methods: Array[Method] =
    propertyNames.map(getter).toArray

  @transient
  private lazy val methodByName =
    methods.map(m => (m.getName, m)).toMap

  def extract(obj: T): Array[AnyRef] =
    extract(obj, Array.ofDim(methods.length))

  def extract(obj: T, target: Array[AnyRef]): Array[AnyRef] = {
    for (i <- methods.indices)
      target(i) = methods(i).invoke(obj)
    target
  }

  def extractProperty(obj: T, propertyName: String): AnyRef = {
    val m = methodByName(propertyName)
    m.invoke(obj)
  }
}

object PropertyExtractor {

  def availablePropertyNames(cls: Class[_], requestedPropertyNames: Seq[String]): Seq[String] =
    requestedPropertyNames.filter(name => Try(cls.getMethod(name)).isSuccess)

}
