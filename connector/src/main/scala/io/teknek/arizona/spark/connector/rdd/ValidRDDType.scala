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

package io.teknek.arizona.spark.connector.rdd

import java.io.{Serializable => JavaSerializable}

import _root_.io.teknek.arizona.spark.connector.types.TypeConverter

import scala.annotation.implicitNotFound

@implicitNotFound("Not a valid RDD type. There should exists either a type converter for the type or the type should implement Serializable")
trait ValidRDDType[T]

object ValidRDDType {
  implicit def withTypeConverterAsValidRDDType[T](implicit tc: TypeConverter[T]): ValidRDDType[T] = null

  implicit def javaSerializableAsValidRDDType[T <: JavaSerializable]: ValidRDDType[T] = null
}
