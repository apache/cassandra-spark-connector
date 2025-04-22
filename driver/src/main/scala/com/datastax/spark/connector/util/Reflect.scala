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

package com.datastax.spark.connector.util

import scala.reflect.runtime.universe._

private[connector] object Reflect {

  def constructor(tpe: Type): Symbol = tpe.decl(termNames.CONSTRUCTOR)

  def member(tpe: Type, name: String): Symbol = tpe.member(TermName(name))

  def methodSymbol(tpe: Type): MethodSymbol = {
    val constructors = constructor(tpe).asTerm.alternatives.map(_.asMethod)
    val paramCount = constructors.map(_.paramLists.flatten.size).max
    constructors.filter(_.paramLists.flatten.size == paramCount) match {
      case List(onlyOne) => onlyOne
      case _             => throw new IllegalArgumentException(
        "Multiple constructors with the same number of parameters not allowed.")
    }
  }
}

