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

import _root_.io.teknek.arizona.spark.connector.ColumnRef

/** A column map for saving objects to Cassandra.
  * Lists available getters. */
trait ColumnMapForWriting extends Serializable {
  /** Maps a getter method name to a column reference */
  def getters: Map[String, ColumnRef]
}

/** A column map for reading objects from Cassandra.
  * Describes object's constructor and setters. */
trait ColumnMapForReading extends Serializable {

  /** A sequence of column references associated with parameters of the main constructor.
    * If the class contains multiple constructors, the main constructor is assumed to be the one with the
    * highest number of parameters. Multiple constructors with the same number of parameters are not allowed. */
  def constructor: Seq[ColumnRef]

  /** Maps a setter method name to a column reference */
  def setters: Map[String, ColumnRef]

  /** Whether Java nulls are allowed to be directly set as object properties.
    * This is desired for compatibility with Java classes, but in Scala, we have Option,
    * therefore we need to fail fast if one wants to assign a Cassandra null
    * value to a non-optional property. */
  def allowsNull: Boolean
}

case class SimpleColumnMapForWriting(getters: Map[String, ColumnRef]) extends ColumnMapForWriting

case class SimpleColumnMapForReading(
  constructor: Seq[ColumnRef],
  setters: Map[String, ColumnRef],
  allowsNull: Boolean = false) extends ColumnMapForReading
