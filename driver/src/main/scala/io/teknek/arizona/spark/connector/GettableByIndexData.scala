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

import _root_.io.teknek.arizona.spark.connector.types.TypeConverter.StringConverter

trait GettableByIndexData extends Serializable {

  def columnValues: IndexedSeq[AnyRef]

  /** Returns a column value by index without applying any conversion.
    * The underlying type is the same as the type returned by the low-level Cassandra driver,
    * is implementation defined and may change in the future.
    * Cassandra nulls are returned as Scala nulls. */
  def getRaw(index: Int): AnyRef = columnValues(index)

  /** Total number of columns in this row. Includes columns with null values. */
  def length = columnValues.size

  /** Total number of columns in this row. Includes columns with null values. */
  def size = columnValues.size

  /** Returns true if column value is Cassandra null */
  def isNullAt(index: Int): Boolean =
    columnValues(index) == null
  
  /** Displays the content in human readable form, including the names and values of the columns */
  def dataAsString: String =
    columnValues
      .map(StringConverter.convert)
      .mkString("(", ", ", ")")

  override def toString =
    dataAsString

  override def equals(o: Any): Boolean = o match {
    case that: GettableByIndexData if this.columnValues == that.columnValues => true
    case _ => false
  }

  override def hashCode: Int =
    columnValues.hashCode()
}
