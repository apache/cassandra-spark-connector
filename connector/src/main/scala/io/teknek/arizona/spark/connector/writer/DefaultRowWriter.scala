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

package io.teknek.arizona.spark.connector.writer

import scala.reflect.runtime.universe._
import scala.collection.Seq
import _root_.io.teknek.arizona.spark.connector.ColumnRef
import _root_.io.teknek.arizona.spark.connector.cql.TableDef
import _root_.io.teknek.arizona.spark.connector.mapper.{ColumnMapper, MappedToGettableDataConverter}

/** A `RowWriter` suitable for saving objects mappable by a [[io.teknek.arizona.spark.connector.mapper.ColumnMapper ColumnMapper]].
  * Can save case class objects, java beans and tuples. */
class DefaultRowWriter[T : TypeTag : ColumnMapper](
    table: TableDef, 
    selectedColumns: IndexedSeq[ColumnRef])
  extends RowWriter[T] {

  private val converter = MappedToGettableDataConverter[T](table, selectedColumns)
  override val columnNames = selectedColumns.map(_.columnName)

  override def readColumnValues(data: T, buffer: Array[Any]) = {
    val row = converter.convert(data)
    for (i <- columnNames.indices)
      buffer(i) = row.getRaw(i)
  }
}

object DefaultRowWriter {

  def factory[T : ColumnMapper : TypeTag] = new RowWriterFactory[T] {
    override def rowWriter(tableDef: TableDef, selectedColumns: IndexedSeq[ColumnRef]) = {
      new DefaultRowWriter[T](tableDef, selectedColumns)
    }
  }
}

