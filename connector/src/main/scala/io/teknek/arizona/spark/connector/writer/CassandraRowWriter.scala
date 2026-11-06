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

import _root_.io.teknek.arizona.spark.connector.{ColumnRef, CassandraRow}
import _root_.io.teknek.arizona.spark.connector.cql.TableDef

/** A [[RowWriter]] that can write [[CassandraRow]] objects.*/
class CassandraRowWriter(table: TableDef, selectedColumns: IndexedSeq[ColumnRef]) extends RowWriter[CassandraRow] {

  override val columnNames = selectedColumns.map(_.columnName)

  private val columns = columnNames.map(table.columnByName).toIndexedSeq
  private val converters = columns.map(_.columnType.converterToCassandra)

  override def readColumnValues(data: CassandraRow, buffer: Array[Any]) = {
    for ((c, i) <- columnNames.zipWithIndex) {
      val value = data.getRaw(c)
      val convertedValue = converters(i).convert(value)
      buffer(i) = convertedValue
    }
  }
}


object CassandraRowWriter {

  object Factory extends RowWriterFactory[CassandraRow] {
    override def rowWriter(table: TableDef, selectedColumns: IndexedSeq[ColumnRef]) =
      new CassandraRowWriter(table, selectedColumns)
  }

}
