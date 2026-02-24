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

package com.datastax.spark.connector.writer

import scala.reflect.runtime.universe._
import com.datastax.spark.connector.ColumnRef
import com.datastax.spark.connector.cql.TableDef
import com.datastax.spark.connector.mapper.ColumnMapper
import org.apache.spark.sql.Row

/** Creates instances of [[RowWriter]] objects for the given row type `T`.
  * `RowWriterFactory` is the trait you need to implement if you want to support row representations
  * which cannot be simply mapped by a [[com.datastax.spark.connector.mapper.ColumnMapper ColumnMapper]]. */
trait RowWriterFactory[T] {

  /** Creates a new `RowWriter` instance.
    * @param table target table the user wants to write into
    * @param selectedColumns columns selected by the user; the user might wish to write only a
    *                        subset of columns */
  def rowWriter(table: TableDef, selectedColumns: IndexedSeq[ColumnRef]): RowWriter[T]
}

/** Provides a low-priority implicit `RowWriterFactory` able to write objects of any class for which
  * a [[com.datastax.spark.connector.mapper.ColumnMapper ColumnMapper]] is defined. */
trait LowPriorityRowWriterFactoryImplicits {
  implicit def sqlRowWriterFactory[T <: Row : TypeTag]: RowWriterFactory[Row] =
    SqlRowWriter.Factory

  implicit def defaultRowWriterFactory[T : TypeTag : ColumnMapper]: RowWriterFactory[T] =
    DefaultRowWriter.factory
}

/** Provides an implicit `RowWriterFactory` for saving [[com.datastax.spark.connector.CassandraRow CassandraRow]] objects. */
object RowWriterFactory extends LowPriorityRowWriterFactoryImplicits {
  implicit val genericRowWriterFactory: CassandraRowWriter.Factory.type = CassandraRowWriter.Factory
}