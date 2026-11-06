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

package io.teknek.arizona.spark.connector.rdd.reader

import java.io.Serializable

import com.datastax.oss.driver.api.core.cql.Row
import _root_.io.teknek.arizona.spark.connector.{CassandraRow, CassandraRowMetadata, ColumnRef}
import _root_.io.teknek.arizona.spark.connector.cql.TableDef
import _root_.io.teknek.arizona.spark.connector.mapper.ColumnMapper
import _root_.io.teknek.arizona.spark.connector.types.TypeConverter
import _root_.io.teknek.arizona.spark.connector.util.MagicalTypeTricks.{DoesntHaveImplicit, IsNotSubclassOf}

import scala.annotation.implicitNotFound
import scala.reflect.runtime.universe._


/** Creates [[RowReader]] objects prepared for reading rows from the given Cassandra table. */
@implicitNotFound("No RowReaderFactory can be found for this type")
trait RowReaderFactory[T] {
  def rowReader(table: TableDef, selectedColumns: IndexedSeq[ColumnRef]): RowReader[T]
  def targetClass: Class[T]
}

/** Helper for implementing `RowReader` objects that can be used as `RowReaderFactory` objects. */
trait ThisRowReaderAsFactory[T] extends RowReaderFactory[T] {
  this: RowReader[T] =>
  def rowReader(table: TableDef, selectedColumns: IndexedSeq[ColumnRef]): RowReader[T] = this
}

trait LowPriorityRowReaderFactoryImplicits {

  trait IsSingleColumnType[T]
  implicit def isSingleColumnType[T](
    implicit
      ev1: TypeConverter[T],
      ev2: T IsNotSubclassOf (_, _),
      ev3: T IsNotSubclassOf (_, _, _)): IsSingleColumnType[T] = null

  implicit def classBasedRowReaderFactory[R <: Serializable](
    implicit
      tt: TypeTag[R],
      cm: ColumnMapper[R],
      ev: R DoesntHaveImplicit IsSingleColumnType[R]): RowReaderFactory[R]  =
    new ClassBasedRowReaderFactory[R]

  implicit def valueRowReaderFactory[T](
    implicit
      ev1: TypeConverter[T],
      ev2: IsSingleColumnType[T]): RowReaderFactory[T] =
    new ValueRowReaderFactory[T]()
}

object RowReaderFactory extends LowPriorityRowReaderFactoryImplicits {

  /** Default `RowReader`: reads a `Row` into serializable [[CassandraRow]] */
  implicit object GenericRowReader$
    extends RowReader[CassandraRow] with ThisRowReaderAsFactory[CassandraRow] {

    override def targetClass: Class[CassandraRow] = classOf[CassandraRow]

    override def read(row: Row,  rowMetaData: CassandraRowMetadata) = {
      assert(row.getColumnDefinitions.size() == rowMetaData.columnNames.size,
        "Number of columns in a row must match the number of columns in the table metadata")
      CassandraRow.fromJavaDriverRow(row, rowMetaData)
    }

    override def neededColumns: Option[Seq[ColumnRef]] = None
  }

}
