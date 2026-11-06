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

import com.datastax.oss.driver.api.core.cql.Row
import _root_.io.teknek.arizona.spark.connector._
import _root_.io.teknek.arizona.spark.connector.cql.TableDef
import _root_.io.teknek.arizona.spark.connector.mapper._
import _root_.io.teknek.arizona.spark.connector.util.JavaApiHelper

import scala.reflect.runtime.universe._


/** Transforms a Cassandra Java driver `Row` into an object of a user provided class,
  * calling the class constructor */
final class ClassBasedRowReader[R : TypeTag : ColumnMapper](
    table: TableDef,
    selectedColumns: IndexedSeq[ColumnRef])
  extends RowReader[R] {

  private val converter =
    new GettableDataToMappedTypeConverter[R](table, selectedColumns)

  private val isReadingTuples =
    typeTag[R].tpe.typeSymbol.fullName startsWith "scala.Tuple"

  override val neededColumns = {
    val ctorRefs = converter.columnMap.constructor
    val setterRefs = converter.columnMap.setters.values
    Some(ctorRefs ++ setterRefs)
  }

  override def read(row: Row,  rowMetaData: CassandraRowMetadata): R = {
    val cassandraRow = CassandraRow.fromJavaDriverRow(row, rowMetaData)
    converter.convert(cassandraRow)
  }
}


class ClassBasedRowReaderFactory[R : TypeTag : ColumnMapper] extends RowReaderFactory[R] {

  def columnMapper = implicitly[ColumnMapper[R]]

  override def rowReader(tableDef: TableDef, selection: IndexedSeq[ColumnRef]) =
    new ClassBasedRowReader[R](tableDef, selection)

  override def targetClass: Class[R] = JavaApiHelper.getRuntimeClass(typeTag[R])
}
