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

package io.teknek.arizona.spark.connector.japi

import com.datastax.oss.driver.api.core.cql.Row
import _root_.io.teknek.arizona.spark.connector.CassandraRowMetadata

final class CassandraRow(val metaData:CassandraRowMetadata, val columnValues: IndexedSeq[AnyRef])
  extends JavaGettableData with Serializable {

  private[spark] def this() = this(null: CassandraRowMetadata, null: IndexedSeq[AnyRef]) // required by Kryo for deserialization :(

  def this(metaData: CassandraRowMetadata, columnValues: Array[AnyRef]) =
    this(metaData, columnValues.toIndexedSeq)

  /**
    * the consturctor is for testing and backward compatibility only.
    * Use default constructor with shared metadata for memory saving and performance.
    */
  def this(columnNames: Array[String], columnValues: Array[AnyRef]) =
    this(CassandraRowMetadata.fromColumnNames(columnNames.toIndexedSeq), columnValues.toIndexedSeq)

  protected def fieldNames = metaData
  protected def fieldValues = columnValues

  def iterator = columnValues.iterator
  override def toString = "CassandraRow" + dataAsString
}


object CassandraRow {

  /** Deserializes first n columns from the given `Row` and returns them as
    * a `CassandraRow` object. The number of columns retrieved is determined by the length
    * of the columnNames argument. The columnNames argument is used as metadata for
    * the newly created `CassandraRow`, but it is not used to fetch data from
    * the input `Row` in order to improve performance. Fetching column values by name is much
    * slower than fetching by index. */
  def fromJavaDriverRow(row: Row, metaData: CassandraRowMetadata): CassandraRow = {
    new CassandraRow(metaData, io.teknek.arizona.spark.connector.CassandraRow.dataFromJavaDriverRow(row, metaData))
  }
  /** Creates a CassandraRow object from a map with keys denoting column names and
    * values denoting column values. */
  def fromMap(map: Map[String, Any]): CassandraRow = {
    val (columnNames, values) = map.unzip
    new CassandraRow(CassandraRowMetadata.fromColumnNames(columnNames.toIndexedSeq), values.map(_.asInstanceOf[AnyRef]).toArray)
  }

}
