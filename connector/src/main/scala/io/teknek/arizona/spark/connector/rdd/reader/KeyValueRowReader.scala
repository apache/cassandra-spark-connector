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
import _root_.io.teknek.arizona.spark.connector.cql.TableDef
import _root_.io.teknek.arizona.spark.connector.{CassandraRowMetadata, ColumnRef, ColumnSelector}

private[connector] class KeyValueRowReaderFactory[K, V](
    keySelection: ColumnSelector,
    keyRRF: RowReaderFactory[K],
    valueRRF: RowReaderFactory[V])
  extends RowReaderFactory[(K, V)] {

  override def rowReader(table: TableDef, columnSelection: IndexedSeq[ColumnRef]): RowReader[(K, V)] = {
    val keyReader = keyRRF.rowReader(table, keySelection.selectFrom(table))
    val valueReader = valueRRF.rowReader(table, columnSelection)
    new KeyValueRowReader(keyReader, valueReader)
  }

  override def targetClass: Class[(K, V)] = classOf[(K, V)]
}

private[connector] class KeyValueRowReader[K, V](keyReader: RowReader[K], valueReader: RowReader[V])
  extends RowReader[(K, V)] {

  override def neededColumns: Option[Seq[ColumnRef]] =
    (for (keyNames <- keyReader.neededColumns; valueNames <- valueReader.neededColumns) yield keyNames ++ valueNames)
      .orElse(keyReader.neededColumns).orElse(valueReader.neededColumns)

  override def read(row: Row, rowMetaData: CassandraRowMetadata): (K, V) = {
    (keyReader.read(row, rowMetaData), valueReader.read(row, rowMetaData))
  }
}
