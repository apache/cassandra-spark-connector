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
import _root_.io.teknek.arizona.spark.connector.{CassandraRowMetadata, ColumnRef}

/** Transforms a Cassandra Java driver `Row` into high-level row representation, e.g. a tuple
  * or a user-defined case class object. The target type `T` must be serializable. */
trait RowReader[T] extends Serializable {

  /** Reads column values from low-level `Row` and turns them into higher level representation.
 *
    * @param row row fetched from Cassandra
    * @param rowMetaData column names and codec available in the `row` */
  def read(row: Row, rowMetaData: CassandraRowMetadata): T

  /** List of columns this `RowReader` is going to read.
    * Useful to avoid fetching the columns that are not needed. */
  def neededColumns: Option[Seq[ColumnRef]]

}
