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

package org.apache.spark.sql.cassandra

import org.apache.spark.sql.connector.expressions.{FieldReference, NamedReference}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.sources.Filter

/** Single-part column references, built the way Spark builds them (`FieldReference` is internal to Spark). */
object ColumnReferences {

  /** A reference to the top-level column `name`; the name is not parsed, so dots and quotes stay part of it. */
  def column(name: String): NamedReference = FieldReference.column(name)

  /** The attribute name Spark gives a V1 runtime filter on column `name` (the name quoted with backticks if needed). */
  def filterAttributeName(name: String): String = FieldReference.column(name).toString

  /** The V2 form of a V1 filter (`Filter.toV2` is internal to Spark). */
  def toV2(filter: Filter): Predicate = filter.toV2
}
