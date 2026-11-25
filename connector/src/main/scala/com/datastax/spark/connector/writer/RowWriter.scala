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


/** `RowWriter` knows how to extract column names and values from custom row objects
  * and how to convert them to values that can be written to Cassandra.
  * `RowWriter` is required to apply any user-defined data type conversion. */
trait RowWriter[T] extends Serializable {

  /** List of columns this `RowWriter` is going to write.
    * Used to construct appropriate INSERT or UPDATE statement. */
  def columnNames: Seq[String]

  /** Extracts column values from `data` object and writes them into the given buffer
    * in the same order as they are listed in the columnNames sequence. */
  def readColumnValues(data: T, buffer: Array[Any]): Unit

}
