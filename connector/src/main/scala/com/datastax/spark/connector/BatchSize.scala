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

package com.datastax.spark.connector

import com.datastax.spark.connector.writer.WriteConf

import scala.language.implicitConversions

sealed trait BatchSize

case class RowsInBatch(batchSize: Int) extends BatchSize
case class BytesInBatch(batchSize: Int) extends BatchSize

object BatchSize {
  @deprecated("Use com.datastax.spark.connector.FixedBatchSize instead of a number", "1.1")
  implicit def intToFixedBatchSize(batchSize: Int): RowsInBatch = RowsInBatch(batchSize)

  val Automatic = BytesInBatch(WriteConf.BatchSizeBytesParam.default)
}
