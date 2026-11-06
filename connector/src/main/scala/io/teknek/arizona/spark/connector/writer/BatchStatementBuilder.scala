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

import com.datastax.oss.driver.api.core.ConsistencyLevel
import com.datastax.oss.driver.api.core.cql.BatchType
import _root_.io.teknek.arizona.spark.connector.util.Logging

private[connector] class BatchStatementBuilder(
  val batchType: BatchType,
  val consistencyLevel: ConsistencyLevel) extends Logging {

  /** Converts a sequence of statements into a batch if its size is greater than 1.
    * Sets the routing key and consistency level. */
  def maybeCreateBatch(stmts: Seq[RichBoundStatementWrapper]): RichStatement = {
    require(stmts.nonEmpty, "Statements list cannot be empty")
    val stmt = stmts.head

    if (stmts.size == 1) {
      stmt.setConsistencyLevel(consistencyLevel)
    } else {
      new RichBatchStatementWrapper(batchType, consistencyLevel, stmts)
    }
  }

}
