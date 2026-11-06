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

import java.nio.ByteBuffer

import com.datastax.oss.driver.api.core.ConsistencyLevel
import com.datastax.oss.driver.api.core.cql._
import _root_.io.teknek.arizona.spark.connector.util.maybeExecutingAs
import _root_.io.teknek.arizona.spark.connector.writer.RichStatement.DriverStatement

trait RichStatement {
  def bytesCount: Int
  def rowsCount: Int
  def stmt: DriverStatement
  def executeAs(executeAs: Option[String]): RichStatement
}

object RichStatement {
  type DriverStatement = Statement[_ <: Statement[_]]
}

private[connector] class RichBoundStatementWrapper(initStatement: BoundStatement)
  extends RichStatement {

  def update(updateFunction: BoundStatement => BoundStatement): RichBoundStatementWrapper = {
    _stmt = updateFunction(_stmt)
    this
  }

  private var _stmt = initStatement
  var bytesCount = 0
  val rowsCount = 1

  def setConsistencyLevel(consistencyLevel: ConsistencyLevel): RichBoundStatementWrapper = {
    _stmt = _stmt.setConsistencyLevel(consistencyLevel)
    this
  }

  override def stmt: BoundStatement = _stmt

  override def executeAs(executeAs: Option[String]): RichStatement = {
    _stmt = maybeExecutingAs(_stmt, executeAs)
    this
  }
}

private[connector] class RichBatchStatementWrapper(
    batchType: BatchType,
    consistencyLevel: ConsistencyLevel,
    stmts: Seq[RichBoundStatementWrapper])
  extends RichStatement {

  private var _stmt = BatchStatement.newInstance(batchType, stmts.map(_.stmt):_*).setConsistencyLevel(consistencyLevel)

  override val bytesCount: Int = stmts.map(_.bytesCount).sum

  override val rowsCount = _stmt.size()

  override def stmt: BatchStatement = _stmt

  override def executeAs(executeAs: Option[String]): RichStatement = {
    _stmt = maybeExecutingAs(_stmt, executeAs)
    this
  }
}
