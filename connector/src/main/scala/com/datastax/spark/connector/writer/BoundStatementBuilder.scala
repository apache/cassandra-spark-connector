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

import com.datastax.oss.driver.api.core.`type`.DataType
import com.datastax.oss.driver.api.core.cql.{BoundStatement, PreparedStatement}
import com.datastax.oss.driver.api.core.{DefaultProtocolVersion, ProtocolVersion}
import com.datastax.spark.connector.types.{ColumnType, Unset}
import com.datastax.spark.connector.util.{CodecRegistryUtil, Logging}

/**
 * Class for binding row-like objects into prepared statements. prefixVals
 * is used for binding constant values into each bound statement. This supports parametrized
 * .where clauses in [[com.datastax.spark.connector.rdd.CassandraJoinRDD]]
 */
private[connector] class BoundStatementBuilder[T](
    val rowWriter: RowWriter[T],
    val preparedStmt: PreparedStatement,
    val prefixVals: Seq[Any] = Seq.empty,
    val ignoreNulls: Boolean = false,
    val protocolVersion: ProtocolVersion) extends Logging {

  private val columnNames = rowWriter.columnNames.toIndexedSeq
  private val columnTypes = columnNames.map(preparedStmt.getVariableDefinitions.get(_).getType)
  private val converters = columnTypes.map(ColumnType.converterToCassandra(_))
  private val buffer = Array.ofDim[Any](columnNames.size)

  /** Whether the prepared statement contains an auto-timestamp placeholder that
    * needs to be bound with a unique, incrementing microsecond timestamp per row. */
  private val hasAutoTimestamp: Boolean = {
    val varDefs = preparedStmt.getVariableDefinitions
    (0 until varDefs.size()).exists(i => varDefs.get(i).getName.asInternal() == TableWriter.AutoTimestampParam)
  }

  /** Monotonically incrementing microsecond counter for auto-timestamps.
    * Uses a global counter to guarantee uniqueness across all concurrent
    * BoundStatementBuilder instances within the same JVM. */
  private val autoTsCounter = BoundStatementBuilder.globalAutoTsCounter


  require(!ignoreNulls || protocolVersion.getCode >= DefaultProtocolVersion.V4.getCode,
    s"""
       |Protocol Version $protocolVersion does not support ignoring null values and leaving
       |parameters unset. This is only supported in ${DefaultProtocolVersion.V4.getCode} and greater.
    """.stripMargin)

  var logUnsetToNullWarning = false
  val UnsetToNullWarning =
    s"""Unset values can only be used with C* >= 2.2. They have been replaced
        |with nulls. Found protocol version ${protocolVersion}.
        |${DefaultProtocolVersion.V4.getCode}  or greater required"
    """.stripMargin


  private def maybeLeaveUnset(
    boundStatement: BoundStatement,
    columnName: String): Unit = protocolVersion match {
      case pv if (pv.getCode() <= DefaultProtocolVersion.V3.getCode()) => {
        boundStatement.setToNull(columnName)
        logUnsetToNullWarning = true
      }
      case _ =>
  }

  private def bindColumnNull(
    boundStatement: RichBoundStatementWrapper,
    columnName: String,
    columnType: DataType,
    columnValue: AnyRef): Unit = {

    if (columnValue == Unset || (ignoreNulls && columnValue == null)) {
      boundStatement.update(s => s.setToNull(columnName))
      logUnsetToNullWarning = true
    } else {
      val codec = CodecRegistryUtil.codecFor(boundStatement.stmt.codecRegistry(),columnType, columnValue)
      boundStatement.update(s => s.set(columnName, columnValue, codec))
    }
  }

  private def bindColumnUnset(
    boundStatement: RichBoundStatementWrapper,
    columnName: String,
    columnType: DataType,
    columnValue: AnyRef): Unit = {

    if (columnValue == Unset || (ignoreNulls && columnValue == null)) {
      //Do not bind
    } else {
      val codec = CodecRegistryUtil.codecFor(boundStatement.stmt.codecRegistry(),columnType, columnValue)
      boundStatement.update(s => s.set(columnName, columnValue, codec))
    }
  }

  /**
  * If the protocol version is greater than V3 (C* 2.2 and Greater) then
  * we can leave values in the prepared statement unset. If the version is
  * less than V3 then we need to place a `null` in the bound statement.
  */
  val bindColumn: (RichBoundStatementWrapper, String, DataType, AnyRef) => Unit = protocolVersion match {
    case pv if pv.getCode() <= DefaultProtocolVersion.V3.getCode => bindColumnNull
    case _ => bindColumnUnset
  }

  private val prefixConverted = for {
    prefixIndex: Int <- prefixVals.indices
    prefixVal = prefixVals(prefixIndex)
    prefixType = preparedStmt.getVariableDefinitions.get(prefixIndex).getType
    prefixConverter =  ColumnType.converterToCassandra(prefixType)
  } yield prefixConverter.convert(prefixVal)

  /** Creates `BoundStatement` from the given data item */
  def bind(row: T): RichBoundStatementWrapper = {

    val boundStatement = new RichBoundStatementWrapper(preparedStmt.bind(prefixConverted: _*))

    rowWriter.readColumnValues(row, buffer)
    var bytesCount = 0
    for (i <- columnNames.indices) {
      val converter = converters(i)
      val columnName = columnNames(i)
      val columnType = columnTypes(i)
      val columnValue = converter.convert(buffer(i))
      bindColumn(boundStatement, columnName, columnType, columnValue)
      val serializedValue = boundStatement.stmt.getBytesUnsafe(i)
      if (serializedValue != null) bytesCount += serializedValue.remaining()
    }

    if (hasAutoTimestamp) {
      boundStatement.update(_.setLong(TableWriter.AutoTimestampParam, autoTsCounter.getAndIncrement()))
    }

    boundStatement.bytesCount = bytesCount
    boundStatement
  }
}

private[connector] object BoundStatementBuilder {

  /** Global monotonically incrementing microsecond counter shared across all
    * BoundStatementBuilder instances in the JVM. Ensures that every bound
    * statement gets a unique timestamp even when multiple Spark tasks bind
    * concurrently. Initialized to the current wall-clock time in microseconds
    * so that timestamps stay in the same ballpark as server-assigned ones. */
  private[writer] val globalAutoTsCounter =
    new java.util.concurrent.atomic.AtomicLong(System.currentTimeMillis() * 1000)

  /** Calculate bound statement size in bytes. */
  def calculateDataSize(stmt: BoundStatement): Int = {
    var size = 0
    for (i <- 0 until stmt.getPreparedStatement.getVariableDefinitions.size())
      if (!stmt.isNull(i)) size += stmt.getBytesUnsafe(i).remaining()

    size
  }
}
