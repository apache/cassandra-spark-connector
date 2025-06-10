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

package com.datastax.spark.connector.datasource

import com.datastax.spark.connector.cql.{CassandraConnector, TableDef}
import com.datastax.spark.connector.writer.{TableWriter, WriteConf}
import com.datastax.spark.connector.{ColumnName, SomeColumns}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType
import org.apache.spark.metrics.OutputMetricsUpdater
import org.apache.spark.TaskContext

case class CassandraDriverDataWriterFactory(
  connector: CassandraConnector,
  tableDef: TableDef,
  inputSchema: StructType,
  writeConf: WriteConf)
  extends DataWriterFactory
    with StreamingDataWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = getWriter

  private def getWriter = CassandraDriverDataWriter(connector, tableDef, inputSchema, writeConf)

  override def createWriter(partitionId: Int, taskId: Long, epochId: Long): DataWriter[InternalRow] = getWriter
}

/*
Since the AsyncWriter here is not Serializable we need to place this code in a a separate object than the
factory above.
 */
case class CassandraDriverDataWriter(
  connector: CassandraConnector,
  tableDef: TableDef,
  inputSchema: StructType,
  writeConf: WriteConf) extends DataWriter[InternalRow] {

  private val unsafeRowWriterFactory = new InternalRowWriterFactory(inputSchema)

  private val columns = SomeColumns(inputSchema.fieldNames.map(name => ColumnName(name)): _*)

  private val metricsUpdater = OutputMetricsUpdater(TaskContext.get(), writeConf)

  private val asycWriter =
    TableWriter(connector, tableDef, columns, writeConf, false)(unsafeRowWriterFactory)
      .getAsyncWriter()

  private val writer = asycWriter.copy(
      successHandler = Some(metricsUpdater.batchFinished(success = true, _, _, _)),
      failureHandler = Some(metricsUpdater.batchFinished(success = false, _, _, _)))

  override def write(record: InternalRow): Unit = writer.write(record)

  override def commit(): WriterCommitMessage = {
    metricsUpdater.finish()
    writer.close()
    CassandraCommitMessage()
  }

  override def abort(): Unit = {
    metricsUpdater.finish()
    writer.close()
  }

  override def close(): Unit = {
    metricsUpdater.finish()
    //Our proxy Session Handler handles double closes by ignoring them so this is fine
    writer.close()
  }
}

case class CassandraCommitMessage() extends WriterCommitMessage
