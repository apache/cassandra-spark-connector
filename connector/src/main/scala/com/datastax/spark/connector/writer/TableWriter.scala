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

import java.io.{Closeable, IOException}
import java.util.function.Supplier

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{DefaultBatchType, PreparedStatement, SimpleStatement}
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.rdd.partitioner.{CassandraPartition, CqlTokenRange}
import com.datastax.spark.connector.types.{ListType, MapType}
import com.datastax.spark.connector.util.Quote._
import com.datastax.spark.connector.util._
import com.datastax.spark.connector.writer.AsyncExecutor.Handler
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.metrics.OutputMetricsUpdater

import scala.collection._

/** Writes RDD data into given Cassandra table.
  * Individual column values are extracted from RDD objects using given [[RowWriter]]
  * Then, data are inserted into Cassandra with batches of CQL INSERT statements.
  * Each RDD partition is processed by a single thread. */
class TableWriter[T] private (
    connector: CassandraConnector,
    tableDef: TableDef,
    columnSelector: IndexedSeq[ColumnRef],
    rowWriter: RowWriter[T],
    writeConf: WriteConf,
    partitions: Array[Partition],
    tokenRangeAcc: Option[TokenRangeAccumulator]) extends Serializable with Logging {

  require(!tableDef.isView,
    s"${tableDef.name} is a Materialized View and Views are not writable")

  val keyspaceName = tableDef.keyspaceName
  val tableName = tableDef.tableName
  val columnNames = rowWriter.columnNames diff writeConf.optionPlaceholders
  val columns = columnNames.map(tableDef.columnByName)

  private[connector] lazy val queryTemplateUsingInsert: String = {
    val quotedColumnNames: Seq[String] = columnNames.map(quote)
    val columnSpec = quotedColumnNames.mkString(", ")
    val valueSpec = quotedColumnNames.map(":" + _).mkString(", ")

    val ifNotExistsSpec = if (writeConf.ifNotExists) "IF NOT EXISTS " else ""

    val ttlSpec = writeConf.ttl match {
      case TTLOption(PerRowWriteOptionValue(placeholder)) => Some(s"""TTL :$placeholder""")
      case TTLOption(StaticWriteOptionValue(value)) => Some(s"TTL $value")
      case _ => None
    }

    val timestampSpec = writeConf.timestamp match {
      case TimestampOption(PerRowWriteOptionValue(placeholder)) => Some(s"""TIMESTAMP :$placeholder""")
      case TimestampOption(StaticWriteOptionValue(value)) => Some(s"TIMESTAMP $value")
      case _ => None
    }

    val options = List(ttlSpec, timestampSpec).flatten
    val optionsSpec = if (options.nonEmpty) s"USING ${options.mkString(" AND ")}" else ""

    s"INSERT INTO ${quote(keyspaceName)}.${quote(tableName)} ($columnSpec) VALUES ($valueSpec) $ifNotExistsSpec$optionsSpec".trim
  }

  private def deleteQueryTemplate(deleteColumns: ColumnSelector): String = {
    val deleteColumnNames: Seq[String] = deleteColumns.selectFrom(tableDef).map(_.columnName)
    val (primaryKey, regularColumns) = columns.partition(_.isPrimaryKeyColumn)
    if (regularColumns.nonEmpty) {
      throw new IllegalArgumentException(
        s"Only primary key columns can be used in delete. Regular columns found: ${regularColumns.mkString(", ")}")
    }
    TableWriter.checkMissingColumns(tableDef, deleteColumnNames)

    def quotedColumnNames(columns: Seq[ColumnDef]) = columns.map(_.columnName).map(quote)
    val deleteColumnsClause = deleteColumnNames.map(quote).mkString(", ")
    val whereClause = quotedColumnNames(primaryKey).map(c => s"$c = :$c").mkString(" AND ")

    s"DELETE ${deleteColumnsClause} FROM ${quote(keyspaceName)}.${quote(tableName)} WHERE $whereClause"
  }
  private lazy val queryTemplateUsingUpdate: String = {
    val (primaryKey, regularColumns) = columns.partition(_.isPrimaryKeyColumn)
    val (counterColumns, nonCounterColumns) = regularColumns.partition(_.isCounterColumn)

    val nameToBehavior = (columnSelector collect {
        case cn:CollectionColumnName => cn.columnName -> cn.collectionBehavior
      }).toMap

    val setNonCounterColumnsClause = for {
      colDef <- nonCounterColumns
      name = colDef.columnName
      collectionBehavior = nameToBehavior.get(name)
      quotedName = quote(name)
    } yield collectionBehavior match {
        case Some(CollectionAppend)           => s"$quotedName = $quotedName + :$quotedName"
        case Some(CollectionPrepend)          => s"$quotedName = :$quotedName + $quotedName"
        case Some(CollectionRemove)           => s"$quotedName = $quotedName - :$quotedName"
        case Some(CollectionOverwrite) | None => s"$quotedName = :$quotedName"
      }

    def quotedColumnNames(columns: Seq[ColumnDef]) = columns.map(_.columnName).map(quote)
    val setCounterColumnsClause = quotedColumnNames(counterColumns).map(c => s"$c = $c + :$c")
    val setClause = (setNonCounterColumnsClause ++ setCounterColumnsClause).mkString(", ")
    val whereClause = quotedColumnNames(primaryKey).map(c => s"$c = :$c").mkString(" AND ")

    // Counter updates don't support USING TIMESTAMP.
    // For all other UPDATEs, assign per-statement timestamps to ensure that multiple
    // mutations to the same list within a batch get distinct timestamps. Without this,
    // Scylla drops all but one list mutation when they share the same timestamp.
    // See: https://github.com/scylladb/spark-scylladb-connector/issues/26
    val usingClause = if (isCounterUpdate) {
      ""
    } else {
      writeConf.timestamp match {
        case TimestampOption(PerRowWriteOptionValue(placeholder)) => s" USING TIMESTAMP :$placeholder"
        case TimestampOption(StaticWriteOptionValue(value)) => s" USING TIMESTAMP $value"
        case _ => s" USING TIMESTAMP :${TableWriter.AutoTimestampParam}"
      }
    }

    s"UPDATE ${quote(keyspaceName)}.${quote(tableName)}${usingClause} SET $setClause WHERE $whereClause"
  }

  private val isCounterUpdate =
    tableDef.columns.exists(_.isCounterColumn)

  private val containsCollectionBehaviors =
    columnSelector.exists(_.isInstanceOf[CollectionColumnName])

  private[connector] val isIdempotent: Boolean = {
    //All counter operations are not Idempotent
    if (columns.exists(_.isCounterColumn)) {
       false
    } else {
      columnSelector.forall {
        //Any appends or prepends to a list are non-idempotent
        case cn: CollectionColumnName =>
          val name = cn.columnName
          val behavior = cn.collectionBehavior
          val isNotList = !tableDef.columnByName(name).columnType.isInstanceOf[ListType[_]]
          behavior match {
            case CollectionPrepend => isNotList
            case CollectionAppend => isNotList
            case _ => true
          }
       //All other operations on regular columns are idempotent
       case regularColumn: ColumnRef => true
      }
    }
  }

  private def prepareStatement(queryTemplate:String, session: CqlSession): PreparedStatement = {
    try {
      val stmt = SimpleStatement.newInstance(queryTemplate)
        .setIdempotent(isIdempotent)
        .setConsistencyLevel(writeConf.consistencyLevel)
      session.prepare(stmt)
    }
    catch {
      case t: Throwable =>
        throw new IOException(s"Failed to prepare statement $queryTemplate: " + t.getMessage, t)
    }
  }

  def batchRoutingKey(session: CqlSession)(bs: RichBoundStatementWrapper): Any = {
    def missingMetadataException = new Supplier[IllegalArgumentException] {
      override def get(): IllegalArgumentException = new IllegalArgumentException("TokenMap Metadata Missing")
    }

    writeConf.batchGroupingKey match {
      case BatchGroupingKey.None =>  0

      case BatchGroupingKey.ReplicaSet =>
        session.getMetadata.getTokenMap.orElseThrow(missingMetadataException)
          .getReplicas(keyspaceName, QueryUtils.getRoutingKeyOrError(bs.stmt))

      case BatchGroupingKey.Partition => QueryUtils.getRoutingKeyOrError(bs.stmt)
    }
  }

  /**
    * Main entry point
    * if counter or collection column need to be updated Cql UPDATE command will be used
    * INSERT otherwise
    */
  def write(taskContext: TaskContext, data: Iterator[T]): Unit = {
    val asyncStatementWriter = getAsyncWriter()
    writeInternal(asyncStatementWriter, taskContext, data)
  }

  /**
    * Cql DELETE statement
    * @param columns columns to delete, the row will be deleted completely if the list is empty
    * @param taskContext
    * @param data primary key values to select delete rows
    */
  def delete(columns: ColumnSelector) (taskContext: TaskContext, data: Iterator[T]): Unit =
    writeInternal(getAsyncWriterInternal(deleteQueryTemplate(columns)), taskContext, data)

  def extractTokenRange(partitionId: Int): Iterable[CqlTokenRange[_, _]] =
    partitions.lift(partitionId) match {
      case Some(CassandraPartition(_, _, ranges, _)) => ranges
      case _ => List()
    }

  def getAsyncWriter(): AsyncStatementWriter[T] = {
    if (isCounterUpdate || containsCollectionBehaviors) {
      getAsyncWriterInternal(queryTemplateUsingUpdate)
    }
    else {
      getAsyncWriterInternal(queryTemplateUsingInsert)
    }
  }

  private def getAsyncWriterInternal(queryTemplate: String): AsyncStatementWriter[T] = {
    connector.withSessionDo { session =>
      val protocolVersion = session.getContext.getProtocolVersion
      val stmt = prepareStatement(queryTemplate, session)
      val batchType = if (isCounterUpdate) DefaultBatchType.COUNTER else DefaultBatchType.UNLOGGED

      val boundStmtBuilder = new BoundStatementBuilder(
        rowWriter,
        stmt,
        protocolVersion = protocolVersion,
        ignoreNulls = writeConf.ignoreNulls)

      val batchStmtBuilder = new BatchStatementBuilder(batchType, writeConf.consistencyLevel)
      val batchKeyGenerator = batchRoutingKey(session) _
      val batchBuilder = new GroupingBatchBuilderBase(boundStmtBuilder, batchStmtBuilder, batchKeyGenerator,
        writeConf.batchSize, writeConf.batchGroupingBufferSize)

      val maybeRateLimit: RichStatement => Unit = writeConf.throughputMiBPS match {
        case Some(throughput) =>
          val rateLimiter = new RateLimiter(
            (throughput * 1024 * 1024).toLong,
            1024 * 1024)
          (stmt: RichStatement) => rateLimiter.maybeSleep(stmt.bytesCount)
        case None =>
          (stmt: RichStatement) => ()
      }

      AsyncStatementWriter(connector, writeConf, tableDef, stmt, batchBuilder, maybeRateLimit)
    }
  }

  private def writeInternal(asyncStatementWriter: AsyncStatementWriter[T], taskContext: TaskContext, data: Iterator[T]): Unit = {
    val updater = OutputMetricsUpdater(taskContext, writeConf)
    val tokenRanges = extractTokenRange(taskContext.partitionId())
    logInfo(s"Writing ranges: ${tokenRanges}")

    val metricMonitoringWriter = asyncStatementWriter.copy(
        successHandler = Some(updater.batchFinished(success = true, _, _, _)),
        failureHandler = Some(updater.batchFinished(success = false, _, _, _)))

    val rowIterator = new CountingIterator(data)

    logDebug(s"Writing data partition to $keyspaceName.$tableName in batches of ${writeConf.batchSize}.")

    for (stmtToWrite <- rowIterator) {
      metricMonitoringWriter.write(stmtToWrite)
    }

    metricMonitoringWriter.close()

    val duration = updater.finish() / 1000000000d
    logInfo(f"Wrote ${rowIterator.count} rows to $keyspaceName.$tableName in $duration%.3f s.")

    tokenRangeAcc.foreach(_.add(tokenRanges.toSet))
    logInfo("Added token ranges to accumulator")
  }
}

case class AsyncStatementWriter[T](
  connector: CassandraConnector,
  writeConf: WriteConf,
  tableDef: TableDef,
  preparedStatement: PreparedStatement,
  groupingBatchBuilderBase: GroupingBatchBuilderBase[T],
  maybeRateLimit: RichStatement => Unit,
  successHandler: Option[Handler[RichStatement]] = None,
  failureHandler: Option[Handler[RichStatement]] = None)
  extends Closeable
    with Logging {

  //Don't grab a connection or queryExecutor unless we are using this statement writer
  private lazy val session: CqlSession = connector.openSession()
  private val keyspaceName: String = tableDef.keyspaceName
  private val tableName: String = tableDef.tableName

  private lazy val queryExecutor = new QueryExecutor(
    session, writeConf.parallelismLevel, successHandler, failureHandler,
    maxRetries = connector.conf.queryRetryMaxRetries)

  def write(record: T): Unit= {
    groupingBatchBuilderBase.batchRecord(record).foreach{ stmt =>
      queryExecutor.executeAsync(stmt.executeAs(writeConf.executeAs))
      maybeRateLimit(stmt)
    }
  }

  override def close(): Unit = {
    for (statement <- groupingBatchBuilderBase.finish()) {
      queryExecutor.executeAsync(statement.executeAs(writeConf.executeAs))
      maybeRateLimit(statement)
    }

    queryExecutor.waitForCurrentlyExecutingTasks()
    queryExecutor.getLatestException().map {
      case exception =>
        throw new IOException(
          s"""Failed to write statements to $keyspaceName.$tableName. The
             |latest exception was
             |  ${exception.getMessage}
             |
             |Please check the executor logs for more exceptions and information
             """.stripMargin)
    }
    queryExecutor.waitForCurrentlyExecutingTasks()
    session.close()
  }
}

object TableWriter {

  /** Name of the auto-generated timestamp bind parameter added to UPDATE statements. */
  private[writer] val AutoTimestampParam = "autots"

  private def checkMissingColumns(table: TableDef, columnNames: Seq[String]): Unit = {
    val allColumnNames = table.columns.map(_.columnName)
    val missingColumns = columnNames.toSet -- allColumnNames
    if (missingColumns.nonEmpty)
      throw new IllegalArgumentException(
        s"Column(s) not found: ${missingColumns.mkString(", ")}")
  }

  private def checkMissingPrimaryKeyColumns(table: TableDef, columnNames: Seq[String]): Unit = {
    val primaryKeyColumnNames = table.primaryKey.map(_.columnName)
    val missingPrimaryKeyColumns = primaryKeyColumnNames.toSet -- columnNames
    if (missingPrimaryKeyColumns.nonEmpty)
      throw new IllegalArgumentException(
        s"Some primary key columns are missing in RDD or have not been selected: ${missingPrimaryKeyColumns.mkString(", ")}")
  }

  private def checkMissingPartitionKeyColumns(table: TableDef, columnNames: Seq[String]): Unit = {
    val partitionKeyColumnNames = table.partitionKey.map(_.columnName)
    val missingPartitionKeyColumns = partitionKeyColumnNames.toSet -- columnNames
    if (missingPartitionKeyColumns.nonEmpty)
      throw new IllegalArgumentException(
        s"Some partition key columns are missing in RDD or have not been selected: ${missingPartitionKeyColumns.mkString(", ")}")
  }

  private def onlyPartitionKeyAndStatic(table: TableDef, columnNames: Seq[String]): Boolean = {
    val nonPartitionKeyColumnNames = columnNames.toSet -- table.partitionKey.map(_.columnName)
    val nonPartitionKeyColumnRefs = table
      .allColumns
      .filter(columnDef => nonPartitionKeyColumnNames.contains(columnDef.columnName))
    nonPartitionKeyColumnRefs.forall( columnDef => columnDef.columnRole == StaticColumn)
  }

  /**
   * Check whether a collection behavior is being applied to a non collection column
   * Check whether prepend is used on any Sets or Maps
   * Check whether remove is used on Maps
   */
  private def checkCollectionBehaviors(table: TableDef, columnRefs: IndexedSeq[ColumnRef]): Unit = {
    val tableCollectionColumns = table.columns.filter(cd => cd.isCollection)
    val tableCollectionColumnNames = tableCollectionColumns.map(_.columnName)
    val tableListColumnNames = tableCollectionColumns
      .map(c => (c.columnName, c.columnType))
      .collect { case (name, x: ListType[_]) => name }

    val tableMapColumnNames = tableCollectionColumns
      .map(c => (c.columnName, c.columnType))
      .collect { case (name, x: MapType[_, _]) => name }

    val refsWithCollectionBehavior = columnRefs collect {
      case columnName: CollectionColumnName => columnName
    }

    val collectionBehaviorColumnNames = refsWithCollectionBehavior.map(_.columnName)

    //Check for non-collection columns with a collection Behavior
    val collectionBehaviorNormalColumn =
      collectionBehaviorColumnNames.toSet -- tableCollectionColumnNames.toSet

    if (collectionBehaviorNormalColumn.nonEmpty)
      throw new IllegalArgumentException(
        s"""Collection behaviors (add/remove/append/prepend) are only allowed on collection columns.
           |Normal Columns with illegal behavior: ${collectionBehaviorNormalColumn.mkString}"""
          .stripMargin
      )

    //Check that prepend is used only on lists
    val prependBehaviorColumnNames = refsWithCollectionBehavior
      .filter(_.collectionBehavior == CollectionPrepend)
      .map(_.columnName)
    val prependOnNonList = prependBehaviorColumnNames.toSet -- tableListColumnNames.toSet

    if (prependOnNonList.nonEmpty)
      throw new IllegalArgumentException(
        s"""The prepend collection behavior only applies to Lists. Prepend used on:
           |${prependOnNonList.mkString}""".stripMargin
      )

    //Check that remove is not used on Maps

    val removeBehaviorColumnNames = refsWithCollectionBehavior
      .filter(_.collectionBehavior == CollectionRemove)
      .map(_.columnName)

    val removeOnMap = removeBehaviorColumnNames.toSet & tableMapColumnNames.toSet

    if (removeOnMap.nonEmpty)
      throw new IllegalArgumentException(
        s"The remove operation is currently not supported for Maps. Remove used on: ${removeOnMap
          .mkString}"
      )
  }

  private def checkColumns(table: TableDef, columnRefs: IndexedSeq[ColumnRef], checkPartitionKey: Boolean) = {
    val columnNames = columnRefs.map(_.columnName)
    checkMissingColumns(table, columnNames)
    if (checkPartitionKey) {
      // For Deletes we only need a partition Key for a valid delete statement
      checkMissingPartitionKeyColumns(table, columnNames)
    }
    else if (onlyPartitionKeyAndStatic(table, columnNames)) {
      // Cassandra only requires a Partition Key Column on insert if all other columns are Static
      checkMissingPartitionKeyColumns(table, columnNames)
    }
    else {
      // For all other normal Cassandra writes we require the full primary key to be present
      checkMissingPrimaryKeyColumns(table, columnNames)
    }
    checkCollectionBehaviors(table, columnRefs)
  }

  /** Columns that cannot actually be written to because they representt virtual endpoints
    */
  private val InternalColumns = Set("solr_query")

  def apply[T : RowWriterFactory](
      connector: CassandraConnector,
      keyspaceName: String,
      tableName: String,
      columnNames: ColumnSelector,
      writeConf: WriteConf,
      checkPartitionKey: Boolean = false,
      partitions: Array[Partition] = Array(),
      tokenRangeAcc: Option[TokenRangeAccumulator] = None): TableWriter[T] = {

    val tableDef = tableFromCassandra(connector, keyspaceName, tableName)
    TableWriter(connector, tableDef, columnNames, writeConf, checkPartitionKey, partitions, tokenRangeAcc)
  }

  def apply[T : RowWriterFactory](
       connector: CassandraConnector,
       tableDef: TableDef,
       columnNames: ColumnSelector,
       writeConf: WriteConf,
       checkPartitionKey: Boolean,
       partitions: Array[Partition],
       tokenRangeAcc: Option[TokenRangeAccumulator]): TableWriter[T] = {

    val optionColumns = writeConf.optionsAsColumns(tableDef.keyspaceName, tableDef.tableName)
    val tablDefWithMeta = tableDef.copy(regularColumns = tableDef.regularColumns ++ optionColumns)

    val selectedColumns = columnNames
      .selectFrom(tablDefWithMeta)
      .filter(col => !InternalColumns.contains(col.columnName))
    val rowWriter = implicitly[RowWriterFactory[T]].rowWriter(tablDefWithMeta, selectedColumns)

    checkColumns(tablDefWithMeta, selectedColumns, checkPartitionKey)
    new TableWriter[T](connector, tablDefWithMeta, selectedColumns, rowWriter, writeConf, partitions, tokenRangeAcc)
  }
}
