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

import java.net.InetAddress
import java.util.UUID
import com.datastax.spark.connector.cql.{CassandraConnector, ColumnDef, TableDef}
import com.datastax.spark.connector.datasource.CassandraSourceUtil.consolidateConfs
import com.datastax.spark.connector.datasource.ScanHelper.CqlQueryParts
import com.datastax.spark.connector.rdd.{CqlWhereClause, ReadConf}
import com.datastax.spark.connector.types.{InetType, UUIDType, VarIntType}
import com.datastax.spark.connector.util.Quote.quote
import com.datastax.spark.connector.util.{Logging, ReflectionUtil}
import com.datastax.spark.connector.{ColumnRef, RowCountRef, TTL, WriteTime}
import org.apache.spark.SparkConf
import org.apache.spark.sql.cassandra.CassandraSourceRelation.{AdditionalCassandraPushDownRulesParam, InClauseToJoinWithTableConversionThreshold}
import org.apache.spark.sql.cassandra.{AnalyzedPredicates, Auto, BasicCassandraPredicatePushDown, CassandraPredicateRules, CassandraSourceRelation, DsePredicateRules, DseSearchOptimizationSetting, InClausePredicateRules, Off, On, SolrConstants, SolrPredicateRules, TimeUUIDPredicateRules}
import org.apache.spark.sql.connector.expressions.{Expression, Expressions}
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.read.partitioning.{KeyGroupedPartitioning, Partitioning}
import org.apache.spark.sql.sources.{EqualTo, Filter, In}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.{SparkSession, sources}
import org.apache.spark.unsafe.types.UTF8String

import scala.jdk.CollectionConverters._

case class CassandraScanBuilder(
  session: SparkSession,
  tableDef: TableDef,
  catalogName: String,
  options: CaseInsensitiveStringMap)

  extends ScanBuilder
    with SupportsPushDownFilters
    with SupportsPushDownRequiredColumns
    with Logging {

  val consolidatedConf = consolidateConfs(session.sparkContext.getConf, session.conf.getAll, catalogName, tableDef.keyspaceName, options.asScala.toMap)
  val readConf = ReadConf.fromSparkConf(consolidatedConf)

  private val connector = CassandraConnector(consolidatedConf)
  private val tableIsSolrIndexed =
    tableDef
      .indexes
      .exists(index => index.className.contains(SolrConstants.DseSolrIndexClassName))

  //Metadata Read Fields
  // ignore case
  private val regularColumnNames = tableDef.regularColumns.map(_.columnName.toLowerCase())
  private val nonRegularColumnNames = (tableDef.clusteringColumns ++ tableDef.partitionKey).map(_.columnName.toLowerCase)
  private val ignoreMissingMetadataColumns: Boolean = consolidatedConf.getBoolean(CassandraSourceRelation.IgnoreMissingMetaColumns.name,
    CassandraSourceRelation.IgnoreMissingMetaColumns.default)

  private val pushdownEnabled = consolidatedConf.getOption("pushdown").getOrElse("true").toBoolean
  private var filtersForCassandra = Array.empty[Filter]
  private var filtersForSpark = Array.empty[Filter]
  private var selectedColumns: IndexedSeq[ColumnRef] = tableDef.columns.map(_.ref)
  private var readSchema: StructType = _

  override def pushFilters(filters: Array[Filter]): Array[Filter] = if (!pushdownEnabled) {
    filters
  } else {
    logDebug(s"Input Predicates: [${filters.mkString(", ")}]")

    val pv = connector.withSessionDo(_.getContext.getProtocolVersion)

    /** Apply built in rules **/
    val bcpp = new BasicCassandraPredicatePushDown(filters.toSet, tableDef, pv)
    val basicPushdown = AnalyzedPredicates(bcpp.predicatesToPushDown, bcpp.predicatesToPreserve)

    logDebug(s"Basic Rules Applied:\n$basicPushdown")

    val predicatePushDownRules = Seq(
      DsePredicateRules,
      InClausePredicateRules) ++
      solrPredicateRules ++
      additionalRules()  :+
      TimeUUIDPredicateRules

    /** Apply non-basic rules **/
    val finalPushdown = predicatePushDownRules.foldLeft(basicPushdown)(
      (pushdowns, rules) => {
        val pd = rules(pushdowns, tableDef, consolidatedConf)
        logDebug(s"Applied ${rules.getClass.getSimpleName} Pushdown Filters:\n$pd")
        pd
      }
    )
    logDebug(s"Final Pushdown filters:\n$finalPushdown")

    filtersForCassandra = finalPushdown.handledByCassandra.toArray
    filtersForSpark = finalPushdown.handledBySpark.toArray

    filtersForSpark
  }

  def additionalRules(): Seq[CassandraPredicateRules] = {
    consolidatedConf.getOption(AdditionalCassandraPushDownRulesParam.name)
    match {
      case Some(classes) =>
        classes
          .trim
          .split("""\s*,\s*""")
          .map(ReflectionUtil.findGlobalObject[CassandraPredicateRules]).toIndexedSeq
      case None => AdditionalCassandraPushDownRulesParam.default
    }
  }

  private def solrPredicateRules: Option[CassandraPredicateRules] = {
    if (searchOptimization().enabled) {
      logDebug(s"Search Optimization Enabled - ${searchOptimization()}")
      Some(new SolrPredicateRules(searchOptimization()))
    } else {
      None
    }
  }

  private def searchOptimization(): DseSearchOptimizationSetting =
    consolidatedConf.get(
      CassandraSourceRelation.SearchPredicateOptimizationParam.name,
      CassandraSourceRelation.SearchPredicateOptimizationParam.default
    ).toLowerCase match {
      case "auto" => Auto(consolidatedConf.getDouble(
        CassandraSourceRelation.SearchPredicateOptimizationRatioParam.name,
        CassandraSourceRelation.SearchPredicateOptimizationRatioParam.default))
      case "on" | "true" => On
      case "off" | "false" => Off
      case unknown => throw new IllegalArgumentException(
        s"""
           |Attempted to set ${CassandraSourceRelation.SearchPredicateOptimizationParam.name} to
           |$unknown which is invalid. Acceptable values are: auto, on, and off
           """.stripMargin)
    }


  val TTLCapture = "TTL\\((.*)\\)".r
  val WriteTimeCapture = "WRITETIME\\((.*)\\)".r

  override def pruneColumns(requiredSchema: StructType): Unit = {
    selectedColumns = requiredSchema.fieldNames.collect {
      case name@TTLCapture(column) => TTL(column, Some(name))
      case name@WriteTimeCapture(column) => WriteTime(column, Some(name))
      case column => tableDef.columnByName(column).ref
    }.toIndexedSeq
    readSchema = requiredSchema
  }

  override def build(): Scan = {
    val currentPushdown = AnalyzedPredicates(filtersForCassandra.toSet, filtersForSpark.toSet)
    if (isConvertableToJoinWithCassandra(currentPushdown)) {
      logInfo(
        s"""Number of keys in 'IN' clauses exceeds ${InClauseToJoinWithTableConversionThreshold.name},
           |converting to joinWithCassandraTable.""".stripMargin)
      //Remove all Primary Join Restricted Filters
      val primaryKeyFilters = eqAndInColumnFilters(tableDef.primaryKey, currentPushdown)
      filtersForCassandra = (filtersForCassandra.toSet -- primaryKeyFilters).toArray

      //Reframe all primary key restrictions as IN
      val inClauses = primaryKeyFilters.collect {
        case EqualTo(attribute, value) => In(attribute, Array(value))
        case in: In => in
        case other =>
          throw new IllegalAccessException(
            s"""In Clause to Join Conversion Failed,
               |Illegal predicate on primary key $other""".stripMargin)
      }

      CassandraInJoin(session, connector, tableDef, inClauses, getQueryParts(), readSchema, readConf, consolidatedConf)
    } else {
      CassandraScan(session, connector, tableDef, getQueryParts(), readSchema, readConf, consolidatedConf)
    }
  }

  private def getQueryParts(): CqlQueryParts = {
    //Get all required ColumnRefs, MetaDataRefs should be picked out of the ReadColumnsMap
    val requiredCassandraColumns = selectedColumns

    val solrCountEnabled = searchOptimization().enabled && tableIsSolrIndexed && cqlWhereClause.predicates.isEmpty
    val solrCountWhere = CqlWhereClause(Seq(s"${SolrConstants.SolrQuery} = '*:*'"), Seq.empty)

    if (requiredCassandraColumns.isEmpty) {
      //Count Pushdown
      CqlQueryParts(IndexedSeq(RowCountRef),
        if (solrCountEnabled) cqlWhereClause and solrCountWhere else cqlWhereClause,
        None,
        None)

    } else {
      //No Count Pushdown
      CqlQueryParts(requiredCassandraColumns, cqlWhereClause, None, None)
    }
  }

  override def pushedFilters(): Array[Filter] = filtersForCassandra

  /** Construct where clause from pushdown filters */
  private def cqlWhereClause = CassandraScanBuilder.filterToCqlWhereClause(tableDef, filtersForCassandra)

  /** Is convertable to joinWithCassandraTable if query
    * - uses all partition key columns
    * - spans multiple partitions
    * - contains IN key values and the cartesian set of those values is greater than threshold
    */
  private def isConvertableToJoinWithCassandra(predicates: AnalyzedPredicates): Boolean = {
    val inClauseConversionThreshold = consolidatedConf.getLong(InClauseToJoinWithTableConversionThreshold.name, InClauseToJoinWithTableConversionThreshold.default)
    if (inClauseConversionThreshold == 0L || !pushdownEnabled) {
      false
    } else {
      val partitionFilters = eqAndInColumnFilters(tableDef.partitionKey, predicates)
      val clusteringFilters = eqAndInColumnFilters(tableDef.clusteringColumns, predicates)
      val inClauseValuesCartesianSize = (partitionFilters ++ clusteringFilters).foldLeft(1L) {
        case (cartSize, In(_, values)) => cartSize * values.length
        case (cartSize, _) => cartSize
      }
      partitionFilters.exists(_.isInstanceOf[In]) &&
        tableDef.partitionKey.length == partitionFilters.length &&
        inClauseValuesCartesianSize >= inClauseConversionThreshold
    }
  }

  /** Preserves `columns` order */
  private def eqAndInColumnFilters(columns: Seq[ColumnDef], predicates: AnalyzedPredicates): Seq[Filter] = {
    val predicatesByColumnName = (predicates.handledByCassandra ++ predicates.handledBySpark).collect {
      case eq@EqualTo(column, _) => (column, eq)
      case in@In(column, _) => (column, in)
    }.toMap
    columns.flatMap(column => predicatesByColumnName.get(column.columnName))
  }
}

object CassandraScanBuilder {
  private[connector] def filterToCqlWhereClause(tableDef: TableDef, filters: Array[Filter]): CqlWhereClause = {
    filters.foldLeft(CqlWhereClause.empty) { case (where, filter) =>
      val (predicate, values) = filterToCqlAndValue(tableDef, filter)
      val newClause = CqlWhereClause(Seq(predicate), values)
      where and newClause
    }
  }

  /** Construct Cql clause and retrieve the values from filter */
  private def filterToCqlAndValue(tableDef: TableDef, filter: Any): (String, Seq[Any]) = {
    filter match {
      case sources.EqualTo(attribute, value) => (s"${quote(attribute)} = ?", Seq(toCqlValue(tableDef, attribute, value)))
      case sources.LessThan(attribute, value) => (s"${quote(attribute)} < ?", Seq(toCqlValue(tableDef, attribute, value)))
      case sources.LessThanOrEqual(attribute, value) => (s"${quote(attribute)} <= ?", Seq(toCqlValue(tableDef, attribute, value)))
      case sources.GreaterThan(attribute, value) => (s"${quote(attribute)} > ?", Seq(toCqlValue(tableDef, attribute, value)))
      case sources.GreaterThanOrEqual(attribute, value) => (s"${quote(attribute)} >= ?", Seq(toCqlValue(tableDef, attribute, value)))
      case sources.In(attribute, values) =>
        (quote(attribute) + " IN " + values.map(_ => "?").mkString("(", ", ", ")"), toCqlValues(tableDef, attribute, values))
      case _ =>
        throw new UnsupportedOperationException(
          s"It's not a valid filter $filter to be pushed down, only >, <, >=, <= and In are allowed.")
    }
  }

  private def toCqlValues(tableDef: TableDef, columnName: String, values: Array[Any]): Seq[Any] = {
    values.map(toCqlValue(tableDef, columnName, _)).toSeq
  }

  /** If column is VarInt column, convert data to BigInteger */
  private def toCqlValue(tableDef: TableDef, columnName: String, value: Any): Any = {
    value match {
      case decimal: Decimal =>
        val isVarIntColumn = tableDef.columnByName(columnName).columnType == VarIntType
        if (isVarIntColumn) decimal.toJavaBigDecimal.toBigInteger else decimal
      case utf8String: UTF8String =>
        val columnType = tableDef.columnByName(columnName).columnType
        if (columnType == InetType) {
          InetAddress.getByName(utf8String.toString)
        } else if (columnType == UUIDType) {
          UUID.fromString(utf8String.toString)
        } else {
          utf8String
        }
      case other => other
    }
  }
}

case class CassandraScan(
  session: SparkSession,
  connector: CassandraConnector,
  tableDef: TableDef,
  cqlQueryParts: CqlQueryParts,
  readSchema: StructType,
  readConf: ReadConf,
  consolidatedConf: SparkConf) extends Scan
  with Batch
  with SupportsReportPartitioning {


  private lazy val inputPartitions = partitionGenerator.getInputPartitions()
  private val partitionGenerator = ScanHelper.getPartitionGenerator(
    connector,
    tableDef,
    cqlQueryParts.whereClause,
    session.sparkContext.defaultParallelism * 2 + 1,
    readConf.splitCount,
    readConf.splitSizeInMB * 1024L * 1024L)

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    inputPartitions
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    CassandraScanPartitionReaderFactory(connector, tableDef, readSchema, readConf, cqlQueryParts)
  }

  override def outputPartitioning(): Partitioning = {
   new CassandraPartitioning(tableDef.partitionKey.map(_.columnName).map(Expressions.identity).toArray, inputPartitions.length)
  }

  override def description(): String = {
    s"""Cassandra Scan: ${tableDef.keyspaceName}.${tableDef.tableName}
       | - Cassandra Filters: ${cqlQueryParts.whereClause}
       | - Requested Columns: ${cqlQueryParts.selectedColumnRefs.mkString("[", ",", "]")}""".stripMargin
  }
}

class CassandraPartitioning(keys: Array[Expression], numPartitions: Int) extends KeyGroupedPartitioning(keys, numPartitions) {
}

case class CassandraInJoin(
  session: SparkSession,
  connector: CassandraConnector,
  tableDef: TableDef,
  inClauses: Seq[In],
  cqlQueryParts: CqlQueryParts,
  readSchema: StructType,
  readConf: ReadConf,
  consolidatedConf: SparkConf) extends Scan
  with Batch
  with SupportsReportPartitioning {

  private val numPartitions = readConf.splitCount.getOrElse(session.sparkContext.defaultParallelism)

  override def toBatch() = this

  override def planInputPartitions(): Array[InputPartition] = {
    val partitions = for (i <- 0 until numPartitions) yield {
      NumberedInputPartition(i, numPartitions)
    }
    partitions.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    CassandraInJoinReaderFactory(connector, tableDef, inClauses, readConf, readSchema, cqlQueryParts)

  }

  override def outputPartitioning(): Partitioning = {
    new CassandraPartitioning(tableDef.partitionKey.map(_.columnName).map(Expressions.identity).toArray, numPartitions)
  }
}

case class NumberedInputPartition(index: Int, total: Int) extends InputPartition

