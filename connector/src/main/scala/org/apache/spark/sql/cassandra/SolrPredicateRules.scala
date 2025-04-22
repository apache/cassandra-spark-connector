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

import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

import org.apache.commons.lang3.StringEscapeUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.cassandra.SolrConstants._
import org.apache.spark.sql.sources._

import com.datastax.dse.driver.api.core.metadata.DseNodeProperties
import com.datastax.oss.driver.api.core.config.DefaultDriverOption
import com.datastax.oss.driver.api.core.cql.SimpleStatement
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance
import com.datastax.spark.connector.cql.{CassandraConnector, TableDef}
import com.datastax.spark.connector.util.Logging

class SolrPredicateRules(searchOptimizationEnabled: DseSearchOptimizationSetting)
  extends CassandraPredicateRules
    with Logging {

  /**
    Constructor for testing, Takes solrIndexedFields as a function. This allows us
    to avoid reading from Solr.xml in testing functions.

    We assume that the basic Cassandra Rules have been applied first. Do nothing if there is a full
    partition_key restriction or primary key restriction. Otherwise try the solr conversion
  */
  private[cassandra] def apply(
    predicates: AnalyzedPredicates,
    tableDef: TableDef,
    sparkConf: SparkConf,
    getSolrIndexedColumns: (TableDef, SparkConf) => Set[String]): AnalyzedPredicates = {

    //This could be done in the SCC as it's not Solr Specific
    val uselessIsNotNulls =
      findUselessIsNotNulls(predicates.handledByCassandra ++ predicates.handledBySpark, tableDef)
    val usefulPredicates =
      AnalyzedPredicates(predicates.handledByCassandra, predicates.handledBySpark -- uselessIsNotNulls)

    val pkRestriction = getPartitionKeyRestriction(usefulPredicates, tableDef).asInstanceOf[Set[Filter]]

    val primaryKeyRestrictionExists =
      pkRestriction.nonEmpty &&
      pkRestriction.subsetOf(usefulPredicates.handledByCassandra) &&
      usefulPredicates.handledBySpark.isEmpty

    val solrEnabledOnTargetHosts = CassandraConnector(sparkConf)
      .withSessionDo{ session =>
        val hosts = session.getMetadata.getNodes.values().asScala
        val possibleHosts = hosts.filter(host => host.getDistance != NodeDistance.IGNORED)
        possibleHosts.forall { host =>
          val workloads = Option(host.getExtras.get(DseNodeProperties.DSE_WORKLOADS))
          workloads.exists(_.asInstanceOf[java.util.Set[String]].contains("Search"))
        }
      }
    val failedRequirement = Seq[(Boolean, String)](
      (!solrEnabledOnTargetHosts, "Search is not enabled on DSE Target nodes."),
      (!searchOptimizationEnabled.enabled, "Automatic Search optimizations for Spark SQL are disabled."),
      (primaryKeyRestrictionExists, "There is a primary key restriction present"),
      (alreadyContainsSolrQuery(usefulPredicates), "Manual Solr query (solr_query = xxx) present.")
    ).collectFirst{ case (true, reason) => reason}
    failedRequirement match {
      case Some(reasonForFailure) =>
        logDebug(s"Not using Solr Optimizations. $reasonForFailure")
        usefulPredicates
      case None =>
        convertToSolrQuery(
          usefulPredicates,
          tableDef,
          getSolrIndexedColumns(tableDef, sparkConf),
          searchOptimizationEnabled,
          sparkConf)
    }
  }

  /**
    * Entry point for Spark Cassandra Connector. Reads SolrIndexedColumn information from
    * C*. See above Apply method for actual implementation.
    */
  override def apply(
    predicates: AnalyzedPredicates,
    tableDef: TableDef,
    sparkConf: SparkConf): AnalyzedPredicates = {
    apply(predicates, tableDef, sparkConf, getSolrIndexedColumnsFromSolrXML)
  }

  /**
    * Unfortunately the easiest current way to remotely determine what
    * columns have been indexed by solr is to read the Schema.xml. To obtain
    * this we check the Cassandra solr_admin table and pull the text from
    * the schema.xml.bak.
    *
    * Schema.xml.bak is the current "live" schema while schema.xml is the schema that
    * will be applied on refresh.
    */
  def getSolrIndexedColumnsFromSolrXML(tableDef: TableDef, sparkConf: SparkConf): Set[String] = {
    def solrIndexedFail(str: String): Set[String] = {
      logDebug(s"""Retrieval of Solr Index Info Failed: $str""")
      Set.empty
    }

    val SelectSolrSchema =
      s"""SELECT blobAsText(resource_value) FROM
         |solr_admin.solr_resources
         |where core_name = '${tableDef.keyspaceName}.${tableDef.tableName}'
         |and resource_name = 'schema.xml.bak' """.stripMargin

    Try {
      CassandraConnector(sparkConf)
        .withSessionDo(_.execute(SelectSolrSchema))
        .one()
        .getString(0)
    } match {
      case Success(schema: String) => Try {
        val schemaXML = scala.xml.XML.loadString(schema)

        schemaXML
          .child
          .filter(_.label == "fields")
          .head
          .child
          .filter(x => x.label == "field")
            //multiValued indexes are for graph internal use only
          .filter(!_.attributes.asAttrMap.getOrElse("multiValued", "false").toBoolean)
          .filter(_.attributes.asAttrMap.getOrElse("indexed", "false").toBoolean)
          .map(_.attribute("name").get.toString)
          .toSet[String]
      } match {
        case Success(indexed) => indexed
        case Failure(e) => solrIndexedFail(s"Unable to parse schema.xml.bak: $e")
      }
      case Failure(e) => solrIndexedFail(s"Unable to access Solr Info: $e")
    }
  }


  def alreadyContainsSolrQuery(predicates: AnalyzedPredicates): Boolean = {
    (predicates.handledByCassandra ++ predicates.handledBySpark).collect {
      case EqualTo(column, value) if column == SolrQuery => EqualTo(column, value)
    }.nonEmpty
  }

  /**
    * Checks that the filter and all dependent filters are indexed by
    * SolrIndexes.
    */
  def isConvertibleToSolr(filter: Filter, indexedCols: Set[String]): Boolean = filter match {
    case EqualTo(attr: String, value: Any) => indexedCols.contains(sanitizeSolrField(attr))
    case EqualNullSafe(attr: String, value: Any) => indexedCols.contains(sanitizeSolrField(attr))
    case In(attr: String, values: Array[Any]) => indexedCols.contains(sanitizeSolrField(attr))

    //Range Queries
    case GreaterThan(attr: String, value: Any) => indexedCols.contains(sanitizeSolrField(attr))
    case GreaterThanOrEqual(attr: String, value: Any) => indexedCols.contains(sanitizeSolrField(attr))
    case LessThan(attr: String, value: Any) => indexedCols.contains(sanitizeSolrField(attr))
    case LessThanOrEqual(attr: String, value: Any) => indexedCols.contains(sanitizeSolrField(attr))

    //Null Checks
    case IsNull(attr: String) => indexedCols.contains(sanitizeSolrField(attr))
    case IsNotNull(attr: String) => indexedCols.contains(sanitizeSolrField(attr))

    //Conjunctions
    case And(left: Filter, right: Filter) =>
      isConvertibleToSolr(left, indexedCols) && isConvertibleToSolr(right, indexedCols)
    case Or(left: Filter, right: Filter) =>
      isConvertibleToSolr(left, indexedCols) && isConvertibleToSolr(right, indexedCols)
    case Not(child: Filter) => isConvertibleToSolr(child, indexedCols)

    //StringMatching
    case StringStartsWith(attr: String, value: String) => indexedCols.contains(sanitizeSolrField(attr))
    case StringEndsWith(attr: String, value: String) => indexedCols.contains(sanitizeSolrField(attr))
    case StringContains(attr: String, value: String) => indexedCols.contains(sanitizeSolrField(attr))

    //Unknown
    case unknownFilter =>
      logError(s"Unknown Filter Type $unknownFilter")
      false
  }

  case class SolrFilter(solrQuery: String, references: Array[String])

  /** Sometimes the Java String Representation of the Value is not what solr is expecting
    * so we need to do conversions. Additionally we need to encode that encoded string
    * for JSON so we can pass it through to Solr.
    *
    * Solr JSON query is defined as
    * {{{
    * ... WHERE solr_query='{ "q": "field:value"}'
    * }}}
    * this method converts the values.
    */
  private def toSolrValue(value: Any): String = StringEscapeUtils.escapeJson(
    escapeSolrCondition(
      value match {
        case date: java.sql.Timestamp => DateTimeFormatter.ISO_INSTANT.format(date.toInstant)
        case default => default.toString
      }
    )
  )

  /** Similarly to [[toSolrValue]] converts Solr fields to Solr-consumable format.
    *
    * Solr JSON query is defined as
    * {{{
    * ... WHERE solr_query='{ "q": "field:value"}'
    * }}}
    * this method converts the fields. */
  private def toSolrField(field: String): String = toSolrValue(sanitizeSolrField(field))

  /** Spark attributes containing special characters may be enclosed with backticks (`), this could break
    * matching with index column names. */
  private def sanitizeSolrField(field: String) = {
    field match {
      case quoted: String if quoted.startsWith("`") && quoted.endsWith("`") && quoted.length > 1 => quoted.substring(1, quoted.length - 1)
      case str => str
    }
  }

  def convertToSolrFilter(filter: Filter): SolrFilter = filter match {
    //Equivalence queries
    case EqualTo(attr: String, value: Any) => SolrFilter(s"${toSolrField(attr)}:${toSolrValue(value)}", filter.references)
    case EqualNullSafe(attr: String, value: Any) => SolrFilter(s"${toSolrField(attr)}:${toSolrValue(value)}", filter.references)
    case In(attr: String, values: Array[Any]) =>
      SolrFilter(s"${toSolrField(attr)}:(${values.map(toSolrValue).mkString(" ")})", filter.references)

    //Range Queries
    case GreaterThan(attr: String, value: Any) => SolrFilter(s"${toSolrField(attr)}:{${toSolrValue(value)} TO *]", filter.references)
    case GreaterThanOrEqual(attr: String, value: Any) =>
      SolrFilter(s"${toSolrField(attr)}:[${toSolrValue(value)} TO *]", filter.references)

    case LessThan(attr: String, value: Any) => SolrFilter(s"${toSolrField(attr)}:[* TO ${toSolrValue(value)}}", filter.references)
    case LessThanOrEqual(attr: String, value: Any) =>
      SolrFilter(s"${toSolrField(attr)}:[* TO ${toSolrValue(value)}]", filter.references)

    //Null Checks
    case IsNull(attr: String) => SolrFilter(s"-${toSolrField(attr)}:[* TO *]", filter.references)
    case IsNotNull(attr: String) => SolrFilter(s"${toSolrField(attr)}:*", filter.references)

    //Conjunctions
    case And(left: Filter, right: Filter) =>
      SolrFilter(s"""(${convertToSolrFilter(left).solrQuery} AND ${convertToSolrFilter(right).solrQuery})""", filter.references)
    case Or(left: Filter, right: Filter) =>
      SolrFilter(s"""(${convertToSolrFilter(left).solrQuery} OR ${convertToSolrFilter(right).solrQuery})""", filter.references)
    case Not(child: Filter) =>
      SolrFilter(s"""-(${convertToSolrFilter(child).solrQuery})""", filter.references)

    //StringMatching
    case StringStartsWith(attr: String, value: String) => SolrFilter(s"${toSolrField(attr)}:${toSolrValue(value)}*", filter.references)
    case StringEndsWith(attr: String, value: String) => SolrFilter(s"${toSolrField(attr)}:*${toSolrValue(value)}", filter.references)
    case StringContains(attr: String, value: String) => SolrFilter(s"${toSolrField(attr)}:*${toSolrValue(value)}*", filter.references)

    //Unknown
    case unknown =>
      throw new IllegalArgumentException(s"$unknown cannot be converted")
  }

  /**
    * Returns all predicates that can be treated as a single partition restriction.
    *
    * Follows the same rules as in SCC Basic Cassandra Predicates
    *
    * If no single partition restriction can be found returns nothing.
    */
  def getPartitionKeyRestriction(
    predicates: AnalyzedPredicates,
    tableDef: TableDef): Set[EqualTo] = {

    val equalsRestrictions =
      predicates
        .handledByCassandra
        .collect{ case equals: EqualTo => equals}

    val partitionKeyColumnNames = tableDef.partitionKey.map(_.columnName).toSet

    val partitionKeyRestrictions =
      equalsRestrictions.filter{ case EqualTo(attr, _) => partitionKeyColumnNames.contains(attr)}

    val partitionKeyFullyRestricted =
      (partitionKeyColumnNames -- partitionKeyRestrictions.map(_.attribute)).isEmpty

    if (partitionKeyFullyRestricted)
      partitionKeyRestrictions
    else
      Set.empty[EqualTo]
  }

  /**
    * Whenever we have an attribute filter we don't need to do an IS_NOT_NULL check. This
    * also helps when we remove partition key restrictions because we don't keep useless
    * IsNotNulls which generate bad Solr.
    *
    */
  def findUselessIsNotNulls(filters: Set[Filter], tableDef: TableDef): Set[IsNotNull] = {
    val primaryKeyColumnNames = tableDef.primaryKey.map(_.columnName)

    val isNotNullColumns = filters.collect{ case IsNotNull(attr) => attr -> IsNotNull(attr) }.toMap

    val restrictedColumns = filters.collect {
      case LessThan(attr, _) => attr
      case LessThanOrEqual(attr, _) => attr
      case GreaterThan(attr, _) => attr
      case GreaterThanOrEqual(attr, _) => attr
      case EqualNullSafe(attr, _) => attr
      case EqualTo(attr, _) => attr
    }

    val uselessIsNotNulls = (primaryKeyColumnNames ++ restrictedColumns)
      .flatMap( attr => isNotNullColumns.get(attr))
      .toSet
    logDebug(s"Found isNotNulls $uselessIsNotNulls that are already not needed")

    uselessIsNotNulls
  }

  /**
    *
    * For all top level filters. If the filter can be changed into a SolrQuery
    * we will convert it and mark it as handled by Cassandra. All other filters will be
    * filtered within Spark
    *
    */
  def convertToSolrQuery(
    predicates: AnalyzedPredicates,
    tableDef: TableDef,
    solrIndexedFields: Set[String],
    searchOptimizationEnabled: DseSearchOptimizationSetting,
    sparkConf: SparkConf): AnalyzedPredicates = {

    val allPredicates = predicates.handledByCassandra ++ predicates.handledBySpark

    val pkRestriction = getPartitionKeyRestriction(predicates, tableDef)
    if (pkRestriction.nonEmpty)
      logDebug(s"Partition restriction being withheld from Solr Conversion:  $pkRestriction")

    val possibleSolrPredicates = allPredicates -- pkRestriction

    val (solrConvertibleFilters, sparkFilters) = possibleSolrPredicates
      .partition(isConvertibleToSolr(_, solrIndexedFields))

    logDebug(s"Converting $solrConvertibleFilters to Solr Predicates")
    val solrFilters = solrConvertibleFilters.map(convertToSolrFilter)

    // Recommendation from Caleb :
    // Using separate filters ["filter","filter"] allows for reuse of filters
    val combinedFilterQuery = solrFilters
      .map { case SolrFilter(query, references) => s""""$query"""" }
      .mkString(", ")

    val solrString =  s"""{"q":"*:*", "fq":[$combinedFilterQuery]}"""

    /*
    See https://docs.datastax.com/en/datastax_enterprise/4.8/datastax_enterprise/srch/srchJSON.html#srchJSON__distQueryShardTol
    By setting these parameters for our estimate queries we will be more tolerant to partial results and shards not responding
    Which is ok because we are just trying to get an estimate
    */
    val FaultTolerant = Seq("\"shards.failover\": false", "\"shards.tolerant\": true").mkString(",")
    val solrStringNoFailoverTolerant = s"""{"q":"*:*", "fq":[$combinedFilterQuery], $FaultTolerant}"""

    val combinedSolrFilter: Filter = EqualTo(SolrQuery, solrString)

    val optimizedPredicates = AnalyzedPredicates(Set(combinedSolrFilter) ++ pkRestriction, sparkFilters)

    if (solrConvertibleFilters.isEmpty) {
      logDebug("No Solr Convertible Filters Found")
      predicates
    } else {
      searchOptimizationEnabled match {
        case Auto(ratio) =>
          val conn = CassandraConnector(sparkConf)
          val request = s"""SELECT COUNT(*) from "${tableDef.keyspaceName}"."${tableDef.tableName}" where solr_query=?"""

          logDebug(s"Checking total number of records")
          val (totalRecords:Long, queryRecords:Long) = conn.withSessionDo{ session =>
            //Disable Paging for the count requests since we are fault tolerant and paging cannot
            // be used during a fault tolerant request
            // https://docs.datastax.com/en/drivers/java/2.2/com/datastax/driver/core/Statement.html#setFetchSize-int-
            val pagingDisabled = session.getContext.getConfig.getDefaultProfile.withInt(DefaultDriverOption.REQUEST_PAGE_SIZE, -1)
            val totalRequest = SimpleStatement.newInstance(request, s"""{"q":"*:*", $FaultTolerant}""")
              .setExecutionProfile(pagingDisabled)
            val queryRequest = SimpleStatement.newInstance(request, solrStringNoFailoverTolerant)
              .setExecutionProfile(pagingDisabled)

            val totalFuture = session.executeAsync(totalRequest)
            val queryFuture = session.executeAsync(queryRequest)//TODO THIS can be done in a more reactive way I believe
            (totalFuture.toCompletableFuture.get(5, TimeUnit.SECONDS).one().getLong(0),
              queryFuture.toCompletableFuture.get(5, TimeUnit.SECONDS).one().getLong(0))
          }

          val queryRatio = if (totalRecords == 0) 0 else queryRecords.toDouble / totalRecords.toDouble
          if (queryRatio > ratio) {
            logDebug(s"Requesting $queryRatio of the total records. Required to be less than $ratio for DSE Search, falling back to Full Table Scan")
            predicates
          }
          else {
            logDebug(s"Requesting $queryRatio of the total records. Less than $ratio, using DSE Search Optimized request")
            optimizedPredicates
          }
        case On =>
          logDebug(s"Converted $solrConvertibleFilters to $combinedSolrFilter")
          optimizedPredicates
        case Off =>
          predicates
      }
    }
  }

  /** the following code is a copy paste from com.datastax.bdp.search.solr.SolrQueries
    * it is expected that after DSP-16706 is closed it will be possible to extract
    * this functionality to a common module and remove code duplication
    */
  private val escapableWordTokens = Array("AND", "OR", "NOT")
  private val escapableChars = "\\+-!():^[]\"{}~*?|&;/".split("").map(ch => Pattern.quote(ch))
  private val escapableWhitespaces = Array("\\s")
  private val escapables: Pattern = Pattern.compile(escapableEntities.mkString("|"))

  private def escapableEntities: Array[String] = Array(escapableWordTokens, escapableChars, escapableWhitespaces).flatten

  def escapeSolrCondition(condition: String): String = {
    val matcher = escapables.matcher(condition)
    val escaped = StringBuilder.newBuilder
    var firstUnprocessedCharPosition = 0
    while (matcher.find) {
      escaped.append(condition.substring(firstUnprocessedCharPosition, matcher.start))
      firstUnprocessedCharPosition = matcher.end
      escaped.append("\\")
      escaped.append(matcher.group)
    }
    escaped.append(condition.substring(firstUnprocessedCharPosition, condition.length))
    escaped.toString
  }
}
