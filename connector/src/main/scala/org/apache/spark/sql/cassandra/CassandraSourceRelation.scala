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

import com.datastax.spark.connector.cql.TableDef
import com.datastax.spark.connector.datasource.{CassandraScan, CassandraTable}
import com.datastax.spark.connector.util._
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.jdk.CollectionConverters._

sealed trait DirectJoinSetting
case object AlwaysOn extends DirectJoinSetting
case object AlwaysOff extends DirectJoinSetting
case object Automatic extends DirectJoinSetting

sealed trait DseSearchOptimizationSetting { def enabled = false }

case object On extends DseSearchOptimizationSetting {
  override def enabled = true
  override def toString: String = "on"
}

case object Off extends DseSearchOptimizationSetting {
  override def toString: String = "off"
}

case class Auto(ratio: Double) extends DseSearchOptimizationSetting {
  override def enabled = true
  override def toString: String = "auto"
}

trait CassandraTableDefProvider {
  def tableDef: TableDef
  def withSparkConfOption(key: String, value: String): BaseRelation
}

object CassandraSourceRelation extends Logging {
  private lazy val hiveConf = new HiveConf()

  val ReferenceSection = "Cassandra Datasource Parameters"
  val TableOptions = "Cassandra Datasource Table Options"

  val TableSizeInBytesParam = ConfigParameter[Option[Long]](
    name = "spark.cassandra.table.size.in.bytes",
    section = ReferenceSection,
    default = None,
    description =
      """Used by DataFrames Internally, will be updated in a future release to
        |retrieve size from Cassandra. Can be set manually now""".stripMargin
  )

  val WriteTimeParam = ConfigParameter[Option[String]](
    name = "writetime",
    section = TableOptions,
    default = None,
    description =
      """Surfaces the Cassandra Row Writetime as a Column
        |with the named specified. When reading use writetime.columnName=aliasForWritetime. This
        |can be done for every column with a writetime. When Writing use writetime=columnName and the
        |columname will be used to set the writetime for that row.""".stripMargin
  )

  val TTLParam = ConfigParameter[Option[String]](
    name = "ttl",
    section = TableOptions,
    default = None,
    description =
      """Surfaces the Cassandra Row TTL as a Column
        |with the named specified. When reading use ttl.columnName=aliasForTTL. This
        |can be done for every column with a TTL. When writing use writetime=columnName and the
        |columname will be used to set the TTL for that row.""".stripMargin
  )

  val AdditionalCassandraPushDownRulesParam = ConfigParameter[List[CassandraPredicateRules]](
    name = "spark.cassandra.sql.pushdown.additionalClasses",
    section = ReferenceSection,
    default = List.empty,
    description =
      """A comma separated list of classes to be used (in order) to apply additional
        | pushdown rules for Cassandra Dataframes. Classes must implement CassandraPredicateRules
      """.stripMargin
  )

  val SearchPredicateOptimizationRatioParam = ConfigParameter[Double](
    name = "spark.sql.dse.search.autoRatio",
    section = ReferenceSection,
    default = 0.03,
    description = "When Search Predicate Optimization is set to auto, Search optimizations will be " +
      "preformed if this parameter * the total number of rows is greater than the number of rows " +
      "to be returned by the solr query"
  )

  val SearchPredicateOptimizationParam = ConfigParameter[String](
    name = "spark.sql.dse.search.enableOptimization",
    section = ReferenceSection,
    default = "auto",
    description =
      s"""Enables SparkSQL to automatically replace Cassandra Pushdowns with DSE Search
         |Pushdowns utilizing lucene indexes. Valid options are On, Off, and Auto. Auto enables
         |optimizations when the solr query will pull less than ${SearchPredicateOptimizationRatioParam.name} * the
         |total table record count""".stripMargin
  )

  val SolrPredciateOptimizationParam = DeprecatedConfigParameter(
    name = "spark.sql.dse.solr.enable_optimization",
    replacementParameter = Some(SearchPredicateOptimizationParam),
    deprecatedSince = "DSE 6.0.0"
  )

  val DirectJoinSizeRatioParam = ConfigParameter[Double](
    name = "directJoinSizeRatio",
    section = TableOptions,
    default = 0.9d,
    description =
      s"""
         | Sets the threshold on when to perform a DirectJoin in place of a full table scan. When
         | the size of the (CassandraSource * thisParameter) > The other side of the join, A direct
         | join will be performed if possible.
      """.stripMargin
  )

  val DirectJoinSettingParam = ConfigParameter[String](
    name = "directJoinSetting",
    section = TableOptions,
    default = "auto",
    description =
      s"""Acceptable values, "on", "off", "auto"
         |"on" causes a direct join to happen if possible regardless of size ratio.
         |"off" disables direct join even when possible
         |"auto" only does a direct join when the size ratio is satisfied see ${DirectJoinSizeRatioParam.name}
      """.stripMargin
  )


  val InClauseToJoinWithTableConversionThreshold = ConfigParameter[Long](
    name = "spark.cassandra.sql.inClauseToJoinConversionThreshold",
    section = ReferenceSection,
    default = 2500L,
    description =
      s"""Queries with `IN` clause(s) are converted to JoinWithCassandraTable operation if the size of cross
         |product of all `IN` value sets exceeds this value. To disable `IN` clause conversion, set this setting to 0.
         |Query `select * from t where k1 in (1,2,3) and k2 in (1,2) and k3 in (1,2,3,4)` has 3 sets of `IN` values.
         |Cross product of these values has size of 24.
         """.stripMargin
  )

  val DseInClauseToJoinWithTableConversionThreshold = DeprecatedConfigParameter[Long](
    name = "spark.sql.dse.inClauseToJoinConversionThreshold",
    replacementParameter = Some(InClauseToJoinWithTableConversionThreshold),
    deprecatedSince = "3.0.0",
    rational = "Renamed since this is no longer DSE Specific"
  )

  val InClauseToFullTableScanConversionThreshold = ConfigParameter[Long](
    name = "spark.cassandra.sql.inClauseToFullScanConversionThreshold",
    section = ReferenceSection,
    default = 20000000L,
    description =
      s"""Queries with `IN` clause(s) are not converted to JoinWithCassandraTable operation if the size of cross
         |product of all `IN` value sets exceeds this value. It is meant to stop conversion for huge `IN` values sets
         |that may cause memory problems. If this limit is exceeded full table scan is performed.
         |This setting takes precedence over ${InClauseToJoinWithTableConversionThreshold.name}.
         |Query `select * from t where k1 in (1,2,3) and k2 in (1,2) and k3 in (1,2,3,4)` has 3 sets of `IN` values.
         |Cross product of these values has size of 24.
         """.stripMargin
  )

  val DseInClauseToFullTableScanConversionThreshold = DeprecatedConfigParameter[Long](
    name = "spark.sql.dse.inClauseToFullScanConversionThreshold",
    replacementParameter = Some(InClauseToFullTableScanConversionThreshold),
    deprecatedSince = "3.0.0",
    rational = "Renamed because this is no longer DSE Specific"
  )

  val IgnoreMissingMetaColumns = ConfigParameter[Boolean](
    name = "ignoreMissingMetaColumns",
    section = TableOptions,
    default = false,
    description =
      s"""Acceptable values, "true", "false"
         |"true" ignore missing meta properties
         |"false" throw error if missing property is requested
      """.stripMargin
  )

  private val proxyPerSourceRelationEnabled = sys.env.getOrElse("DSE_ENABLE_PROXY_PER_SRC_RELATION", "false").toBoolean

  private def getProxyUser(sqlContext: SQLContext): Option[String] = {
    val doAsEnabled = hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS)
    val authenticationEnabled = hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION).toUpperCase match {
      case "NONE" => false
      case _ => true
    }
    lazy val user = UserGroupInformation.getCurrentUser

    if (doAsEnabled && authenticationEnabled) {
      Some(user.getUserName)
    } else {
      None
    }
  }

  def setDirectJoin[K: Encoder](ds: Dataset[K], directJoinSetting: DirectJoinSetting = AlwaysOn): Dataset[K] = {
    val classicDs = ds.asInstanceOf[org.apache.spark.sql.classic.Dataset[K]]
    val classicSession = classicDs.sparkSession
    // In Spark 4.x, queryExecution.logical returns the unanalyzed plan (e.g. UnresolvedDataSource).
    // We need the analyzed plan which contains the resolved DataSourceV2Relation nodes.
    val oldPlan = classicDs.queryExecution.analyzed
    org.apache.spark.sql.classic.Dataset[K](classicSession,
      oldPlan.transform {
        case ds@DataSourceV2Relation(table: CassandraTable, _, _, _, options) =>
          // In Spark 4.x, V2ScanRelationPushDown is an optimizer rule. It calls
          // table.newScanBuilder(relation.options) which combines table.catalogConf with
          // the relation options. We update both to ensure the setting propagates.
          val newTable = table.copy(catalogConf = applyDirectJoinSetting(table.catalogConf, directJoinSetting))
          ds.copy(table = newTable, options = applyDirectJoinSetting(options, directJoinSetting))
        case ds@DataSourceV2ScanRelation(_: DataSourceV2Relation, scan: CassandraScan, _, _, _) =>
          ds.copy(scan = scan.copy(consolidatedConf = applyDirectJoinSetting(scan.consolidatedConf, directJoinSetting)))
      }
    )
  }

  val defaultClusterName = "default"

  def directJoinSettingToString(directJoinSetting: DirectJoinSetting) = directJoinSetting match {
    case AlwaysOn => "on"
    case AlwaysOff => "off"
    case Automatic => "auto"
  }

  def applyDirectJoinSetting(options: CaseInsensitiveStringMap, directJoinSetting: DirectJoinSetting) : CaseInsensitiveStringMap = {
    val value = directJoinSettingToString(directJoinSetting)
    new CaseInsensitiveStringMap((Map(DirectJoinSettingParam.name -> value) ++ options.asScala).asJava)
  }

  def applyDirectJoinSetting(sparkConf: SparkConf, directJoinSetting: DirectJoinSetting) : SparkConf = {
    val value = directJoinSettingToString(directJoinSetting)
    sparkConf.set(DirectJoinSettingParam.name, value)
  }

  def getDirectJoinSetting(conf: SparkConf) = {
    conf
      .get(DirectJoinSettingParam.name, DirectJoinSettingParam.default)
      .toLowerCase() match {
      case "auto" => Automatic
      case "on" => AlwaysOn
      case "off" => AlwaysOff
      case invalid => throw new IllegalArgumentException(
        s"""
           |$invalid is not a valid ${DirectJoinSettingParam.name} value.
           |${DirectJoinSettingParam.description}""".stripMargin)
    }
  }
}

object SolrConstants {
  val DseSolrIndexClassName = "com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex"
  val SolrQuery = "solr_query"
}
