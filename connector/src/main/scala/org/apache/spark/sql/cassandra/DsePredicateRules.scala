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

import com.datastax.spark.connector.cql.{IndexDef, TableDef}
import com.datastax.spark.connector.types._
import com.datastax.spark.connector.util.Logging
import org.apache.spark.SparkConf
import org.apache.spark.sql.cassandra.PredicateOps.FilterOps
import org.apache.spark.sql.sources.{EqualTo, Filter, IsNotNull}
import scala.collection.compat._


/**
  * A series of pushdown rules that only apply when connecting to Datastax Enterprise
  */
object DsePredicateRules extends CassandraPredicateRules with Logging {

  val StorageAttachedIndex = "StorageAttachedIndex"

  private type PredicateRule = (AnalyzedPredicates, TableDef) => AnalyzedPredicates
  private val saiTextTypes = Set[ColumnType[_]](TextType, AsciiType, VarCharType, UUIDType)
  private val saiNumericTypes = Set[ColumnType[_]](IntType, BigIntType, SmallIntType, TinyIntType, FloatType, DoubleType,
    VarIntType, DecimalType, TimestampType, DateType, InetType, TimeType, TimestampType, TimeUUIDType)

  override def apply(
    predicates: AnalyzedPredicates,
    tableDef: TableDef,
    sparkConf: SparkConf): AnalyzedPredicates = {

    val pushDownRules = Seq[PredicateRule](
      pushOnlySolrQuery
    ) ++ storageAttachedIndexPredicateRules(predicates, tableDef)

    pushDownRules.foldLeft(predicates) { (p, rule) => rule(p, tableDef) }
  }

  /**
    * When using a "solr_query = clause" we need to remove all other predicates to be pushed down.
    *
    * Example : SparkSQL : "SELECT * FROM ks.tab WHERE solr_query = data:pikachu and pkey = bob"
    *
    * In this example only the predicate solr_query can be handled in CQL. Passing pkey as well
    * will cause an exception.
    */
  private def pushOnlySolrQuery(predicates: AnalyzedPredicates, tableDef: TableDef): AnalyzedPredicates = {

    val allPredicates = predicates.handledByCassandra ++ predicates.handledBySpark

    val solrQuery: Set[Filter] = allPredicates.collect {
      case EqualTo(column, value) if column == "solr_query" => EqualTo(column, value)
    }

    //Spark 2.0 + generates an IsNotNull when we do "= literal"
    val solrIsNotNull: Set[Filter] = allPredicates.collect {
      case IsNotNull(column) if column == "solr_query" => IsNotNull(column)
    }

    if (solrQuery.nonEmpty) {
      AnalyzedPredicates(solrQuery, allPredicates -- solrQuery -- solrIsNotNull)
    } else {
      predicates
    }
  }

  // TODO: InClausePredicateRules is applied after DsePredicateRules, it may remove some of the IN clause from
  // pushdown - we might consider applying SAI later then IN rules.
  private def saiIndexes(table: TableDef): Seq[IndexDef] = {
    table.indexes.filter(_.className.contains(StorageAttachedIndex))
  }

  private def pushDownPredicates(predicates: AnalyzedPredicates, table: TableDef)
    (eligibleForPushDown: Filter => Boolean): AnalyzedPredicates = {
    val indexColumns = saiIndexes(table).map(_.targetColumn)
    val pushedEqualityPredicates = predicates.handledByCassandra.collect {
      case f if FilterOps.isEqualToPredicate(f) => FilterOps.columnName(f)
    }.to(collection.mutable.Set)

    val (handledByCassandra, handledBySpark) = predicates.handledBySpark.partition { filter =>
      lazy val columnName = FilterOps.columnName(filter)
      lazy val eligibleForSAIPushDown = eligibleForPushDown(filter) &&
        indexColumns.contains(columnName) &&
        !pushedEqualityPredicates.contains(columnName)

      val isEqualityPredicate = FilterOps.isEqualToPredicate(filter)
      if (isEqualityPredicate && eligibleForSAIPushDown) {
        pushedEqualityPredicates += columnName
      }
      eligibleForSAIPushDown
    }

    AnalyzedPredicates(predicates.handledByCassandra ++ handledByCassandra, handledBySpark)
  }

  private def pushDownSAITextPredicates(predicates: AnalyzedPredicates, table: TableDef): AnalyzedPredicates =
    pushDownPredicates(predicates, table) { filter =>
      FilterOps.isEqualToPredicate(filter) &&
        saiTextTypes.contains(table.columnByName(FilterOps.columnName(filter)).columnType)
    }

  private def pushDownSAINumericPredicates(predicates: AnalyzedPredicates, table: TableDef): AnalyzedPredicates =
    pushDownPredicates(predicates, table) { filter =>
      (FilterOps.isEqualToPredicate(filter) || FilterOps.isRangePredicate(filter)) &&
        saiNumericTypes.contains(table.columnByName(FilterOps.columnName(filter)).columnType)
    }

  /**
    * 1. IN clause for a partition key part prevents SAI push downs
    * 2. OR clause prevents any push down
    * 3. At most one equality is pushed
    * 4. Only equality is pushed if equality and range are defined
    * 5. Supported predicates:
    *  - numeric types: >, <, >=, <=, =
    *  - text types: =
    *  - set and list types: contains (non-frozen collections), = (frozen collections)
    *  - map type: contains key, contains value, key lookup result equality (map(key) == value), = (frozen collections)
    *
    *  NOTE that collection types support is going to be introduces in SPARKC-630
    */
  private def storageAttachedIndexPredicateRules(predicates: AnalyzedPredicates, table: TableDef): Seq[PredicateRule] = {
    val partitionKeyColumns = table.partitionKey.map(_.columnName).toSet
    val inClauseInPartitionKey = predicates.handledByCassandra
      .find(f => FilterOps.isInPredicate(f) && f.references.exists(partitionKeyColumns.contains))

    if (inClauseInPartitionKey.isDefined) {
      logDebug(s"SAI pushdown is not possible, the query contains IN clause " +
        s"on ${inClauseInPartitionKey.get.references.mkString(",")} partition key column(s).")
      Seq()
    } else if (saiIndexes(table).isEmpty) {
      logDebug(s"There are no SAI indexes on the given table: ${table.name}")
      Seq()
    } else {
      Seq(
        pushDownSAITextPredicates,
        pushDownSAINumericPredicates
      )
    }
  }
}



