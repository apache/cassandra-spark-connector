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

import com.datastax.spark.connector.cql.{ColumnDef, TableDef}
import com.datastax.spark.connector.util.Logging
import org.apache.spark.SparkConf
import org.apache.spark.sql.cassandra.CassandraSourceRelation.InClauseToFullTableScanConversionThreshold
import org.apache.spark.sql.cassandra.PredicateOps.FilterOps.{columnName, isInPredicate}
import org.apache.spark.sql.sources.{Filter, In}

object InClausePredicateRules extends CassandraPredicateRules with Logging {

  def inCrossProductSize(filters: Set[Filter]): Long =
    filters.toSeq.collect { case in@In(_, _) => in.values.length }.product

  private def pushFiltersToSpark(predicates: AnalyzedPredicates, filters: Set[Filter]): AnalyzedPredicates =
    predicates.copy(
      handledBySpark = predicates.handledBySpark ++ filters,
      handledByCassandra = predicates.handledByCassandra -- filters)

  private def columnsFilters(filters: Set[Filter], columns: Seq[ColumnDef]): Set[Filter] =
    filters.filter(f => columns.exists(_.columnName == columnName(f)))

  private def filterOutHugeInClausePredicates(
    predicates: AnalyzedPredicates,
    tableDef: TableDef,
    sparkConf: SparkConf): AnalyzedPredicates = {
    val fullTableScanConversionThreshold = sparkConf.getLong(
      InClauseToFullTableScanConversionThreshold.name,
      InClauseToFullTableScanConversionThreshold.default)

    val inFilters = predicates.handledByCassandra.filter(isInPredicate)

    val partitionColumnsFilters = columnsFilters(inFilters, tableDef.partitionKey)
    val clusteringColumnsFilters = columnsFilters(inFilters, tableDef.clusteringColumns)

    val partitionCartesianSize = inCrossProductSize(partitionColumnsFilters)
    val clusteringCartesianSize = inCrossProductSize(clusteringColumnsFilters)

    if (partitionCartesianSize * clusteringCartesianSize < fullTableScanConversionThreshold) {
      predicates
    } else if (partitionCartesianSize < fullTableScanConversionThreshold) {
      logInfo(s"Number of key combinations in 'IN' clauses exceeds ${InClauseToFullTableScanConversionThreshold.name} " +
        s"($fullTableScanConversionThreshold), clustering columns filters are not pushed down.")
      pushFiltersToSpark(predicates, clusteringColumnsFilters)
    } else {
      logInfo(s"Number of key combinations in 'IN' clauses exceeds ${InClauseToFullTableScanConversionThreshold.name} " +
        s"($fullTableScanConversionThreshold), partition key filters are not pushed down. This results in full table " +
        s"scan with Spark side filtering.")
      pushFiltersToSpark(predicates, partitionColumnsFilters ++ clusteringColumnsFilters)
    }
  }

  override def apply(predicates: AnalyzedPredicates, tableDef: TableDef, conf: SparkConf): AnalyzedPredicates = {
    filterOutHugeInClausePredicates(predicates, tableDef, conf)
  }
}
