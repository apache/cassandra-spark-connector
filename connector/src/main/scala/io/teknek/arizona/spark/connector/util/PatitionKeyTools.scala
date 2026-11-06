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

package io.teknek.arizona.spark.connector.util

import java.io.IOException

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.PreparedStatement
import _root_.io.teknek.arizona.spark.connector.cql.{ColumnDef, TableDef}
import _root_.io.teknek.arizona.spark.connector.util.Quote._

import scala.collection.Seq

object PatitionKeyTools {
  /**
    * This query is only used to build a prepared statement so we can more easily extract
    * partition tokens from tables. We prepare a statement of the form SELECT * FROM keyspace.table
    * where x= .... This statement is never executed.
    */
  private[connector] def querySelectUsingOnlyPartitionKeys(tableDef: TableDef): String = {
    val partitionKeys = tableDef.partitionKey
    def quotedColumnNames(columns: Seq[ColumnDef]) = partitionKeys.map(_.columnName).map(quote)
    val whereClause = quotedColumnNames(partitionKeys).map(c => s"$c = :$c").mkString(" AND ")
    s"SELECT * FROM ${quote(tableDef.keyspaceName)}.${quote(tableDef.tableName)} WHERE $whereClause"
  }

  private[connector] def prepareDummyStatement(session: CqlSession, tableDef: TableDef): PreparedStatement = {
    try {
      session.prepare(querySelectUsingOnlyPartitionKeys(tableDef))
    }
    catch {
      case t: Throwable =>
        throw new IOException(
          s"""Failed to prepare statement
             | ${querySelectUsingOnlyPartitionKeys(tableDef)}: """.stripMargin + t.getMessage, t)
    }
  }

}
