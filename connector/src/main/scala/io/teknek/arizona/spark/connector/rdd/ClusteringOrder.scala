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

package io.teknek.arizona.spark.connector.rdd

import _root_.io.teknek.arizona.spark.connector.cql.TableDef

sealed trait ClusteringOrder extends Serializable {
  private[connector] def toCql(tableDef: TableDef): String
}

object ClusteringOrder {
  private[connector] def cqlClause(tableDef: TableDef, order: String) =
    tableDef.clusteringColumns.headOption.map(cc => s"""ORDER BY "${cc.columnName}" $order""")
      .getOrElse(throw new IllegalArgumentException("Order by can be specified only if there are some clustering columns"))

  case object Ascending extends ClusteringOrder {
    override private[connector] def toCql(tableDef: TableDef): String = cqlClause(tableDef, "ASC")
  }

  case object Descending extends ClusteringOrder {
    override private[connector] def toCql(tableDef: TableDef): String = cqlClause(tableDef, "DESC")
  }

}
