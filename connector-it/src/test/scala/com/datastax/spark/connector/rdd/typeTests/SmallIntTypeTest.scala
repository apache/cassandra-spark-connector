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

package com.datastax.spark.connector.rdd.typeTests

import com.datastax.oss.driver.api.core.DefaultProtocolVersion
import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.spark.connector.cluster.DefaultCluster

class SmallIntTypeTest extends AbstractTypeTest[Short, java.lang.Short] with DefaultCluster {
  override val minPV = DefaultProtocolVersion.V4
  override protected val typeName: String = "smallint"

  override protected val typeData: Seq[Short] = (1 to 10).map(_.toShort)
  override protected val addData: Seq[Short] = (11 to 20).map(_.toShort)

  override def getDriverColumn(row: Row, colName: String): Short = row.getShort(colName)

}
