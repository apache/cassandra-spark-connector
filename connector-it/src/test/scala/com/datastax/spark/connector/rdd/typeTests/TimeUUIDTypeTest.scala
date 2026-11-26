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

import java.util.UUID

import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.spark.connector.cluster.DefaultCluster

class TimeUUIDTypeTest extends AbstractTypeTest[UUID, UUID] with DefaultCluster {

  override val typeName = "timeuuid"

  override val typeData: Seq[UUID] = Seq(UUID.fromString("61129590-FBE4-11E3-A3AC-0800200C9A66"), UUID.fromString("61129591-FBE4-11E3-A3AC-0800200C9A66"),
    UUID.fromString("61129592-FBE4-11E3-A3AC-0800200C9A66"), UUID.fromString("61129593-FBE4-11E3-A3AC-0800200C9A66"), UUID.fromString("61129594-FBE4-11E3-A3AC-0800200C9A66"))
  override val addData: Seq[UUID] = Seq(UUID.fromString("204FF380-FBE5-11E3-A3AC-0800200C9A66"), UUID.fromString("204FF381-FBE5-11E3-A3AC-0800200C9A66"), UUID.fromString("204FF382-FBE5-11E3-A3AC-0800200C9A66"), UUID.fromString("204FF383-FBE5-11E3-A3AC-0800200C9A66"), UUID.fromString("204FF384-FBE5-11E3-A3AC-0800200C9A66"))

  override def getDriverColumn(row: Row, colName: String): UUID = {
    row.getUuid(colName)
  }
}

