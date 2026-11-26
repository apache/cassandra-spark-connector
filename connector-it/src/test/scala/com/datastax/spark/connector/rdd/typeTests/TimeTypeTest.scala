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

import java.time.LocalTime

import com.datastax.spark.connector._

import com.datastax.oss.driver.api.core.DefaultProtocolVersion
import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.spark.connector.cluster.DefaultCluster

class TimeTypeTest extends AbstractTypeTest[LocalTime, LocalTime] with DefaultCluster {

  override val minPV = DefaultProtocolVersion.V4

  override def getDriverColumn(row: Row, colName: String): LocalTime = row.getLocalTime(colName)

  override protected val typeName: String = "time"

  override protected val typeData: Seq[LocalTime] = (1L to 5L).map(LocalTime.ofNanoOfDay)
  override protected val addData: Seq[LocalTime] = (6L to 10L).map(LocalTime.ofNanoOfDay)

  "Time Types" should "be writable as dates" in skipIfProtocolVersionLT(minPV) {
    val times = (100 to 500 by 100).map(LocalTime.ofNanoOfDay(_))
    sc.parallelize(times.map(x => (x, x, x, x))).saveToCassandra(keyspaceName, typeNormalTable)
    val results = sc.cassandraTable[(LocalTime, LocalTime, LocalTime, LocalTime)](keyspaceName, typeNormalTable).collect
    checkNormalRowConsistency(times, results)
  }

}
