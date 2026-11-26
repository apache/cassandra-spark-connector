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

import java.text.SimpleDateFormat
import java.time.Instant
import java.util.Date

import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.spark.connector.cluster.DefaultCluster

class TimestampTypeTest extends AbstractTypeTest[Instant, Instant] with DefaultCluster {
  override val typeName = "timestamp"
  val sdf = new SimpleDateFormat("dd/MM/yyyy")

  override val typeData: Seq[Instant] = Seq(
    sdf.parse("03/08/1985"),
    sdf.parse("03/08/1986"),
    sdf.parse("03/08/1987"),
    sdf.parse("03/08/1988"),
    sdf.parse("03/08/1989")).map(_.toInstant)
  override val addData: Seq[Instant] = Seq(
    sdf.parse("03/08/1990"),
    sdf.parse("03/08/1991"),
    sdf.parse("03/08/1992"),
    sdf.parse("03/08/1993"),
    sdf.parse("03/08/1994")).map(_.toInstant)

  override def getDriverColumn(row: Row, colName: String): Instant = {
    row.getInstant(colName)
  }

}

