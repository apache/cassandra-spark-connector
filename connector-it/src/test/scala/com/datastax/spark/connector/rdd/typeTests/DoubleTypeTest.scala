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

import java.lang.Double

import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.spark.connector.cluster.DefaultCluster

class DoubleTypeTest extends AbstractTypeTest[Double, Double] with DefaultCluster {
  override val typeName = "double"

  override val typeData: Seq[Double] = Seq(new Double(100.1), new Double(200.2),new Double(300.3), new Double(400.4), new Double(500.5))
  override val addData: Seq[Double] = Seq(new Double(600.6), new Double(700.7), new Double(800.8), new Double(900.9), new Double(1000.12))

  override def getDriverColumn(row: Row, colName: String): Double = {
    row.getDouble(colName)
  }
}

