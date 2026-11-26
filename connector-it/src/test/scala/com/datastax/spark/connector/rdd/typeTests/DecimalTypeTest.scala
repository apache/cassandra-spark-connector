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

import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.spark.connector.cluster.DefaultCluster


class DecimalTypeTest extends AbstractTypeTest[BigDecimal, java.math.BigDecimal] with DefaultCluster {

  implicit def toBigDecimal(str: String) = BigDecimal(str)

  override def convertToDriverInsertable(testValue: BigDecimal): java.math.BigDecimal = testValue.bigDecimal

  override val typeName = "decimal"

  override val typeData: Seq[BigDecimal] = Seq("100.1", "200.2", "301.1")
  override val addData: Seq[BigDecimal] = Seq("600.6", "700.7", "721.444")

  override def getDriverColumn(row: Row, colName: String): BigDecimal = {
    BigDecimal(row.getBigDecimal(colName))
  }
}
