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

class AsciiTypeTest extends AbstractTypeTest[String, String] with DefaultCluster {
  override val typeName = "ascii"

  override val typeData: Seq[String] = Seq("row1", "row2", "row3", "row4", "row5")
  override val addData: Seq[String] = Seq("row6", "row7", "row8", "row9", "row10")

  override def getDriverColumn(row: Row, colName: String): String = {
    row.getString(colName)
  }

}

