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
import org.apache.spark.sql.SaveMode

case class UUIDStringRow(pkey: String, ckey1: String, ckey2: String, data1: String)

class UUIDTypeTest extends AbstractTypeTest[UUID, UUID] with DefaultCluster {
  override val typeName = "uuid"
  override val typeData: Seq[UUID] = Seq(UUID.fromString("05FC7B82-758B-4FC6-91D1-0BD56911DFE8"), UUID.fromString("D27DEEBB-2573-4D2B-AB83-6CBB0708A688"),
    UUID.fromString("0B045E30-9BB4-4932-B11A-3B06FE1658C6"), UUID.fromString("0719091A-429D-4761-AB3D-3728E531718C"), UUID.fromString("DAE83E3D-C67E-4353-9D30-178A8CCBD7C9"))

  override val addData: Seq[UUID] = Seq(UUID.fromString("48830B99-F860-46A9-8187-31EC3F4F614A"), UUID.fromString("C8EF503C-EF97-479E-8E2E-FA363F7CEFD7"), UUID.fromString("77A07FDB-3ACC-4EEB-BEE4-DAE9388A3347"), UUID.fromString("89A6EC10-11F8-408A-A2AD-1A875A6D2E2B"), UUID.fromString("3B20E502-2993-42AE-A993-9425DBAB9EB1"))

  override def getDriverColumn(row: Row, colName: String): UUID = {
    row.getUuid(colName)
  }

  "A String DataFrame" should "write to C* UUIDs" in {
    val UUIDString = "11111111-1111-1111-1111-111111111111"
    val stringRDD = sc.parallelize(Seq(UUIDStringRow(UUIDString, UUIDString, UUIDString, UUIDString)))
    val stringDf = spark.createDataFrame(stringRDD)

    val normOptions = Map("keyspace" -> keyspaceName, "table" -> typeNormalTable)
    stringDf.write
      .format("org.apache.spark.sql.cassandra")
      .options(normOptions)
      .mode(SaveMode.Append)
      .save()

    val row = conn.withSessionDo(session =>
      session.execute(s"SELECT * FROM $keyspaceName.$typeNormalTable WHERE pkey = $UUIDString and ckey1 = $UUIDString").one)
    row.getUuid("data1").toString should be (UUIDString)
  }
}

