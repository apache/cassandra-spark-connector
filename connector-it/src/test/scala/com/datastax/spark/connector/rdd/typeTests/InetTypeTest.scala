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

import java.net.InetAddress

import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.spark.connector.cluster.DefaultCluster
import org.apache.spark.sql.SaveMode

import scala.jdk.CollectionConverters._

case class InetStringRow(pkey: String, ckey1: String, ckey2: String, data1: String)
class InetTypeTest extends AbstractTypeTest[InetAddress, InetAddress] with DefaultCluster {
  override val typeName = "inet"

  override val typeData: Seq[InetAddress] = Seq(InetAddress.getByName("192.168.2.1"), InetAddress.getByName("192.168.2.2"),InetAddress.getByName("192.168.2.3"),InetAddress.getByName("192.168.2.4"),InetAddress.getByName("192.168.2.5"))
  override val addData: Seq[InetAddress] = Seq(InetAddress.getByName("192.168.2.6"), InetAddress.getByName("192.168.2.7"),InetAddress.getByName("192.168.2.8"),InetAddress.getByName("192.168.2.9"),InetAddress.getByName("192.168.2.10"))

  override def getDriverColumn(row: Row, colName: String): InetAddress = {
    row.getInetAddress(colName)
  }

  "A String DataFrame" should "write to C* Inets" in {
    val InetString = "111.111.111.111"
    val stringRDD = sc.parallelize(Seq(InetStringRow(InetString, InetString, InetString, InetString)))
    val stringDf = spark.createDataFrame(stringRDD)

    val normOptions = Map("keyspace" -> keyspaceName, "table" -> typeNormalTable)
    stringDf.write
      .format("org.apache.spark.sql.cassandra")
      .options(normOptions)
      .mode(SaveMode.Append)
      .save()

    val row = conn.withSessionDo(session =>
      session.execute(s"SELECT * FROM $keyspaceName.$typeNormalTable WHERE pkey = '$InetString' and ckey1 = '$InetString'").one)
    row.getInetAddress("data1") should be (InetAddress.getByName(InetString))
  }
}

