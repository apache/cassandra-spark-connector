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

import java.math.BigDecimal
import java.math.BigInteger

import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.spark.connector.cluster.DefaultCluster
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.SaveMode

case class LongRow(pkey: Long, ckey1: Long, ckey2: Long, data1: Long)
case class BigDecimalRow(pkey: BigDecimal, ckey1: BigDecimal, ckey2: BigDecimal, data1: BigDecimal)

class VarintTypeTest extends AbstractTypeTest[BigInteger, BigInteger] with DefaultCluster {
  override val typeName = "varint"

  override val typeData: Seq[BigInteger] = Seq(new BigInteger("1001"), new BigInteger("2002"),new BigInteger("3003"), new BigInteger("4004"), new BigInteger("5005"))
  override val addData: Seq[BigInteger] = Seq(new BigInteger("6006"), new BigInteger("7007"), new BigInteger("8008"), new BigInteger("9009"), new BigInteger("100012"))

  override def getDriverColumn(row: Row, colName: String): BigInteger = {
    row.getBigInteger(colName)
  }

  "A LongType DataFrame" should "write to VarInt C* tables" in {
    val LongValue = 11111111
    val longRDD = sc.parallelize(Seq(LongRow(LongValue, LongValue, LongValue, LongValue)))
    val longDf = spark.createDataFrame(longRDD)

    val normOptions = Map("keyspace" -> keyspaceName, "table" -> typeNormalTable)
    longDf.write
      .format("org.apache.spark.sql.cassandra")
      .options(normOptions)
      .mode(SaveMode.Append)
      .save()

    val row = conn.withSessionDo(session =>
      session.execute(s"SELECT * FROM $keyspaceName.$typeNormalTable WHERE pkey = $LongValue and ckey1 = $LongValue").one)
    row.getBigInteger("data1").longValue() should be (LongValue)

  }

  "A DecimalType DataFrame with Scale 0" should "write to VarInt C* Tables" in {
    val BigDecimalValue = new BigDecimal(22222)

    val bigDecimalRDD = sc.parallelize(Seq(BigDecimalRow(BigDecimalValue, BigDecimalValue, BigDecimalValue, BigDecimalValue)))
    val bigDecimalDf = spark.createDataFrame(bigDecimalRDD)

    val normOptions = Map("keyspace" -> keyspaceName, "table" -> typeNormalTable)
    bigDecimalDf
      .withColumn("pkey", bigDecimalDf("pkey").cast(DecimalType(12,0)))
      .withColumn("ckey1", bigDecimalDf("ckey2").cast(DecimalType(12,0)))
      .withColumn("ckey2", bigDecimalDf("ckey2").cast(DecimalType(12,0)))
      .withColumn("data1", bigDecimalDf("data1").cast(DecimalType(12,0)))
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(normOptions)
      .mode(SaveMode.Append)
      .save()

    val row = conn.withSessionDo(session =>
      session.execute(s"SELECT * FROM $keyspaceName.$typeNormalTable WHERE pkey = $BigDecimalValue and ckey1 = $BigDecimalValue").one)
    row.getBigInteger("data1").longValue() should be (BigDecimalValue.longValue())
  }
}

