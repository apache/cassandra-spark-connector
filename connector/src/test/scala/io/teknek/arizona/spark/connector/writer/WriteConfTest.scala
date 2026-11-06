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

package io.teknek.arizona.spark.connector.writer

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel
import _root_.io.teknek.arizona.spark.connector.{BytesInBatch, RowsInBatch}
import org.apache.spark.SparkConf
import org.scalatest.{FlatSpec, Matchers}

class WriteConfTest extends FlatSpec with Matchers {

  "WriteConf" should "be configured with proper defaults" in {
    val conf = new SparkConf(false)
    val writeConf = WriteConf.fromSparkConf(conf)

    writeConf.batchSize should be (BytesInBatch(WriteConf.BatchSizeBytesParam.default))
    writeConf.consistencyLevel should be (WriteConf.ConsistencyLevelParam.default)
    writeConf.parallelismLevel should be (WriteConf.ParallelismLevelParam.default)
  }

  it should "allow setting the rate limit as a decimal" in {
    val conf = new SparkConf(false)
      .set("spark.cassandra.output.throughputMBPerSec", "0.5")
    val writeConf = WriteConf.fromSparkConf(conf)
      writeConf.throughputMiBPS.get should equal ( 0.5 +- 0.02 )
  }

  it should "allow to set consistency level" in {
    val conf = new SparkConf(false)
      .set("spark.cassandra.output.consistency.level", "THREE")
    val writeConf = WriteConf.fromSparkConf(conf)

    writeConf.consistencyLevel should be(DefaultConsistencyLevel.THREE)
  }

  it should "allow to set parallelism level" in {
    val conf = new SparkConf(false)
      .set("spark.cassandra.output.concurrent.writes", "17")
    val writeConf = WriteConf.fromSparkConf(conf)

    writeConf.parallelismLevel should be(17)
  }

  it should "allow to set batch size in bytes" in {
    val conf = new SparkConf(false)
      .set("spark.cassandra.output.batch.size.bytes", "12345")
    val writeConf = WriteConf.fromSparkConf(conf)

    writeConf.batchSize should be(BytesInBatch(12345))
  }

  it should "allow to set batch size in bytes when rows are set to auto" in {
    val conf = new SparkConf(false)
      .set("spark.cassandra.output.batch.size.bytes", "12345")
      .set("spark.cassandra.output.batch.size.rows", "auto")
    val writeConf = WriteConf.fromSparkConf(conf)

    writeConf.batchSize should be(BytesInBatch(12345))
  }

  it should "allow to set batch size in rows" in {
    val conf = new SparkConf(false)
      .set("spark.cassandra.output.batch.size.rows", "12345")
    val writeConf = WriteConf.fromSparkConf(conf)

    writeConf.batchSize should be(RowsInBatch(12345))
  }

  it should "allow to set batch level" in {
    val conf = new SparkConf(false)
      .set("spark.cassandra.output.batch.grouping.key", "none")
    val writeConf = WriteConf.fromSparkConf(conf)
    writeConf.batchGroupingKey should be(BatchGroupingKey.None)
  }

  it should "allow to set batch buffer size" in {
    val conf = new SparkConf(false)
      .set("spark.cassandra.output.batch.grouping.buffer.size", "30000")
    val writeConf = WriteConf.fromSparkConf(conf)
    writeConf.batchGroupingBufferSize should be(30000)
  }


}
