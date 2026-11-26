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

package org.apache.spark.sql.datastax.test.empty

import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.execution.streaming.{Offset, Source}
// Spark 4 moved the streaming offset implementations into the .runtime sub-package.
import org.apache.spark.sql.execution.streaming.runtime.LongOffset
import org.apache.spark.sql.sources.StreamSourceProvider
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.classic.{Dataset => ClassicDataset, SparkSession => ClassicSparkSession}


/**
  * @author Russell Spitzer
  */
class DefaultSource extends StreamSourceProvider {
  override def sourceSchema(
                             sqlContext: SQLContext,
                             schema: Option[StructType],
                             providerName: String,
                             parameters: Map[String, String]): (String, StructType) = {

    ("FakeStream", StructType(Seq(StructField("key", IntegerType, nullable = false ))))
  }

  override def createSource(sqlContext: SQLContext, metadataPath: String,
                            schema: Option[StructType], providerName: String, parameters: Map[String, String]) =
    new Source {

    var offset = new LongOffset(0)
    override def schema: StructType = StructType(Seq(StructField("key", IntegerType, nullable = false )))

    override def getOffset: Option[Offset] = {
      offset = offset + 100L
      Some(offset.copy())
    }

    override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
      ClassicDataset.ofRows(sqlContext.sparkSession.asInstanceOf[ClassicSparkSession],
        LocalRelation(DataTypeUtils.toAttributes(schema), isStreaming = true))
    }

    override def stop() {}
  }
}

