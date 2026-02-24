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

package org.apache.spark.sql.datastax.test.monotonic

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.execution.streaming.{LongOffset, Offset, SerializedOffset, Source}
import org.apache.spark.sql.sources.StreamSourceProvider
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

import scala.util.Try


class DefaultSource extends StreamSourceProvider {

  override def createSource(
                             spark: SQLContext,
                             metadataPath: String,
                             schema: Option[StructType],
                             providerName: String,
                             parameters: Map[String, String]): Source = {

    new Source {
      var offset = new LongOffset(0)
      override def schema: StructType = StructType(Seq(StructField("key", IntegerType, nullable = false )))

      override def getOffset: Option[Offset] = {
        offset = offset + 100L
        Some(offset.copy())
      }

      override def getBatch(start: Option[Offset], end: Offset): DataFrame = {

        val startValue = start match {
          case Some(ser: SerializedOffset) => FakeStreamSource.parseOffset(ser.json)
          case Some(LongOffset(x)) => x
          case None => 0
          case _ => -1
        }

        val endValue = end match {
          case ser: SerializedOffset => FakeStreamSource.parseOffset(ser.json)
          case LongOffset(x) => x
        }
        val rows = (startValue.toInt to endValue.toInt).map( value =>
          new GenericInternalRow(values = Array(value)))
        org.apache.spark.sql.classic.Dataset.ofRows(spark.sparkSession.asInstanceOf[org.apache.spark.sql.classic.SparkSession], LocalRelation(DataTypeUtils.toAttributes(schema), rows, isStreaming = true))
      }

      override def stop() {}
    }
  }

  override def sourceSchema(
                             sqlContext: SQLContext,
                             schema: Option[StructType],
                             providerName: String,
                             parameters: Map[String, String]): (String, StructType) = {

    ("FakeStream", StructType(Seq(StructField("key", IntegerType, nullable = false ))))
  }
}

object FakeStreamSource {
  def parseOffset(str: String): Long = {
    Try (str.stripPrefix("[").stripSuffix("]").toLong) getOrElse (0)
  }
}
