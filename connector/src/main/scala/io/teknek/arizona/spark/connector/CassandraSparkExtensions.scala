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

package io.teknek.arizona.spark.connector

import org.apache.spark.sql.{SparkSessionExtensions, catalyst}
import _root_.org.apache.spark.sql.cassandra.execution.CassandraDirectJoinStrategy
import _root_.org.apache.spark.sql.cassandra.{CassandraMetaDataRule, CassandraMetadataFunction}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import _root_.io.teknek.arizona.spark.connector.util.Logging
import org.apache.spark.sql.catalyst.expressions.Expression

class CassandraSparkExtensions extends (SparkSessionExtensions => Unit) with Logging {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectPlannerStrategy(CassandraDirectJoinStrategy.apply)
    extensions.injectResolutionRule(session => CassandraMetaDataRule)
    extensions.injectFunction(CassandraMetadataFunction.cassandraTTLFunctionDescriptor)
    extensions.injectFunction(CassandraMetadataFunction.cassandraWriteTimeFunctionDescriptor)
  }
}
