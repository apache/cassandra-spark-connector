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

package io.teknek.arizona.spark.connector.types

import _root_.io.teknek.arizona.spark.connector.util._

case class ColumnTypeConf(customFromDriver: Option[String])

object ColumnTypeConf {

  val ReferenceSection = "Custom Cassandra Type Parameters (Expert Use Only)"

  val deprecatedCustomDriverTypeParam = DeprecatedConfigParameter(
    "spark.cassandra.dev.customFromDriver",
    None,
    deprecatedSince = "Analytics Connector 1.0",
    rational = "The ability to load new driver type converters at runtime has been removed"
  )

}