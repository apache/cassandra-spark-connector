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

package io.teknek.arizona.spark.connector.cql

import java.nio.ByteBuffer

import com.datastax.oss.driver.api.core.cql.BoundStatement
import _root_.io.teknek.arizona.spark.connector.writer.NullKeyColumnException

import scala.jdk.CollectionConverters._

object QueryUtils {
  /**
    * If a bound statement has all partition key components bound it will
    * return a routing key, but if all components are not bound it returns
    * null. When this is the case we want to let the user know which columns
    * were not correctly bound
    * @param bs a statement completely bound with all parameters
    * @return The routing key
    */
  def getRoutingKeyOrError(bs: BoundStatement): ByteBuffer = {
    val routingKey = bs.getRoutingKey
    if (routingKey == null) throw new NullKeyColumnException(nullPartitionKeyValues(bs))
    routingKey
  }

  private def nullPartitionKeyValues(bs: BoundStatement) = {
    val pkIndicies = bs.getPreparedStatement.getPartitionKeyIndices
    val boundValues = bs.getValues
    pkIndicies.asScala
      .filter(bs.isNull(_))
      .map(bs.getPreparedStatement.getVariableDefinitions.get(_))
      .map(_.getName)
      .mkString(", ")
  }


}
