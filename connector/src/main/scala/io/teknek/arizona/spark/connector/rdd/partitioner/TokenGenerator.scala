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

package io.teknek.arizona.spark.connector.rdd.partitioner

import java.nio.ByteBuffer

import _root_.io.teknek.arizona.spark.driver.core.MetadataHook
import com.datastax.oss.driver.api.core.cql.BoundStatement
import com.datastax.oss.driver.api.core.metadata.token.Token
import _root_.io.teknek.arizona.spark.connector.cql.{CassandraConnector, QueryUtils, TableDef}
import _root_.io.teknek.arizona.spark.connector.util.Logging
import _root_.io.teknek.arizona.spark.connector.util.PatitionKeyTools._
import _root_.io.teknek.arizona.spark.connector.writer.{BoundStatementBuilder, NullKeyColumnException, RowWriter}

import scala.jdk.CollectionConverters._

/**
  * A utility class for determining the token of a given key. Uses a bound statement to determine
  * the routing key and the uses that with the TokenFactory to determine the hashed Token.
  */
private[connector] class TokenGenerator[T] (
  connector: CassandraConnector,
  tableDef: TableDef,
  rowWriter: RowWriter[T]) extends Serializable with Logging {

  val protocolVersion = connector.withSessionDo { session =>
    session.getContext.getProtocolVersion
  }

  //Makes a PreparedStatement which we use only to generate routing keys on the client
  val stmt = connector.withSessionDo { session => prepareDummyStatement(session, tableDef) }
  val metadata = connector.withSessionDo(_.getMetadata)

  val boundStmtBuilder = new BoundStatementBuilder(
    rowWriter,
    stmt,
    protocolVersion = protocolVersion)

  def getRoutingKey(key: T): ByteBuffer = {
    val boundStatement = boundStmtBuilder.bind(key).stmt
    QueryUtils.getRoutingKeyOrError(boundStatement)
  }

  def getTokenFor(key: T): Token = {
    MetadataHook.newToken(metadata, getRoutingKey(key))
  }

  def getStringTokenFor(key: T): String = {
    MetadataHook.newTokenAsString(metadata, getRoutingKey(key))
  }
}
