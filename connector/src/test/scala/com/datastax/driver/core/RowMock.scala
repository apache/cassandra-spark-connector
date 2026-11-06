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

package com.datastax.spark.connector.test

import java.nio.ByteBuffer

import com.datastax.oss.driver.api.core.`type`.codec.registry.CodecRegistry
import com.datastax.oss.driver.api.core.`type`.{DataType, DataTypes}
import com.datastax.oss.driver.api.core.cql.{ColumnDefinition, ColumnDefinitions, Row}
import com.datastax.oss.driver.api.core.detach.AttachmentPoint
import com.datastax.oss.driver.api.core.metadata.token.Token
import com.datastax.oss.driver.api.core.{CqlIdentifier, ProtocolVersion}
import com.datastax.oss.driver.internal.core.cql.{DefaultColumnDefinition, DefaultColumnDefinitions}
import com.datastax.oss.protocol.internal.ProtocolConstants
import com.datastax.oss.protocol.internal.response.result.{ColumnSpec, RawType}

import scala.jdk.CollectionConverters._

class RowMock(columnSizes: Option[Int]*) extends Row {

  val bufs = columnSizes.map {
    case Some(size) => ByteBuffer.allocate(size)
    case _ => null
  }.toArray

  val defs = DefaultColumnDefinitions.valueOf(
    columnSizes.zipWithIndex.map { case (i, id) =>
      val columnSepc = new ColumnSpec("ks", "tab", s"c$i", id, RawType.PRIMITIVES.get(ProtocolConstants.DataType.VARCHAR))
      new DefaultColumnDefinition(columnSepc, null).asInstanceOf[ColumnDefinition]
    }.toList.asJava
  )

  override def getColumnDefinitions: ColumnDefinitions = defs

  override def getBytesUnsafe(i: Int): ByteBuffer = bufs(i)

  override def getBytesUnsafe(s: String): ByteBuffer = getBytesUnsafe(defs.firstIndexOf(s))

  override def isNull(i: Int): Boolean = bufs(i) == null

  override def isNull(s: String): Boolean = isNull(defs.firstIndexOf(s))

  override def getToken(i: Int): Token = ???

  override def getToken(name: String): Token = ???

  override def getType(i: Int): DataType = ???

  override def isDetached: Boolean = ???

  override def attach(attachmentPoint: AttachmentPoint): Unit = ???

  override def firstIndexOf(name: String): Int = ???

  override def getType(name: String): DataType = ???

  override def firstIndexOf(id: CqlIdentifier): Int = ???

  override def getType(id: CqlIdentifier): DataType = ???

  override def size(): Int = ???

  override def codecRegistry(): CodecRegistry = CodecRegistry.DEFAULT

  override def protocolVersion(): ProtocolVersion = ProtocolVersion.DEFAULT
}
