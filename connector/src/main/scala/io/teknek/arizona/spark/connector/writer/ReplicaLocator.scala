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


import java.net.{InetAddress, InetSocketAddress}

import com.datastax.oss.driver.api.core.CqlIdentifier
import _root_.io.teknek.arizona.spark.connector.ColumnSelector
import _root_.io.teknek.arizona.spark.connector.cql._
import _root_.io.teknek.arizona.spark.connector.util.DriverUtil._
import _root_.io.teknek.arizona.spark.connector.util.{DriverUtil, Logging, tableFromCassandra}
import _root_.io.teknek.arizona.spark.connector.util.PatitionKeyTools._

import scala.collection._
import scala.jdk.CollectionConverters._

/**
 * A utility class for determining the Replica Set (Ip Addresses) of a particular Cassandra Row. Used
 * by the [[io.teknek.arizona.spark.connector.RDDFunctions.keyByCassandraReplica]] method. Uses the Java
 * Driver to obtain replica information.
 */
class ReplicaLocator[T] private(
    connector: CassandraConnector,
    tableDef: TableDef,
    rowWriter: RowWriter[T]) extends Serializable with Logging {

  val keyspaceName = tableDef.keyspaceName
  val tableName = tableDef.tableName
  val columnNames = rowWriter.columnNames
  val tokenMap = toOption(connector.withSessionDo(_.getMetadata.getTokenMap))
    .getOrElse(throw new IllegalStateException("Unable to determine Token Map Metadata"))

  /**
   * Pairs each piece of data with the Cassandra Replicas which that data would be found on
   * @param data A source of data which can be bound to a statement by BatchStatementBuilder
   * @return an Iterator over the same data keyed by the replica's ip addresses
   */
  def keyByReplicas(data: Iterator[T]): Iterator[(scala.collection.immutable.Set[InetAddress], T)] = {
      connector.withSessionDo { session =>
        val protocolVersion = session.getContext.getProtocolVersion
        val stmt = prepareDummyStatement(session, tableDef)
        val boundStmtBuilder = new BoundStatementBuilder(rowWriter, stmt, protocolVersion = protocolVersion)
        val clusterMetadata = session.getMetadata
        data.map { row =>
          val hosts = tokenMap
            .getReplicas(CqlIdentifier.fromInternal(keyspaceName), QueryUtils.getRoutingKeyOrError(boundStmtBuilder.bind(row).stmt)).asScala
            .map(node => DriverUtil.toAddress(node)
              .getOrElse(throw new IllegalStateException(s"Unable to determine Node Broadcast Address of $node")))
            .map(_.getAddress)
            .toSet[InetAddress]
          (hosts, row)
        }
    }
  }
}

/**
 * Helper methods for mapping a set of data to their relative locations in a Cassandra Cluster.
 */
object ReplicaLocator {
  def apply[T: RowWriterFactory](
      connector: CassandraConnector,
      keyspaceName: String,
      tableName: String,
      partitionKeyMapper: ColumnSelector): ReplicaLocator[T] = {

    val tableDef = tableFromCassandra(connector, keyspaceName, tableName)
    val rowWriter = implicitly[RowWriterFactory[T]].rowWriter(
      tableDef,
      partitionKeyMapper.selectFrom(tableDef)
    )
    new ReplicaLocator[T](connector, tableDef, rowWriter)
  }
}
