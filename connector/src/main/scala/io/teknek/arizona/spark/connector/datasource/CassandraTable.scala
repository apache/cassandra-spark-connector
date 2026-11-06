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

package io.teknek.arizona.spark.connector.datasource

import java.util
import com.datastax.oss.driver.api.core.metadata.schema.{RelationMetadata, TableMetadata, ViewMetadata}
import _root_.io.teknek.arizona.spark.connector.cql.{CassandraConnector, TableDef}
import _root_.io.teknek.arizona.spark.connector.util._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.expressions.{Expressions, Transform}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.jdk.CollectionConverters._

case class CassandraTable(session: SparkSession, catalogConf: CaseInsensitiveStringMap, connector: CassandraConnector, catalogName: String, metadata: RelationMetadata, optionalSchema: Option[StructType] = None) //Used for adding metadata references

  extends Table
    with SupportsRead
    with SupportsWrite {

  /* Retrieving table def from cassandra may be slow (it implies C* metadata refresh) */
  lazy val tableDef: TableDef = tableFromCassandra(connector, metadata.getKeyspace.asInternal(), name())

  override def schema(): StructType = optionalSchema
    .getOrElse(CassandraSourceUtil.toStructType(metadata))

  override def partitioning(): Array[Transform] = {
    metadata.getPartitionKey.asScala
      .map(_.getName.asInternal())
      .map(Expressions.identity)
      .toArray
  }

  override def properties(): util.Map[String, String] = {
    val clusteringKey = metadata.getClusteringColumns().asScala
      .map { case (col, ord) => s"${col.getName.asInternal}.$ord" }
      .mkString("[", ",", "]")
    (
      Map("clustering_key" -> clusteringKey)
        ++ metadata.getOptions.asScala.map { case (key, value) => key.asInternal() -> value.toString }
      ).asJava
  }

  override def capabilities(): util.Set[TableCapability] = Set(
    TableCapability.ACCEPT_ANY_SCHEMA,
    TableCapability.BATCH_READ,
    TableCapability.BATCH_WRITE,
    TableCapability.STREAMING_WRITE,
    TableCapability.TRUNCATE).asJava


  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    val combinedOptions = (catalogConf.asScala ++ options.asScala).asJava
    CassandraScanBuilder(session, tableDef, catalogName, new CaseInsensitiveStringMap(combinedOptions))
  }

  override def name(): String = metadata.getName.asInternal()

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    val combinedOptions = (catalogConf.asScala ++ info.options.asScala).asJava
    if (metadata.isInstanceOf[ViewMetadata]) {
      throw new UnsupportedOperationException("Writing to a materialized view is not supported")
    }
    CassandraWriteBuilder(session, tableDef, catalogName, new CaseInsensitiveStringMap(combinedOptions), info.schema)
  }
}

