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

package com.datastax.spark.connector.datasource

import java.nio.ByteBuffer

import com.datastax.driver.core.MetadataHook
import com.datastax.spark.connector.cql.{QueryUtils, TableDef}
import com.datastax.spark.connector.datasource.ScanHelper.CqlQueryParts
import com.datastax.spark.connector.rdd.partitioner.dht.{Token, TokenFactory}
import com.datastax.spark.connector.rdd.partitioner.{BucketingRangeIndex, CassandraPartition, CqlTokenRange, RangeBounds}
import com.datastax.spark.connector.types._
import com.datastax.spark.connector.util.CqlWhereParser.{SingleColumnPredicate, UnknownPredicate}
import com.datastax.spark.connector.util.PatitionKeyTools.prepareDummyStatement
import com.datastax.spark.connector.util.{CqlWhereParser, Logging}
import org.apache.spark.SparkConf
import org.apache.spark.sql.cassandra.CassandraSourceRelation.{RuntimeFilteringEnabledParam, RuntimeFilteringMaxKeyLookupsParam}
import org.apache.spark.sql.cassandra.SolrConstants
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.types.StructType

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

/** A Spark partition that reads the given partition keys (values in partition key column order) with one query
  * per key, planned by runtime filtering. `endpoints` are the replicas of the keys' token ranges. */
case class CassandraKeyLookupPartition(index: Int, endpoints: Array[String], keys: Array[Array[AnyRef]])
  extends InputPartition {

  override def preferredLocations(): Array[String] = endpoints
}

/** Plans the partitions of a [[CassandraScan]] after Spark filtered it at runtime on its partition key. */
private[connector] object RuntimeFilterPlanner extends Logging {

  /** Partition key types whose Spark equality matches Cassandra's: equal Spark values serialize to the same
    * Cassandra partition key. Not decimal (1.5 and 1.50 are different Cassandra keys), float and double (-0.0 and
    * 0.0, NaN), nor collections, tuples and UDTs. */
  val SupportedKeyTypes: Set[ColumnType[_]] = Set(
    AsciiType, TextType, VarCharType, IntType, BigIntType, SmallIntType, TinyIntType, VarIntType, BooleanType,
    TimestampType, DateType, TimeType, UUIDType, TimeUUIDType, InetType, BlobType)

  /** With more keys than this per token range on average, pruning token ranges is not worth it: nearly every
    * range holds a key then (a fraction of about 1 - e^(-keys/ranges) of the ranges). */
  val MaxKeysPerTokenRangeForPruning = 3

  /** The partition key columns Spark may filter the scan by at runtime, or none if runtime filtering does not
    * apply to it. */
  def runtimeFilterColumns(
    tableDef: TableDef,
    queryParts: CqlQueryParts,
    readSchema: StructType,
    conf: SparkConf): Seq[String] = {

    val partitionKey = tableDef.partitionKey
    val keyNames = partitionKey.map(_.columnName)
    lazy val wherePredicates = queryParts.whereClause.predicates.flatMap(CqlWhereParser.parse)
    lazy val readColumns = Option(readSchema).map(_.fieldNames.toSet).getOrElse(Set.empty[String])

    val applies =
      conf.getBoolean(RuntimeFilteringEnabledParam.name, RuntimeFilteringEnabledParam.default) &&
        conf.getOption("pushdown").getOrElse("true").toBoolean &&
        partitionKey.nonEmpty &&
        partitionKey.forall(c => SupportedKeyTypes.contains(c.columnType)) &&
        // Spark can only filter by columns the scan returns
        keyNames.forall(readColumns.contains) &&
        // The scan already restricts the partition key (or uses Solr); keep it as planned
        wherePredicates.forall {
          case p: SingleColumnPredicate =>
            !keyNames.contains(p.columnName) && p.columnName != SolrConstants.SolrQuery
          case _: UnknownPredicate => false
          case _ => true
        }

    if (applies) keyNames else Seq.empty
  }

  /** Values of one column present in both `a` and `b`. */
  def intersect(a: Seq[Any], b: Seq[Any]): Seq[Any] = {
    val inB = b.map(normalized).toSet
    a.filter(v => inB.contains(normalized(v)))
  }

  /** Arrays (blobs) compare by reference; compare their content. */
  private def normalized(v: Any): Any = v match {
    case bytes: Array[Byte] => ByteBuffer.wrap(bytes)
    case other => other
  }

  /** Partitions for `keyValues` (runtime filter values per partition key column), or the original `partitions`
    * if the filter does not restrict the scan enough to be worth it. Never returns fewer rows than needed: when in
    * doubt it keeps the original partitions. */
  def plan(
    scan: CassandraScan,
    partitions: Array[InputPartition],
    keyValues: Map[String, Seq[Any]]): Array[InputPartition] = {

    val table = s"${scan.tableDef.keyspaceName}.${scan.tableDef.tableName}"
    val keyNames = scan.tableDef.partitionKey.map(_.columnName)
    if (!keyNames.forall(keyValues.contains)) {
      logDebug(s"Runtime filter on $table does not restrict every partition key column, scanning all token ranges")
      return partitions
    }

    // A null never equals a key
    val valueSets = keyNames.map(c => keyValues(c).filter(_ != null).distinctBy(normalized))
    val keyCount = valueSets.map(_.size.toLong).product
    val maxKeyLookups = scan.consolidatedConf.getInt(
      RuntimeFilteringMaxKeyLookupsParam.name, RuntimeFilteringMaxKeyLookupsParam.default).toLong
    val cassandraPartitions = partitions.collect { case p: CassandraPartition[_, _] => p }
    val rangeCount = cassandraPartitions.map(_.tokenRanges.size.toLong).sum
    val maxKeysForPruning = MaxKeysPerTokenRangeForPruning * rangeCount

    if (keyCount == 0) {
      logInfo(s"Runtime filter on $table matches no partition key, reading nothing (was ${partitions.length} partitions)")
      Array.empty
    } else if (cassandraPartitions.length != partitions.length) {
      partitions
    } else if (keyCount > math.max(maxKeyLookups, maxKeysForPruning)) {
      logInfo(s"Runtime filter on $table has $keyCount partition keys for $rangeCount token ranges, " +
        s"scanning all of them")
      partitions
    } else {
      try {
        val tf = TokenFactory.forSystemLocalPartitioner(scan.connector).asInstanceOf[TokenFactory[Any, Token[Any]]]
        val typed = cassandraPartitions.map(_.asInstanceOf[CassandraPartition[Any, Token[Any]]])
        val planned = planWithTokens[Any, Token[Any]](scan, typed, valueSets, maxKeyLookups, maxKeysForPruning)(tf)
        logInfo(s"Runtime filter on $table: $keyCount partition keys, ${partitions.length} -> ${planned.length} " +
          s"partitions (${planned.headOption.map(_.getClass.getSimpleName).getOrElse("none")})")
        planned
      } catch {
        case NonFatal(e) =>
          logWarning(s"Could not apply the runtime filter on $table, scanning all token ranges", e)
          partitions
      }
    }
  }

  private case class IndexedRange[V, T <: Token[V]](range: CqlTokenRange[V, T], partition: Int, position: Int)

  private def planWithTokens[V, T <: Token[V]](
    scan: CassandraScan,
    partitions: Array[CassandraPartition[V, T]],
    valueSets: Seq[Seq[Any]],
    maxKeyLookups: Long,
    maxKeysForPruning: Long)(implicit tf: TokenFactory[V, T]): Array[InputPartition] = {

    implicit val bounds: RangeBounds[IndexedRange[V, T], T] = new RangeBounds[IndexedRange[V, T], T] {
      override def start(r: IndexedRange[V, T]): T = r.range.range.start
      override def end(r: IndexedRange[V, T]): T = r.range.range.end
      override def isFull(r: IndexedRange[V, T]): Boolean = r.range.range.isFull
      override def contains(r: IndexedRange[V, T], token: T): Boolean = r.range.range.contains(token)
    }
    implicit val ordering: Ordering[T] = tf.tokenOrdering
    implicit val bucketing = tf.tokenBucketing

    val ranges = for {
      (p, partitionIndex) <- partitions.toSeq.zipWithIndex
      (r, position) <- p.tokenRanges.toSeq.zipWithIndex
    } yield IndexedRange(r, partitionIndex, position)
    val rangeIndex = new BucketingRangeIndex[IndexedRange[V, T], T](ranges)

    // The range of every distinct key, by its serialized partition key: Spark values that convert to the same
    // Cassandra key (e.g. timestamps differing in microseconds) must be read once, or rows would repeat.
    val keysByRoutingKey = mutable.LinkedHashMap.empty[ByteBuffer, (Array[AnyRef], IndexedRange[V, T])]
    scan.connector.withSessionDo { session =>
      val stmt = prepareDummyStatement(session, scan.tableDef)
      val converters = stmt.getVariableDefinitions.asScala.map(v => ColumnType.converterToCassandra(v.getType)).toArray
      val metadata = session.getMetadata
      for (key <- InClauseKeyGenerator.cross(valueSets)) {
        val values = key.map(_.asInstanceOf[AnyRef]).toArray
        val bound = stmt.bind(values.zip(converters).map { case (v, c) => c.convert(v).asInstanceOf[AnyRef] }: _*)
        val routingKey = QueryUtils.getRoutingKeyOrError(bound)
        if (!keysByRoutingKey.contains(routingKey)) {
          val token = tf.tokenFromString(MetadataHook.newTokenAsString(metadata, routingKey))
          val range = rangeIndex.rangesContaining(token).headOption.getOrElse(
            throw new IllegalStateException(s"No token range of the scan contains token $token"))
          keysByRoutingKey.put(routingKey, (values, range))
        }
      }
    }

    val keyCount = keysByRoutingKey.size
    if (keyCount <= maxKeyLookups) {
      // One partition per original partition holding keys, on the same replicas; split big ones so the lookups
      // keep the scan's parallelism
      val maxKeysPerPartition = math.max(1, math.ceil(keyCount.toDouble / partitions.length).toInt)
      val lookups = keysByRoutingKey.values.toSeq.groupBy(_._2.partition).toSeq.sortBy(_._1).flatMap {
        case (partitionIndex, keys) =>
          keys.map(_._1).grouped(maxKeysPerPartition).map(chunk => (partitions(partitionIndex).endpoints, chunk))
      }
      lookups.zipWithIndex.map { case ((endpoints, keys), index) =>
        CassandraKeyLookupPartition(index, endpoints, keys.toArray): InputPartition
      }.toArray
    } else if (keyCount <= maxKeysForPruning) {
      val keptRanges = keysByRoutingKey.values.map { case (_, r) => (r.partition, r.position) }.toSet
      partitions.zipWithIndex.flatMap { case (p, partitionIndex) =>
        val kept = p.tokenRanges.toSeq.zipWithIndex.collect {
          case (r, position) if keptRanges.contains((partitionIndex, position)) => r
        }
        if (kept.isEmpty) None else Some(p.copy(tokenRanges = kept))
      }.zipWithIndex.map { case (p, index) => p.copy(index = index): InputPartition }
    } else {
      partitions.map(p => p: InputPartition)
    }
  }
}
