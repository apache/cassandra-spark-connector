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

package com.datastax.spark.connector.rdd

import java.lang.{String => JString}
import java.util.{Map => JMap}

import scala.annotation.meta.param
import scala.language.existentials
import scala.reflect.ClassTag
import org.apache.spark.rdd.{RDD, UnionRDD}
import org.apache.spark.{Partitioner, SparkContext}
import com.datastax.spark.connector.cql.{CassandraConnector, Schema, TableDef}
import com.datastax.spark.connector.rdd.partitioner.dht.{Token, TokenFactory}
import com.datastax.spark.connector.rdd.partitioner.{BucketingRangeIndex, CassandraPartition, TokenGenerator, TokenRangeWithPartitionIndex}
import com.datastax.spark.connector.util.{Logging, schemaFromCassandra}
import com.datastax.spark.connector.writer.RowWriter
import scala.jdk.CollectionConverters._

/**
  * A Java Friendly api for DseGraphUnionedRDD to make it easier to call
  * from VertexInputRDD
  */
object DseGraphUnionedRDD {
  def fromJava[R: ClassTag](
      sc: SparkContext,
      rdds: java.util.List[RDD[R]],
      keyspace: String,
      graphLabels: java.util.List[String])(
      implicit
      connector: CassandraConnector): DseGraphUnionedRDD[R] = {
    val rddSeq: Seq[RDD[R]] = rdds.asScala.toSeq
    val labelSeq: Seq[String] = graphLabels.asScala.toSeq
    new DseGraphUnionedRDD(sc, rddSeq, keyspace, labelSeq)
  }
}

/**
  * An extension of UnionRDD which automatically assigns a partitioner based on the way DseGraph stores and partitions
  * vertex information. The graphLabels should map 1 to 1 with the RDDs provided in sequence. This ordering is used
  * to develop a mapping between labels and the RDDs which represent the data for that label.
  */
class DseGraphUnionedRDD[R: ClassTag](
    sc: SparkContext,
    rdds: Seq[RDD[R]],
    keyspace: String,
    graphLabels: Seq[String])(
    implicit
    connector: CassandraConnector = CassandraConnector(sc.getConf))
    extends UnionRDD[R](sc, rdds) {

  assert(graphLabels.length == rdds.length)

  override val partitioner: Option[Partitioner] = Some(new DseGraphPartitioner(keyspace, rdds, graphLabels))
}


/**
  * A custom partitoner specifically for RDDs made for DseGraph.
  *
  * The general idea is
  *
  * For a vertex
  *    Determine the ~label property
  *    Determine which RDD represents the data in that label
  *    Determine the C* token of the vertex given it's label
  *    Determine which partition in the found RDD would contain that Token
  *    Determine the offset of that RDD's partitions in the UnionRDD
  *    Return the partition index added to offset
  *
  */
class DseGraphPartitioner[V, T <: Token[V]](
    keyspaceName: String,
    @(transient @param) rdds: Seq[RDD[_]],
    graphLabels: Seq[String])(
    implicit
    val connector: CassandraConnector)
    extends Partitioner with Logging {

  type ITR = TokenRangeWithPartitionIndex[V, T]

  /* To avoid a dependency on tinkerpop we explictly set this variable. This may change in Future Tinkerpop */
  val LabelAccessor = "~label"

  /** Spark Conf is not Serializable, use it to get a Connector Conf locally **/
  @transient lazy val conf = rdds.head.sparkContext.getConf

  /** We can only use this locally since we cannot serialize the RDDs **/
  @transient private[connector] val rddToLabel = rdds.zip(graphLabels).toMap

  implicit private val tokenFactory: TokenFactory[V, T] =
    TokenFactory.forSystemLocalPartitioner(connector).asInstanceOf[TokenFactory[V, T]]

  implicit lazy private val tokenOrdering = tokenFactory.tokenOrdering
  implicit lazy private val tokenBucketing = tokenFactory.tokenBucketing

  /**
    * Within the Unioned RDD all of the partitions are offset by their relative position in the
    * RDD Dependency ordering. This will allow us to determine where a Partition belongs given
    * the C* TableName.
    *
    * Serializable
    */
  private[connector] val labelToRddOffset: Map[String, Int] = {
    var cumSum = 0
    val offsets = for (rdd <- rdds) yield {
      val offset = (rddToLabel(rdd), cumSum)
      cumSum += rdd.partitions.length
      offset
    }
    offsets.toMap
  }

  /**
    * All of the partitions in each parent RDD, this is serialized so that we can create our
    * lookup tables remotely
    */
  private[connector] val labelToPartitions: Map[String, Array[CassandraPartition[V, T]]] = {
    rdds.map { case rdd: RDD[_] =>
      (rddToLabel(rdd), rdd.partitions.map { case part: CassandraPartition[V, T] => part })
    }.toMap
  }

  /**
    * Generates a quick lookup data structure for every Vertex Label. These structures and will allow for O(1)
    * lookup of the correct partition for a given Cassandra Token.
    *
    * Our BucketingRangeIndex is not serializable, relies on Partitions being serialized and then building the
    * hashing remotely
    */
  @transient private[connector] lazy val labelToTokenRangeLookup: Map[String, BucketingRangeIndex[ITR, T]] = {
    graphLabels.map { case label =>
      val cassandraPartitions = labelToPartitions(label)
      val indexedTokenRanges = for (p <- cassandraPartitions; tr <- p.tokenRanges) yield {
        TokenRangeWithPartitionIndex(tr.range, p.index)
      }

      (label, new BucketingRangeIndex[ITR, T](indexedTokenRanges))
    }.toMap
  }

  /**
    * Lookup the C* Schemas for every RDD in this Union RDD. These are required to determine the
    * partition key when creating TokenGenerators
    *
    * Table Defs are serializable so we can make this and ship it
    */
  private[connector] val labelToTableDef: Map[String, TableDef] = {
    graphLabels.map { labelName =>
      (labelName,
          (schemaFromCassandra(connector, Some(keyspaceName), Some(s"${labelName}_p")).tables ++
            schemaFromCassandra(connector, Some(keyspaceName), Some(s"${labelName}_e")).tables)
              .headOption
              .getOrElse(
                throw new IllegalArgumentException(s"""Couldn't find table for label ${labelName}""")))
    }.toMap
  }

  /**
    * Creates a HashMap -> C* Token mapper which can translate a given Map representation of a vertex and
    * extract the partition keys required and transform them into a C* token.
    *
    * Must be Made Remotely. Token Generators are not Serializable but only relies on labels
    * and tableDefs, both of which are serializable
    */
  @transient lazy private[connector] val labelToTokenGenerator: Map[String, TokenGenerator[JMap[JString, AnyRef]]] =
    graphLabels.map { graphLabel =>
      val tableDef = labelToTableDef(graphLabel)
      (graphLabel,
          new TokenGenerator(
            connector,
            tableDef,
            new MapRowWriter(tableDef.partitionKey.map(_.columnName))))
    }.toMap


  /**
    * For a given Map based Vertex Key determine which table it belongs to. From this table lookup the correct
    * RDD and the partitioning for that RDD. Once narrowed down generate the Token given the now known table. This
    * token will then be compared against the quick lookup Bucket structure for that RDD. We then add that partition
    * index to the offset for that RDD.
    *
    * Triggers the creation of all lazy transient properties
    */
  override def getPartition(key: Any): Int = key match {
    case vertexId: JMap[JString, AnyRef]@unchecked => {
      val label: String = vertexId.asScala.getOrElse(
        LabelAccessor,
        throw new IllegalArgumentException(s"Couldn't find $LabelAccessor in key $key"))
          .asInstanceOf[String]

      val tokenGenerator = labelToTokenGenerator(label)
      val token = tokenFactory.tokenFromString(tokenGenerator.getStringTokenFor(vertexId))
      val bucketRange = labelToTokenRangeLookup(label)

      val originalPartitionIndex = bucketRange
        .rangesContaining(token)
        .headOption
        .getOrElse(throw new IllegalArgumentException(
          s"Couldn't find $token in for $label"))
        .partitionIndex

      val offSetOfRDD = labelToRddOffset(label)

      originalPartitionIndex + offSetOfRDD
    }
    case x => throw new IllegalArgumentException(s"Unable to partition object $x (${x.getClass})")
  }

  override val numPartitions: Int = rdds.map(_.partitions.length).sum
}

class MapRowWriter(override val columnNames: Seq[String]) extends RowWriter[JMap[JString, AnyRef]] {
  override def readColumnValues(data: JMap[JString, AnyRef], buffer: Array[Any]): Unit =
    columnNames.zipWithIndex.foreach { case (columnName, index) =>
      buffer(index) = data.asScala.getOrElse(columnName,
        throw new IllegalArgumentException(s"""Couldn't find $columnName in $data, unable to generate token"""))
    }
}
