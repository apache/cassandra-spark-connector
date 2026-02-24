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

package com.datastax.spark.connector.rdd.partitioner

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, Partitioner, TaskContext}

import scala.reflect.ClassTag

/**
 * RDD created by repartitionByCassandraReplica with preferred locations mapping to the CassandraReplicas
 * each partition was created for.
 */
class CassandraPartitionedRDD[T](
    prev: RDD[T],
    keyspace:String,
    table:String)(
  implicit
    ct: ClassTag[T])
  extends RDD[T](prev) {

  // We aren't going to change the data
  override def compute(split: Partition, context: TaskContext): Iterator[T] =
    prev.iterator(split, context)

  @transient
  override val partitioner: Option[Partitioner] = prev.partitioner

  private val replicaPartitioner: ReplicaPartitioner[_] =
    partitioner match {
      case Some(rp: ReplicaPartitioner[_]) => rp
      case other => throw new IllegalArgumentException(
        s"""CassandraPartitionedRDD hasn't been
           |partitioned by ReplicaPartitioner. Unable to do any work with data locality.
           |Found: $other""".stripMargin)
    }

  private lazy val nodeAddresses = new NodeAddresses(replicaPartitioner.connector)

  override def getPartitions: Array[Partition] =
    prev.partitions.map(partition => replicaPartitioner.getEndpointPartition(partition))

  override def getPreferredLocations(split: Partition): Seq[String] = split match {
    case epp: ReplicaPartition =>
      epp.endpoints.toIndexedSeq
    case other: Partition => throw new IllegalArgumentException(
      "CassandraPartitionedRDD doesn't have Endpointed Partitions. PreferredLocations cannot be" +
        "deterimined")
  }
}
