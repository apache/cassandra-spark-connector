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

sealed trait BatchGroupingKey

object BatchGroupingKey {

  /** Any row can be added to any batch. This works the same as previous batching implementation. */
  case object None extends BatchGroupingKey

  /** Each batch is associated with a set of replicas. If a set of replicas for the inserted row is
    * the same as it is for a batch, the row can be added to the batch. */
  case object ReplicaSet extends BatchGroupingKey

  /** Each batch is associated with a partition key. If the partition key of the inserted row is the
    * same as it is for a batch, the row can be added to the batch. */
  case object Partition extends BatchGroupingKey

  def apply(name: String): BatchGroupingKey = name.toLowerCase match {
    case "none" => None
    case "replica_set" => ReplicaSet
    case "partition" => Partition
    case _ => throw new IllegalArgumentException(s"Invalid batch level: $name")
  }
}
