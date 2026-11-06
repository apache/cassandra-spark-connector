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

package io.teknek.arizona.spark.connector.rdd

import org.apache.spark.{TaskContext, Partition}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD

import _root_.io.teknek.arizona.spark.connector.util.SpanningIterator

/**
 * Groups items with the same key, assuming items with the same key are next to each other in
 * the parent collection. Contrary to Spark GroupedRDD, it does not perform shuffle, therefore it
 * is much faster. A key for each item is obtained by calling a given function.
 *
 * This RDD is very useful for grouping data coming out from Cassandra, because they are already
 * coming in order of partitioning key i.e. it is not possible for two rows
 * with the same partition key to be in different Spark partitions.
 *
 * @param parent parent RDD
 * @tparam K type of keys
 * @tparam T type of elements to be grouped together
 */
private[connector] class SpannedRDD[K, T](parent: RDD[T], f: T => K)
  extends RDD[(K, Iterable[T])](parent) {

  override protected def getPartitions = parent.partitions

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext) =
    new SpanningIterator(parent.iterator(split, context), f)

}

