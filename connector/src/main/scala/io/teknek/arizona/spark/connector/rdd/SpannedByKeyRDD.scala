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
 * Similar to [[SpannedRDD]] but, instead of extracting the key by the given function,
 * it groups binary tuples by the first element of each tuple.
 */
private[connector] class SpannedByKeyRDD[K, V](parent: RDD[(K, V)]) extends RDD[(K, Seq[V])](parent) {

  override protected def getPartitions = parent.partitions

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext) = {
    val parentIterator = parent.iterator(split, context)
    def keyFunction(item: (K, V)) = item._1
    def extractValues(group: (K, Seq[(K, V)])) = (group._1, group._2.map(_._2))
    new SpanningIterator(parentIterator, keyFunction).map(extractValues)
  }

}
