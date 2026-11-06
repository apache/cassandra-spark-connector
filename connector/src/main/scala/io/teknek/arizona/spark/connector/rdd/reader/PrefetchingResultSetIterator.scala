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

package io.teknek.arizona.spark.connector.rdd.reader

import java.util.concurrent.TimeUnit

import com.codahale.metrics.Timer
import _root_.io.teknek.arizona.spark.bdp.ScalaJavaUtil
import com.datastax.oss.driver.api.core.cql.{AsyncResultSet, Row}
import _root_.io.teknek.arizona.spark.connector.util.Threads.BlockingIOExecutionContext

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/** Allows to efficiently iterate over a large, paged ResultSet,
  * asynchronously prefetching the next page.
  *
  * This iterator is NOT thread safe. Attempting to retrieve elements from many threads without synchronization
  * may yield unspecified results.
  *
  * @param resultSet result set obtained from the Java driver
  * @param timer     a Codahale timer to optionally gather the metrics of fetching time
  */
class PrefetchingResultSetIterator(resultSet: AsyncResultSet, timer: Option[Timer] = None) extends Iterator[Row] {
  private var currentIterator = resultSet.currentPage().iterator()
  private var currentResultSet = resultSet
  private var nextResultSet = fetchNextPage()

  private def fetchNextPage(): Option[Future[AsyncResultSet]] = {
    if (currentResultSet.hasMorePages) {
      val t0 = System.nanoTime();
      val next = ScalaJavaUtil.asScalaFuture(currentResultSet.fetchNextPage())
      timer.foreach { t =>
        next.foreach(_ => t.update(System.nanoTime() - t0, TimeUnit.NANOSECONDS))
      }
      Option(next)
    } else
      None
  }

  private def maybePrefetch(): Unit = {
    if (!currentIterator.hasNext && currentResultSet.hasMorePages) {
      currentResultSet = Await.result(nextResultSet.get, Duration.Inf)
      currentIterator = currentResultSet.currentPage().iterator()
      nextResultSet = fetchNextPage()
    }
  }

  override def hasNext: Boolean =
    currentIterator.hasNext || currentResultSet.hasMorePages

  override def next(): Row = {
    val row = currentIterator.next() // let's try to exhaust the current iterator first
    maybePrefetch()
    row
  }
}
