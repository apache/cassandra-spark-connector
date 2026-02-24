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

package com.datastax.spark.connector.writer

import com.datastax.spark.connector.{BatchSize, BytesInBatch, RowsInBatch}

import scala.collection.mutable.ArrayBuffer

/** A simple wrapper over a collection of bound statements. */
private[writer] sealed trait Batch extends Ordered[Batch] {
  protected[Batch] val buf: ArrayBuffer[RichBoundStatementWrapper]
  protected[Batch] var _bytesCount = 0
  
  /** Returns `true` if the element has been successfully added. Returns `false` if the element
    * cannot be added because adding it would violate the size limitation. If `force` is set to `true`,
    * it adds the item regardless of size limitations and always returns `true`. */
  def add(stmt: RichBoundStatementWrapper, force: Boolean = false): Boolean

  /** Collected statements */
  def statements: Seq[RichBoundStatementWrapper] = buf.toSeq

  /** Only for internal use - batches are compared by this value. */
  protected[Batch] def size: Int

  override def compare(that: Batch): Int = size.compareTo(that.size)

  /** Removes all the collected statements and resets this batch to the initial state. */
  def clear(): Unit = {
    _bytesCount = 0
    buf.clear()
  }

  /** Returns bytes count of the batch */
  def bytesCount: Int = _bytesCount
}

private[writer] object Batch {

  implicit val batchOrdering: Ordering[Batch] = Ordering.ordered[Batch]

  def apply(batchSize: BatchSize): Batch = {
    batchSize match {
      case RowsInBatch(rows) => new RowLimitedBatch(rows)
      case BytesInBatch(bytes) => new SizeLimitedBatch(bytes)
    }
  }
}

/** The implementation which uses the number of items as a size constraint. */
private[writer] class RowLimitedBatch(val maxRows: Int) extends Batch {
  override protected[writer] val buf = new ArrayBuffer[RichBoundStatementWrapper](maxRows)

  override def add(stmt: RichBoundStatementWrapper, force: Boolean = false): Boolean = {
    if (!force && buf.size >= maxRows) {
      false
    } else {
      buf += stmt
      _bytesCount += stmt.bytesCount
      true
    }
  }

  override def size = buf.size

}

/** The implementation which uses length in bytes as a size constraint. */
private[writer] class SizeLimitedBatch(val maxBytes: Int) extends Batch {
  override protected[writer] val buf = new ArrayBuffer[RichBoundStatementWrapper](10)

  override def add(stmt: RichBoundStatementWrapper, force: Boolean = false): Boolean = {
    // buf.nonEmpty here is to allow adding at least a single statement regardless its size
    if (!force && (_bytesCount + stmt.bytesCount) > maxBytes) {
      false
    } else {
      buf += stmt
      _bytesCount += stmt.bytesCount
      true
    }
  }

  override def size = _bytesCount

}
