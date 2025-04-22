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

package com.datastax.spark.connector.cql

import com.datastax.oss.driver.api.core.`type`.UserDefinedType
import com.datastax.oss.driver.api.core.metadata.schema.{AggregateMetadata, FunctionMetadata, KeyspaceMetadata, SchemaChangeListener, TableMetadata, ViewMetadata}
import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap
import scala.util.{Failure, Success, Try}

/**
  * Uses an internal TrieMap to collect incoming listeners based on their hashCodes. This means that identical
  * objects cannot be added, if you need to add the same SchemaListener more than once be sure that it is not
  * the identical object.
  */
class MultiplexingSchemaListener() extends SchemaChangeListener {
  val log = LoggerFactory.getLogger(this.getClass)


  private val listeners : TrieMap[Int, SchemaChangeListener] = new TrieMap[Int, SchemaChangeListener]()

  /**
    * Adds a listener. If another listener already exists with the same hashcode it will
    * be overridden by this listener.
    */
  def addListener(listener: SchemaChangeListener): Option[SchemaChangeListener] = {
    listeners.put(listener.hashCode(), listener)
  }

  /**
    * Removes a listener with the same hashcode as the argument listener if it exists
    */
  def removeListener(listener: SchemaChangeListener): Option[SchemaChangeListener] = {
    listeners.remove(listener.hashCode())
  }

  /**
    * Removes all registered listeners
    */
  def clearListeners(): Unit = {
    listeners.clear()
  }

  /**
    * Returns the current number of registered listeners
    */
  def registeredListenerCount(): Int = {
    listeners.size
  }

  implicit private class EnrichedIterable[U](iterable: Iterable[U]) {
    def foreachTry[T](f:U => T) : Unit = iterable.foreach{ u =>
      Try(f(u)) match {
        case Success(_) =>
        case Failure(exception) =>
          log.error("One of the registered listeners threw an exception, skipping")
          exception.printStackTrace()
      }
    }
  }

  /**
    * Returns a read only snapshot representation of current registered listeners. This is thread-safe
    * but may contain references to items which were already removed or be missing elements that were just
    * added.
    */
  def getListeners(): Iterable[SchemaChangeListener] =
    listeners.readOnlySnapshot().values

  override def onKeyspaceCreated(keyspace: KeyspaceMetadata): Unit =
    getListeners.foreachTry(_.onKeyspaceCreated(keyspace))

  override def onKeyspaceDropped(keyspace: KeyspaceMetadata): Unit =
    getListeners.foreachTry(_.onKeyspaceDropped(keyspace))

  override def onKeyspaceUpdated(current: KeyspaceMetadata, previous: KeyspaceMetadata): Unit =
    getListeners.foreachTry(_.onKeyspaceUpdated(current, previous))

  override def onTableCreated(table: TableMetadata): Unit =
    getListeners.foreachTry(_.onTableCreated(table))

  override def onTableDropped(table: TableMetadata): Unit =
    getListeners.foreachTry(_.onTableDropped(table))

  override def onTableUpdated(current: TableMetadata, previous: TableMetadata): Unit =
    getListeners.foreachTry(_.onTableUpdated(current, previous))

  override def onUserDefinedTypeCreated(`type`: UserDefinedType): Unit =
    getListeners.foreachTry(_.onUserDefinedTypeCreated(`type`))

  override def onUserDefinedTypeDropped(`type`: UserDefinedType): Unit =
    getListeners.foreachTry(_.onUserDefinedTypeDropped(`type`))

  override def onUserDefinedTypeUpdated(current: UserDefinedType, previous: UserDefinedType): Unit =
    getListeners.foreachTry(_.onUserDefinedTypeUpdated(current, previous))

  override def onFunctionCreated(function: FunctionMetadata): Unit =
    getListeners.foreachTry(_.onFunctionCreated(function))

  override def onFunctionDropped(function: FunctionMetadata): Unit =
    getListeners.foreachTry(_.onFunctionDropped(function))

  override def onFunctionUpdated(current: FunctionMetadata, previous: FunctionMetadata): Unit =
    getListeners.foreachTry(_.onFunctionUpdated(current, previous))

  override def onAggregateCreated(aggregate: AggregateMetadata): Unit =
    getListeners.foreachTry(_.onAggregateCreated(aggregate))

  override def onAggregateDropped(aggregate: AggregateMetadata): Unit =
    getListeners.foreachTry(_.onAggregateDropped(aggregate))

  override def onAggregateUpdated(current: AggregateMetadata, previous: AggregateMetadata): Unit =
    getListeners.foreachTry(_.onAggregateUpdated(current, previous))

  override def onViewCreated(view: ViewMetadata): Unit =
    getListeners.foreachTry(_.onViewCreated(view))

  override def onViewDropped(view: ViewMetadata): Unit =
    getListeners.foreachTry(_.onViewDropped(view))

  override def onViewUpdated(current: ViewMetadata, previous: ViewMetadata): Unit =
    getListeners.foreachTry(_.onViewUpdated(current, previous))

  override def close(): Unit =
    getListeners.foreachTry(_.close())
}