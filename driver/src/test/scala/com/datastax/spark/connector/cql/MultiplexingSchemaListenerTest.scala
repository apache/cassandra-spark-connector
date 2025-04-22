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
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar

import scala.collection.mutable
import scala.concurrent.Future
import scala.util.Random

class MultiplexingSchemaListenerTest extends FlatSpec with Matchers with MockitoSugar with BeforeAndAfterEach with Eventually {
  val r = new Random()

  import scala.concurrent.ExecutionContext.Implicits.global

  def count(k: String) = actionsDone.put(k, actionsDone(k) + 1)
  def reset() = actionsDone.clear()

  val listener = new MultiplexingSchemaListener()

  val actionsDone = new mutable.HashMap[String, Int]().withDefaultValue(0)

  override def beforeEach(): Unit = {
    listener.clearListeners()
    actionsDone.clear()
  }

  class IncrementingSchemaListener extends  SchemaChangeListener {
    override def onKeyspaceCreated(keyspace: KeyspaceMetadata): Unit = count("onKeyspaceCreated")
    override def onKeyspaceDropped(keyspace: KeyspaceMetadata): Unit = count("onKeyspaceDropped")
    override def onKeyspaceUpdated(current: KeyspaceMetadata, previous: KeyspaceMetadata): Unit = count("onKeyspaceUpdated")

    override def onTableCreated(table: TableMetadata): Unit = count("onTableCreated")
    override def onTableDropped(table: TableMetadata): Unit = count("onTableDropped")
    override def onTableUpdated(current: TableMetadata, previous: TableMetadata): Unit = count("onTableUpdated")

    override def onUserDefinedTypeCreated(`type`: UserDefinedType): Unit = count("onUserDefinedTypeCreated")
    override def onUserDefinedTypeDropped(`type`: UserDefinedType): Unit = count("onUserDefinedTypeDropped")
    override def onUserDefinedTypeUpdated(current: UserDefinedType, previous: UserDefinedType): Unit = count("onUserDefinedTypeUpdated")

    override def onFunctionCreated(function: FunctionMetadata): Unit = count("onFunctionCreated")
    override def onFunctionDropped(function: FunctionMetadata): Unit = count("onFunctionDropped")
    override def onFunctionUpdated(current: FunctionMetadata, previous: FunctionMetadata): Unit = count("onFunctionUpdated")

    override def onAggregateCreated(aggregate: AggregateMetadata): Unit = count("onAggregateCreated")
    override def onAggregateDropped(aggregate: AggregateMetadata): Unit = count("onAggregateDropped")
    override def onAggregateUpdated(current: AggregateMetadata, previous: AggregateMetadata): Unit = count("onAggregateUpdated")

    override def onViewCreated(view: ViewMetadata): Unit = count("onViewCreated")
    override def onViewDropped(view: ViewMetadata): Unit = count("onViewDropped")
    override def onViewUpdated(current: ViewMetadata, previous: ViewMetadata): Unit = count("onViewUpdated")

    override def close(): Unit = count("close")
  }

  def triggerKeyspaceEvents(times: Int = 1): Unit = {
    val ks = mock[KeyspaceMetadata]
    for (i <- 0 until times) {
      listener.onKeyspaceCreated(ks)
      listener.onKeyspaceDropped(ks)
      listener.onKeyspaceUpdated(ks, ks)
    }
  }

  def triggerTableEvents(times: Int = 1): Unit = {
    val ks = mock[TableMetadata]
    for (i <- 0 until times) {
      listener.onTableCreated(ks)
      listener.onTableDropped(ks)
      listener.onTableUpdated(ks, ks)
    }
  }

  def triggerFunctionEvents(times: Int = 1): Unit = {
    val ks = mock[FunctionMetadata]
    for (i <- 0 until times) {
      listener.onFunctionCreated(ks)
      listener.onFunctionDropped(ks)
      listener.onFunctionUpdated(ks, ks)
    }
  }

  def triggerUserDefinedTypeEvents(times: Int = 1): Unit = {
    val ks = mock[UserDefinedType]
    for (i <- 0 until times) {
      listener.onUserDefinedTypeCreated(ks)
      listener.onUserDefinedTypeDropped(ks)
      listener.onUserDefinedTypeUpdated(ks, ks)
    }
  }

  def triggerAggregateEvents(times: Int = 1): Unit = {
    val ks = mock[AggregateMetadata]
    for (i <- 0 until times) {
      listener.onAggregateCreated(ks)
      listener.onAggregateDropped(ks)
      listener.onAggregateUpdated(ks, ks)
    }
  }

  def triggerViewEvents(times: Int = 1): Unit = {
    val ks = mock[ViewMetadata]
    for (i <- 0 until times) {
      listener.onViewCreated(ks)
      listener.onViewDropped(ks)
      listener.onViewUpdated(ks, ks)
    }
  }

  def triggerCloseEvents(times: Int = 1): Unit = {
    for (i <- 0 until times) {
      listener.close()
    }
  }


  def checkKeyspaceEvents(amount: Int): Unit = {
    checkEvent("Keyspace", amount)
  }
  def checkTableEvents(amount: Int): Unit = {
    checkEvent("Table", amount)
  }
  def checkUserDefinedTypeEvents(amount: Int): Unit = {
    checkEvent("UserDefinedType", amount)
  }
  def checkFunctionEvents(amount: Int): Unit = {
    checkEvent("Function", amount)
  }
  def checkAggregateEvents(amount: Int): Unit = {
    checkEvent("Aggregate", amount)
  }
  def checkViewEvents(amount: Int): Unit = {
    checkEvent("View", amount)
  }
  def checkCloseEvents(amount: Int): Unit = {
    actionsDone("close") shouldBe amount
  }

  def triggerAllEvents(times: Int = 1): Unit = {
    triggerAggregateEvents(times)
    triggerCloseEvents(times)
    triggerFunctionEvents(times)
    triggerKeyspaceEvents(times)
    triggerTableEvents(times)
    triggerUserDefinedTypeEvents(times)
    triggerViewEvents(times)
  }

  def checkAllEvents(amount: Int) = {
    checkAggregateEvents(amount)
    checkCloseEvents(amount)
    checkFunctionEvents(amount)
    checkKeyspaceEvents(amount)
    checkTableEvents(amount)
    checkUserDefinedTypeEvents(amount)
    checkViewEvents(amount)
  }

  private def checkEvent(eventType: String, amount: Int) {
    actionsDone(s"on${eventType}Created") shouldBe (amount)
    actionsDone(s"on${eventType}Dropped") shouldBe (amount)
    actionsDone(s"on${eventType}Updated") shouldBe (amount)
  }


  // Actual tests here

  "MutableSchemaListener" should "allow adding or removing listeners" in {
    val incrementingSchemaListener = new IncrementingSchemaListener()
    listener.addListener(incrementingSchemaListener)
    listener.getListeners().head shouldBe incrementingSchemaListener
    listener.removeListener(incrementingSchemaListener)
    listener.registeredListenerCount() shouldBe (0)
  }


  for (numListener <- 0 to 10) {
    it should s"work with $numListener listeners" in {
      (1 to numListener).foreach(_ => listener.addListener(new IncrementingSchemaListener()))
      var totalEvents = 0
      for (it <- 1 to 100) {
        val eventCount = r.nextInt(25)
        triggerAllEvents(eventCount)
        totalEvents += (eventCount * numListener)
        checkAllEvents(totalEvents)
      }
    }
  }

  it should "allow listeners to be added while triggering events" in {
    for (it <- 1 to 200) {
      Future (listener.addListener(new IncrementingSchemaListener()))
      Future (triggerAllEvents(5))
      Future (listener.addListener(new IncrementingSchemaListener()))
      Future (triggerAllEvents(5))
      if (it % 10 == 0)
        Future (listener.clearListeners())
    }
    eventually {
      actionsDone.values.sum should be > 0
    }
  }

}
