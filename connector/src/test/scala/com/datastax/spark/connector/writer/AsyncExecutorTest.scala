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

import com.datastax.oss.driver.api.core.cql.{AsyncResultSet, SimpleStatement, Statement}
import com.datastax.oss.driver.api.core.{AllNodesFailedException, NodeUnavailableException}
import com.datastax.oss.driver.api.core.connection.BusyConnectionException
import com.datastax.oss.driver.api.core.metadata.Node
import com.datastax.oss.driver.api.core.servererrors.OverloadedException

import java.util
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Callable, CompletableFuture, CompletionStage}

import org.junit.Assert._
import org.junit.Test
import org.mockito.Mockito._
import org.scalatest.Matchers._

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

class AsyncExecutorTest {

  @Test
  def test(): Unit = {
    val taskCount = 20
    val maxParallel = 5

    val currentlyRunningCounter = new AtomicInteger(0)
    val maxParallelCounter = new AtomicInteger(0)
    val totalFinishedExecutionsCounter = new AtomicInteger(0)

    val task = new Callable[String] {
      override def call() = {
        val c = currentlyRunningCounter.incrementAndGet()
        var m = maxParallelCounter.get()
        while (m < c && !maxParallelCounter.compareAndSet(m, c))
          m = maxParallelCounter.get()
        Thread.sleep(100)
        currentlyRunningCounter.decrementAndGet()
        totalFinishedExecutionsCounter.incrementAndGet()
        "ok"
      }
    }

    def execute(callable: Callable[String]): CompletionStage[String] = {
      import ExecutionContext.Implicits.global
      val completableFuture = new CompletableFuture[String]()
      Future { callable.call() }.onComplete {
        case Success(str) => completableFuture.complete(str)
        case Failure(exception) => completableFuture.completeExceptionally(exception)
      }
      completableFuture
    }

    val asyncExecutor = new AsyncExecutor[Callable[String], String](execute, maxParallel, None, None)

    for (i <- 1 to taskCount)
      asyncExecutor.executeAsync(task)

    asyncExecutor.waitForCurrentlyExecutingTasks()

    maxParallelCounter.get() should be <= maxParallel
    totalFinishedExecutionsCounter.get() shouldBe taskCount
    asyncExecutor.getLatestException() shouldBe None
  }

  @Test
  def testGracefullyHandleCqlSessionExecuteExceptions(): Unit = {
    val executor = new AsyncExecutor[Statement[_], AsyncResultSet](
      _ => {
        // simulate exception returned by session.executeAsync() (not future)
        throw new IllegalStateException("something bad happened")
      }, 10, None, None
    )
    val stmt = SimpleStatement.newInstance("INSERT INTO table1 (key, value) VALUES (1, '100')");
    val future = executor.executeAsync(stmt)
    assertTrue(future.isCompleted)
    val value = future.value.get
    assertTrue(value.isInstanceOf[Failure[_]])
    assertTrue(value.asInstanceOf[Failure[_]].exception.isInstanceOf[IllegalStateException])
  }

  private def mockNode: Node = {
    val node = mock(classOf[Node])
    when(node.toString).thenReturn("Node(test)")
    node
  }

  private def allNodesFailedWith(error: Throwable): AllNodesFailedException = {
    val node = mockNode
    val errors = new util.ArrayList[util.Map.Entry[Node, Throwable]]()
    errors.add(new util.AbstractMap.SimpleEntry(node, error))
    AllNodesFailedException.fromErrors(errors)
  }

  @Test
  def testRetryOnNodeUnavailableException(): Unit = {
    val attempts = new AtomicInteger(0)
    val node = mockNode
    val executor = new AsyncExecutor[String, String](
      _ => {
        val future = new CompletableFuture[String]()
        if (attempts.incrementAndGet() <= 2) {
          future.completeExceptionally(allNodesFailedWith(new NodeUnavailableException(node)))
        } else {
          future.complete("ok")
        }
        future
      }, 10, None, None, maxRetries = 5
    )
    val result = Await.result(executor.executeAsync("task"), 30.seconds)
    result shouldBe "ok"
    attempts.get() shouldBe 3
  }

  @Test
  def testRetryOnBusyConnectionException(): Unit = {
    val attempts = new AtomicInteger(0)
    val executor = new AsyncExecutor[String, String](
      _ => {
        val future = new CompletableFuture[String]()
        if (attempts.incrementAndGet() <= 2) {
          future.completeExceptionally(
            allNodesFailedWith(new BusyConnectionException(100)))
        } else {
          future.complete("ok")
        }
        future
      }, 10, None, None, maxRetries = 5
    )
    val result = Await.result(executor.executeAsync("task"), 30.seconds)
    result shouldBe "ok"
    attempts.get() shouldBe 3
  }

  @Test
  def testRetryOnOverloadedException(): Unit = {
    val attempts = new AtomicInteger(0)
    val node = mockNode
    val executor = new AsyncExecutor[String, String](
      _ => {
        val future = new CompletableFuture[String]()
        if (attempts.incrementAndGet() <= 1) {
          future.completeExceptionally(new OverloadedException(node, "overloaded"))
        } else {
          future.complete("ok")
        }
        future
      }, 10, None, None, maxRetries = 5
    )
    val result = Await.result(executor.executeAsync("task"), 30.seconds)
    result shouldBe "ok"
    attempts.get() shouldBe 2
  }

  @Test
  def testRetriesExhausted(): Unit = {
    val attempts = new AtomicInteger(0)
    val node = mockNode
    val maxRetries = 3
    val executor = new AsyncExecutor[String, String](
      _ => {
        val future = new CompletableFuture[String]()
        attempts.incrementAndGet()
        future.completeExceptionally(allNodesFailedWith(new NodeUnavailableException(node)))
        future
      }, 10, None, None, maxRetries = maxRetries
    )
    val future = executor.executeAsync("task")
    val result = Await.ready(future, 30.seconds).value.get
    assertTrue(result.isFailure)
    assertTrue(result.failed.get.isInstanceOf[AllNodesFailedException])
    // 1 initial + maxRetries retries
    attempts.get() shouldBe (maxRetries + 1)
  }
}
