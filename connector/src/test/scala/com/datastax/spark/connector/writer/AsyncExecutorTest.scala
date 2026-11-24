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

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Callable, CompletableFuture, CompletionStage, Executors, TimeUnit}

import org.junit.Assert._
import org.junit.Test
import org.scalatest.Matchers._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}

class AsyncExecutorTest {

  @Test
  def test() {
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

  /** Completes every task on a separate thread after a short delay, like the driver's I/O threads do. */
  private def completedLater[A](result: Either[Throwable, A]): CompletionStage[A] = {
    val f = new CompletableFuture[A]()
    AsyncExecutorTest.ioThread.schedule(new Runnable {
      override def run(): Unit = result.fold(f.completeExceptionally, f.complete)
    }, 5, TimeUnit.MILLISECONDS)
    f
  }

  // The success handler updates the output metrics of a write (TableWriter). The task must not finish before
  // every handler has run, otherwise Spark collects the metrics too early and the stage under-reports rows.
  @Test
  def testSuccessHandlersHaveRunWhenWaitReturns() {
    for (_ <- 1 to 10) {
      val handled = new AtomicInteger()
      val executor = new AsyncExecutor[Int, String](
        _ => completedLater(Right("ok")), 4,
        Some((_, _, _) => { Thread.sleep(20); handled.incrementAndGet() }), None)
      (1 to 3).foreach(executor.executeAsync)
      executor.waitForCurrentlyExecutingTasks()
      handled.get() shouldBe 3
    }
  }

  @Test
  def testFailureHandlersHaveRunWhenWaitReturns() {
    for (_ <- 1 to 10) {
      val handled = new AtomicInteger()
      val executor = new AsyncExecutor[Int, String](
        _ => completedLater(Left(new IllegalStateException("write failed"))), 4,
        None, Some((_, _, _) => { Thread.sleep(20); handled.incrementAndGet() }))
      (1 to 3).foreach(executor.executeAsync)
      executor.waitForCurrentlyExecutingTasks()
      handled.get() shouldBe 3
      executor.getLatestException().isDefined shouldBe true
    }
  }

  // A failing handler must neither leak a permit nor leave its future incomplete.
  @Test
  def testThrowingHandlerDoesNotBlockTheExecutor() {
    import ExecutionContext.Implicits.global
    val executor = new AsyncExecutor[Int, String](
      _ => completedLater(Right("ok")), 1,
      Some((_, _, _) => throw new RuntimeException("handler failed")), None)
    val all = Future { (1 to 5).map(executor.executeAsync).foreach(f => Await.result(f, 10.seconds)) }
    Await.result(all, 20.seconds)
    executor.waitForCurrentlyExecutingTasks()
  }

  @Test
  def testGracefullyHandleCqlSessionExecuteExceptions() {
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
}

object AsyncExecutorTest {
  private val ioThread = Executors.newSingleThreadScheduledExecutor()
}
