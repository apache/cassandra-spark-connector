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

import java.util.concurrent.{CompletableFuture, CompletionStage, ScheduledThreadPoolExecutor, Semaphore, ThreadFactory}
import java.util.function.BiConsumer
import com.datastax.spark.connector.util.Logging

import scala.jdk.CollectionConverters._
import scala.collection.concurrent.TrieMap
import scala.util.Try
import scala.util.control.NonFatal
import AsyncExecutor.Handler
import com.datastax.oss.driver.api.core.{AllNodesFailedException, NoNodeAvailableException, NodeUnavailableException}
import com.datastax.oss.driver.api.core.connection.BusyConnectionException
import com.datastax.oss.driver.api.core.servererrors.OverloadedException

import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}

/** Asynchronously executes tasks but blocks if the limit of unfinished tasks is reached. */
class AsyncExecutor[T, R](asyncAction: T => CompletionStage[R], maxConcurrentTasks: Int,
                          successHandler: Option[Handler[T]] = None, failureHandler: Option[Handler[T]]) extends Logging {

  private val semaphore = new Semaphore(maxConcurrentTasks)
  private val pendingFutures = new TrieMap[Future[R], Boolean]

  @volatile private var latestException: Option[Throwable] = None

  /** Returns an exception if any of the futures had an exception.
    * Returning None means that no exceptions have been thrown.
    */
  def getLatestException(): Option[Throwable] = latestException

  /** Executes task asynchronously or blocks if more than `maxConcurrentTasks` limit is reached */
  def executeAsync(task: T): Future[R] = {
    val submissionTimestamp = System.nanoTime()
    semaphore.acquire()

    val promise = Promise[R]()
    pendingFutures.put(promise.future, true)

    val executionTimestamp = System.nanoTime()

    def tryFuture(): Future[R] = {
      val value = Try(asyncAction(task)) recover {
        case e =>
          val future = new CompletableFuture[R]()
          future.completeExceptionally(e)
          future
      }

      value.get.whenComplete(new BiConsumer[R, Throwable] {
        private def release() {
          semaphore.release()
          pendingFutures.remove(promise.future)
        }

        // The handler runs before the future completes and leaves pendingFutures, so whoever waits for the future
        // (e.g. TableWriter via waitForCurrentlyExecutingTasks) also sees the handler's effects, such as the
        // output metrics of the batch. Otherwise a task could end before its last batch was counted.
        private def runHandlerThenComplete(handler: Option[Handler[T]])(complete: => Unit) {
          try {
            handler.foreach(_ (task, submissionTimestamp, executionTimestamp))
          } catch {
            case NonFatal(e) => logError("Handler failed for: " + task, e)
          } finally {
            release()
            complete
          }
        }

        private def onSuccess(result: R) {
          runHandlerThenComplete(successHandler)(promise.success(result))
        }

        private def onFailure(throwable: Throwable) {
          if (AsyncExecutor.isRetryable(throwable)) {
            logTrace(s"Retrying after ${throwable.getClass.getSimpleName}: $task")
            // The driver often fails such requests synchronously, inside executeAsync. Retrying on this stack would
            // recurse and spin for as long as the pool stays busy, so retry a little later on another thread.
            AsyncExecutor.retryLater(() => tryFuture())
          } else {
            logError("Failed to execute: " + task, throwable)
            latestException = Some(throwable)
            runHandlerThenComplete(failureHandler)(promise.failure(throwable))
          }
        }

        override def accept(r: R, t: Throwable): Unit = {
          Option(t).foreach(onFailure)
          Option(r).foreach(onSuccess)
        }
      })

      promise.future
    }

    tryFuture()
  }

  def execute(task: T): R = {
    Await.result(executeAsync(task), Duration(20, SECONDS))
  }


    /** Waits until the tasks being currently executed get completed.
    * It will not wait for tasks scheduled for execution during this method call,
    * nor tasks for which the [[executeAsync]] method did not complete. */
  def waitForCurrentlyExecutingTasks() {
    for ((future, _) <- pendingFutures.snapshot())
      Try(Await.result(future, Duration.Inf))
  }
}

object AsyncExecutor {
  type Handler[T] = (T, Long, Long) => Unit

  private val RetryDelayMillis = 1L

  private lazy val retryScheduler = new ScheduledThreadPoolExecutor(1, new ThreadFactory {
    override def newThread(r: Runnable): Thread = {
      val thread = new Thread(r, "spark-cassandra-connector-async-executor-retry")
      thread.setDaemon(true)
      thread
    }
  })

  private def retryLater(retry: () => Unit): Unit =
    retryScheduler.schedule(new Runnable { override def run(): Unit = retry() }, RetryDelayMillis, MILLISECONDS)

  /** True for failures that mean "no capacity right now" rather than "this request failed".
    *
    * Since Java driver 4.14 (JAVA-2959) a node whose connections have no free stream id is reported as a
    * NodeUnavailableException inside an AllNodesFailedException; older drivers threw NoNodeAvailableException.
    * Only an AllNodesFailedException made solely of such errors is retried, so a real error from any node still
    * fails the request. */
  private[writer] def isRetryable(throwable: Throwable): Boolean = throwable match {
    case _: NoNodeAvailableException => true
    case _: OverloadedException => true
    case e: AllNodesFailedException =>
      val errors = e.getAllErrors.asScala.values.flatMap(_.asScala)
      errors.exists(_.isInstanceOf[BusyConnectionException]) ||
        (errors.nonEmpty && errors.forall(_.isInstanceOf[NodeUnavailableException]))
    case _ => false
  }
}
