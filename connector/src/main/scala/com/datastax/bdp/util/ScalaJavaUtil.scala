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


package com.datastax.bdp.util

import java.time.{Duration => JavaDuration}
import java.util.concurrent.{Callable, CompletionStage}
import java.util.function
import java.util.function.{BiConsumer, Consumer, Predicate, Supplier}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future, Promise}
import scala.concurrent.duration.{Duration => ScalaDuration}
import scala.language.implicitConversions

object ScalaJavaUtil {

  implicit class JavaDurationWrapper(val duration: JavaDuration) extends AnyVal {
    def asScalaDuration: ScalaDuration = ScalaDuration.fromNanos(duration.toNanos).toCoarsest
  }

  implicit class ScalaDurationWrapper(val duration: ScalaDuration) extends AnyVal {
    def asJavaDuration: JavaDuration = JavaDuration.ofNanos(duration.toNanos)
  }

  implicit def asJavaCallable[T](f: () => T): Callable[T] = new Callable[T] {
    override def call(): T = f.apply()
  }

  implicit def asJavaPredicate[T](f: T => Boolean): Predicate[T] = new Predicate[T] {
    override def test(t: T): Boolean = f.apply(t)
  }

  implicit def asJavaConsumer[T](f: T => Unit): Consumer[T] = new Consumer[T] {
    override def accept(t: T): Unit = f(t)
  }

  implicit def asJavaSupplier[T](f: () => T): Supplier[T] = new Supplier[T] {
    override def get(): T = f()
  }

  implicit def asJavaFunction[T, R](f: T => R): function.Function[T, R] = new function.Function[T, R] {
    override def apply(t: T): R = f(t)
  }

  def asScalaFunction[T, R](f: java.util.function.Function[T, R]): T => R = x => f(x)

  def asScalaFuture[T](completionStage: CompletionStage[T])
                      (implicit context: ExecutionContextExecutor): Future[T] = {
    val promise = Promise[T]()
    completionStage.whenCompleteAsync(new BiConsumer[T, java.lang.Throwable] {
      override def accept(t: T, throwable: Throwable): Unit = {
        if (throwable == null)
          promise.success(t)
        else
          promise.failure(throwable)

      }
    }, context)
    promise.future
  }
}
