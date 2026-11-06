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

package io.teknek.arizona.spark.connector.util

import scala.language.implicitConversions

object MagicalTypeTricks {

  trait DoesntHaveImplicit[A, B]
  implicit def doesntHaveImplicit[A, B]: A DoesntHaveImplicit B = null
  implicit def doesntHaveImplicitAmbiguity1[A, B](implicit ev: B): A DoesntHaveImplicit B = null
  implicit def doesntHaveImplicitAmbiguity2[A, B](implicit ev: B): A DoesntHaveImplicit B = null

  trait IsNotEqualTo[A, B]
  implicit def neq[A, B]: A IsNotEqualTo B = null
  implicit def neqAmbiguity1[A]: A IsNotEqualTo A = null
  implicit def neqAmbiguity2[A]: A IsNotEqualTo A = null

  trait IsNotSubclassOf[A, B]
  implicit def nsub[A, B]: A IsNotSubclassOf B = null
  implicit def nsubAmbiguity1[A, B >: A]: A IsNotSubclassOf B = null
  implicit def nsubAmbiguity2[A, B >: A]: A IsNotSubclassOf B = null

  type ¬[A] = A => Nothing
  type λ[A] = ¬[¬[A]]

  /**
   * Example of how disjunction can be used:
   * {{{
   * import _root_.io.teknek.arizona.spark.connector.util.MagicalTypeTricks._
   *
   * def function[T](t: T)(implicit ev: (λ[T] <:< (Int ∪ String))) = {
   *   println("t = " + t)
   * }
   *
   * function(5)      // t = 5
   * function("five") // t = five
   * function(5d)     // error: Cannot prove that
   *                  // (Double => Nothing) => Nothing <:< Int => Nothing with String => Nothing => Nothing.
   * }}}
   *
   * Based on [[http://www.chuusai.com/2011/06/09/scala-union-types-curry-howard/ this article]].
   */
  type ∪[T, U] = ¬[¬[T] with ¬[U]]

}
