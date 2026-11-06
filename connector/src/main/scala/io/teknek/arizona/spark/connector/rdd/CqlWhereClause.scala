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

/** Represents a logical conjunction of CQL predicates.
  * Each predicate can have placeholders denoted by '?' which get substituted by values from the `values` array.
  * The number of placeholders must match the size of the `values` array. */
case class CqlWhereClause(predicates: Seq[String], values: Seq[Any]) {

  assert(predicates.map(_.count(ch => ch == '?')).sum == values.size, "The number of placeholders is different than " +
    s"the number of values, this should never happen: $predicates, $values")

  /** Returns a conjunction of this clause and the given predicate. */
  def and(other: CqlWhereClause) =
    CqlWhereClause(predicates ++ other.predicates, values ++ other.values)

  override def toString: String = {
    val valuesIterator = values.iterator
    val predicatesWithValues = predicates.map { predicate =>
      val valuesCount = predicate.count(_ == '?')
      (predicate, (0 until valuesCount).map(_ => valuesIterator.next()))
    }
    predicatesWithValues.map { case (pred, values) =>
      val valuesString = if (values.size == 1) values.head else values.mkString("(", ", ", ")")
      s"[$pred, $valuesString]"
    }.mkString("[", ",", "]")
  }
}

object CqlWhereClause {

  /** Empty CQL WHERE clause selects all rows */
  val empty = new CqlWhereClause(Nil, Nil)
}


