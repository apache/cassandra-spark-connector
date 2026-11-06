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

package io.teknek.arizona.spark.connector

import org.scalatest.{Matchers, WordSpecLike}

package object testkit {

  /** Basic unit test abstraction. */
  trait AbstractSpec extends WordSpecLike with Matchers

  val dataSeq = Seq (
    Seq("1first", "1round", "1words"),
    Seq("2second", "2round", "2words"),
    Seq("3third", "3round", "3words"),
    Seq("4fourth", "4round", "4words")
  )

  val data = dataSeq.head

}
