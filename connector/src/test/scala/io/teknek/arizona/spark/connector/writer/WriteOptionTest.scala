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

package io.teknek.arizona.spark.connector.writer

import java.util.Date
import java.util.concurrent.TimeUnit

import org.apache.spark.streaming.{Duration => SparkDuration}
import org.joda.time.{DateTime, Duration => JodaDuration}
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration.{Duration => ScalaDuration}

class WriteOptionTest extends FlatSpec with Matchers {

  "TTLOption" should "properly create constant write option with duration in seconds" in {
    val option = TTLOption.constant(5)
    option.value shouldBe a[StaticWriteOptionValue[_]]
    option.value.asInstanceOf[StaticWriteOptionValue[Int]].value should be(5)
  }

  it should "properly create constant write option with scala.concurrent.duration.Duration" in {
    val option = TTLOption.constant(ScalaDuration.apply(5, TimeUnit.SECONDS))
    option.value shouldBe a[StaticWriteOptionValue[_]]
    option.value.asInstanceOf[StaticWriteOptionValue[Int]].value should be(5)
  }

  it should "properly create constant write option with scala.concurrent.duration.Duration.Infinite" in {
    val option = TTLOption.constant(ScalaDuration.Inf)
    option.value shouldBe a[StaticWriteOptionValue[_]]
    option.value.asInstanceOf[StaticWriteOptionValue[Int]].value should be(0)
  }

  it should "properly create constant write option with org.joda.time.Duration" in {
    val option = TTLOption.constant(JodaDuration.millis(5123L))
    option.value shouldBe a[StaticWriteOptionValue[_]]
    option.value.asInstanceOf[StaticWriteOptionValue[Int]].value should be(5)
  }

  it should "properly create infinite duration" in {
    val option = TTLOption.forever
    option.value shouldBe a[StaticWriteOptionValue[_]]
    option.value.asInstanceOf[StaticWriteOptionValue[Int]].value should be(0)
  }

  it should "properly create per-row duration placeholder" in {
    val option = TTLOption.perRow("test")
    option.value shouldBe a[PerRowWriteOptionValue[_]]
    option.value.asInstanceOf[PerRowWriteOptionValue[Int]].placeholder should be("test")
  }

  "TimestampOption" should "properly create constant write option with timestamp in microseconds" in {
    val option = TimestampOption.constant(12345L)
    option.value shouldBe a[StaticWriteOptionValue[_]]
    option.value.asInstanceOf[StaticWriteOptionValue[Long]].value should be(12345L)
  }

  it should "properly create constant write option with DateTime" in {
    val option = TimestampOption.constant(new DateTime(2010, 5, 6, 7, 8, 8, 10))
    option.value shouldBe a[StaticWriteOptionValue[_]]
    option.value.asInstanceOf[StaticWriteOptionValue[Long]].value should be(new DateTime(2010, 5, 6, 7, 8, 8, 10).getMillis * 1000L)
  }

  it should "properly create constant write option with Date" in {
    val t = new Date()
    val option = TimestampOption.constant(t)
    option.value shouldBe a[StaticWriteOptionValue[_]]
    option.value.asInstanceOf[StaticWriteOptionValue[Long]].value should be(t.getTime * 1000L)
  }

}
