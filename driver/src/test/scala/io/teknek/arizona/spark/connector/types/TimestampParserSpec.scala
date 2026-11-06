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

package io.teknek.arizona.spark.connector.types

import org.scalatest.FlatSpec

class TimestampParserSpec extends FlatSpec {

  it should "parse fast all supported date[time[zone]] formats" in {
    /* look in [[DateTimeFormatter]] for 'X' definition*/
    val validZones = Set(
      "",
      "Z",
      "-08",
      "-0830",
      "-08:30",
      "-083015",
      "-08:30:15",
      "+08",
      "+0830",
      "+08:30",
      "+083015",
      "+08:30:15"
    )

    val validDates = Set(
      "1986-01-02",
      "1986-01-02 21:05",
      "1986-01-02 21:05:07",
      "1986-01-02 21:05:07.1",
      "1986-01-02 21:05:07.12",
      "1986-01-02 21:05:07.123"
    )

    val datesAndDatesWithT = validDates
      .flatMap(date => Set(date) + date.replace(' ', 'T'))

    val allDates = for (date <- datesAndDatesWithT; zone <- validZones) yield {
      date + zone
    }

    allDates.foreach(TimestampParser.parseFastOrThrow)
  }
}
