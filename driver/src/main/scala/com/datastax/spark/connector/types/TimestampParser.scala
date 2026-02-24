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

package com.datastax.spark.connector.types

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, ZoneId, ZonedDateTime}
import java.util.Date

import scala.util.{Success, Try}

/** Parses following formats to [[Date]]
  *   - `yyyy-MM-dd`
  *   - `yyyy-MM-ddX`
  *   - `yyyy-MM-dd HH:mm`
  *   - `yyyy-MM-dd HH:mmX`
  *   - `yyyy-MM-dd HH:mm:ss`
  *   - `yyyy-MM-dd HH:mm:ssX`
  *   - `yyyy-MM-dd HH:mm:ss.S`
  *   - `yyyy-MM-dd HH:mm:ss.SX`
  *   - `yyyy-MM-dd HH:mm:ss.SS`
  *   - `yyyy-MM-dd HH:mm:ss.SSX`
  *   - `yyyy-MM-dd HH:mm:ss.SSS`
  *   - `yyyy-MM-dd HH:mm:ss.SSSX`
  *   - `yyyy-MM-dd'T'HH:mm`
  *   - `yyyy-MM-dd'T'HH:mmX`
  *   - `yyyy-MM-dd'T'HH:mm:ss`
  *   - `yyyy-MM-dd'T'HH:mm:ssX`
  *   - `yyyy-MM-dd'T'HH:mm:ss.S`
  *   - `yyyy-MM-dd'T'HH:mm:ss.SX`
  *   - `yyyy-MM-dd'T'HH:mm:ss.SS`
  *   - `yyyy-MM-dd'T'HH:mm:ss.SSX`
  *   - `yyyy-MM-dd'T'HH:mm:ss.SSS`
  *   - `yyyy-MM-dd'T'HH:mm:ss.SSSX`
  *
  * Waring! If time zone is not specified, the value is parsed to date in the default time zone.
  */
object TimestampParser {

  private val formatsWithoutZoneByLength = Seq(
    "yyyy-MM-dd",
    "yyyy-MM-dd HH:mm",
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd HH:mm:ss.S",
    "yyyy-MM-dd HH:mm:ss.SS",
    "yyyy-MM-dd HH:mm:ss.SSS"
  ).map(p => p.length -> p).toMap

  /** (hasT, hasZone) -> (lengthWithoutZone -> DateTimeFormat) */
  private val converterMatrix = {
    val booleans = Seq(true, false)
    val result = for (addT <- booleans; addZone <- booleans) yield {
      val patterns = formatsWithoutZoneByLength
        .view
        .mapValues(p => if (addT) p.replace(" ", "'T'") else p)
        .mapValues(p => if (addZone) p + "[XXXXX][XXXX][XXX][XX][X]" else p)
        .mapValues(DateTimeFormatter.ofPattern)
        .toMap
      (addT, addZone) -> patterns
    }
    result.toMap
  }

  private val slowPathConverters: Seq[DateTimeFormatter] = converterMatrix.values.flatMap(_.values).toSeq

  private def slowParse(date: String): Date = {
    slowPathConverters.view.map(p => Try(ZonedDateTime.parse(date, p))).find(_.isSuccess) match {
      case Some(Success(d)) => Date.from(d.toInstant)
      case _ => throw new IllegalArgumentException(s"Invalid date: $date")
    }
  }

  private val MaxZoneLength = 9 // e.g +08:30:15
  private val TimeSeparatorPosition = 10

  private def containsTimeSeparator(separator: Char, date: String): Boolean = {
    date.length > TimeSeparatorPosition && date.charAt(TimeSeparatorPosition) == separator
  }

  /** Index of a first time zone char or -1 of there is no zone in the given date.
    * zoneIndex("1986-01-02 21:05+04") returns 16. */
  private def findTimeZoneIndex(date: String): Int = {
    var found = -1
    var currentIndex = Math.max(date.length - MaxZoneLength, TimeSeparatorPosition) // there is no need to look for TZ in date part
    while (found < 0 && currentIndex < date.length) {
      if (date.charAt(currentIndex) == '+' || date.charAt(currentIndex) == '-' || date.charAt(currentIndex) == 'Z') {
        found = currentIndex
      }
      currentIndex = currentIndex + 1
    }
    found
  }

  private[types] def parseFastOrThrow(date: String): Date = {
    val zoneIndex = findTimeZoneIndex(date)
    val hasZone = zoneIndex > 0
    val lookupLength = if (hasZone) zoneIndex else date.length
    val hasT = containsTimeSeparator('T', date)
    val hasSpace = containsTimeSeparator(' ', date)

    // trying guess string format by 'T' presence, time zone presence and date[time] length
    val format = converterMatrix((hasT, hasZone))(lookupLength)

    if (hasT || hasSpace) { // contains time part
      if (hasZone)
        Date.from(ZonedDateTime.parse(date, format).toInstant)
      else
        Date.from(LocalDateTime.parse(date, format).atZone(ZoneId.systemDefault()).toInstant)
    } else {
      Date.from(LocalDate.parse(date, format).atStartOfDay(ZoneId.systemDefault()).toInstant)
    }
  }

  def parse(date: String): Date = {
    try {
      parseFastOrThrow(date)
    } catch {
      // fast parse failed, iterate over all parsers
      case _: Throwable => slowParse(date)
    }
  }
}
