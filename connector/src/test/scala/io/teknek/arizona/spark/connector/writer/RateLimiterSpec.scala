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

import org.scalatest.concurrent.Eventually
import org.scalatest.{FlatSpec, Matchers}


class RateLimiterSpec extends FlatSpec with Matchers with Eventually {

 val TestRates = Seq(1L, 2L, 4L, 6L, 8L, 16L, 32L)

  "RateLimiter" should "not cause delays if rate is not exceeded" in {
    var now: Long = 0
    val sleep: Long => Any = _ => fail("Sleep method should have never been called")

    val limiter = new RateLimiter(Long.MaxValue, 1000, () => now, sleep)
    for (_ <- 1 to 1000000) {
      now += 1
      limiter.maybeSleep(1000)
    }
  }

  it should "sleep to not exceed the target rate" in {
    var now: Long = 0
    var sleepTime: Long = 0

    def sleep(delay: Long) = {
      sleepTime += delay
      now += delay
    }

    // 10 units per second + 5 units burst allowed
    val bucketSize = 5
    val rate = 10
    val limiter = new RateLimiter(rate, bucketSize, () => now, sleep)

    val iterations = 25
    for (_ <- 1 to iterations)
      limiter.maybeSleep(1)

    sleepTime should be((iterations - bucketSize) * 1000L / rate)
  }

  it should "sleep and leak properly with different Rates" in {
    for (rate <- TestRates) {
      val bucketSize = rate * 2
      var now: Long = 0
      var sleepTime: Long = 0

      def sleep(delay: Long) = {
        sleepTime += delay
        now += delay
      }

      val limiter = new RateLimiter(rate, rate * 2, () => now, sleep)
      for (_ <- 1 to 1000) {
        assert(
          limiter.bucketFill.get() >= 0,
          "bucketFill has been overflowed, or has had a large negative number added to it")
        limiter.maybeSleep(rate)
      }

      eventually {
        limiter.leak()
        val delay = (limiter.bucketFill.get() - bucketSize) * 1000 / rate
        assert(delay <= 0, "Rate limiter was unable to leak it's way back to 0 delay")
      }
      sleepTime should not be (0)
    }
  }

}
