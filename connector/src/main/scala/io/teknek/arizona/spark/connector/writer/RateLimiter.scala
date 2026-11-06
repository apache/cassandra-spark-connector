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

import java.util.concurrent.atomic.AtomicLong

import scala.annotation.tailrec

/** A leaking bucket rate limiter.
  * It can be used to limit rate of anything,
  * but typically it is used to limit rate of data transfer.
  *
  * It starts with an empty bucket.
  * When packets arrive, they are added to the bucket.
  * The bucket has a constant size and is leaking at a constant rate.
  * If the bucket overflows, the thread is delayed by the
  * amount of time proportional to the amount of the overflow.
  *
  * This class is thread safe and lockless.
  *
  * @param rate maximum allowed long-term rate per 1000 units of time
  * @param bucketSize maximum acceptable "burst"
  * @param time source of time; typically 1 unit = 1 ms
  * @param sleep a function to call to slow down the calling thread;
  *              must use the same time units as `time`
  */
class RateLimiter (
    rate: Long,
    bucketSize: Long,
    time: () => Long = System.currentTimeMillis,
    sleep: Long => Any = Thread.sleep) extends Serializable {

  require(rate > 0, "A positive rate is required")
  require(bucketSize > 0, "A positive bucket size is required")

  private[writer] val bucketFill = new AtomicLong(0L)
  private[writer] val lastTime = new AtomicLong(time()) //Avoid a large initial step

  @tailrec
  private def leak(toLeak: Long): Unit = {
    val fill = bucketFill.get()
    val reallyToLeak = math.min(fill, toLeak)  // we can't leak more than there is now
    if (!bucketFill.compareAndSet(fill, fill - reallyToLeak))
      leak(toLeak)
  }

  private[writer] def leak(): Unit = {
    val currentTime = time()
    val prevTime = lastTime.getAndSet(currentTime)
    val elapsedTime = math.max(currentTime - prevTime, 0L) // Protect against negative time
    leak(elapsedTime * rate / 1000L)
  }

  /** Processes a single packet.
    * If the packet is bigger than the current amount of
    * space available in the bucket, this method will
    * sleep for appropriate amount of time, in order
    * to not exceed the target rate. */
  def maybeSleep(packetSize: Long): Unit = {
    leak()
    val currentFill = bucketFill.addAndGet(packetSize)
    val overflow = currentFill - bucketSize
    val delay = 1000L * overflow / rate
    if (delay > 0L)
      sleep(delay)
  }

}
