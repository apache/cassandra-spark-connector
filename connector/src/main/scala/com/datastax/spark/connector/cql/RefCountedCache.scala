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

package com.datastax.spark.connector.cql

import java.util.concurrent.{ThreadFactory, TimeUnit, Executors}
import java.util.ConcurrentModificationException

import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap


/** A lockless cache that caches values for multiple users
  * and destroys them once all users release them. One value can be associated with many keys.
  * Useful for sharing a costly resource.
  * @param create function to create new objects if not found in cache
  * @param destroy function to be called once the value is not used any more
  * @param keys function generating additional keys the value should be reachable by */
final class RefCountedCache[K, V](create: K => V,
                                  destroy: V => Any,
                                  keys: (K, V) => Set[K] = (_: K, _: V) => Set.empty[K]) {

  private[cql] case class ReleaseTask(value: V, count: Int, scheduledTime: Long) extends Runnable {
    def run(): Unit = {
      releaseImmediately(value, count)
    }
  }

  private[cql] val refCounter = new RefCountMap[V]
  private[cql] val cache = new TrieMap[K, V]
  private[cql] val valuesToKeys = new TrieMap[V, Set[K]]
  private[cql] val deferredReleases = new TrieMap[V, ReleaseTask]

  private def createNewValueAndKeys(key: K): (V, Set[K]) = {
    val value = create(key)
    try {
      (value, keys(key, value) + key)
    }
    catch {
      case t: Throwable =>
        destroy(value)
        throw t
    }
  }

  /** Acquires a value associated with key.
    * If the value was acquired by another thread and is present in the cache, it will be returned from cache.
    * If the value was not found in cache, a new value will be created by invoking `create` function
    * and will be saved to the cache and associated with `key` and other keys returned by invoking the
    * `keys` function on the value. */
  @tailrec
  def acquire(key: K): V = {
    cache.get(key) match {
      case Some(value) =>
        if (refCounter.acquireIfNonZero(value) > 0)
          value
        else
          acquire(key)
      case None =>
        syncAcquire(key)
    }
  }

  private[this] def syncAcquire(key: K): V = synchronized {
    cache.get(key) match {
      case Some(value) =>
        if (refCounter.acquireIfNonZero(value) > 0)
          value
        else
          acquire(key)
      case None =>
        val (value, keySet) = createNewValueAndKeys(key)
        refCounter.acquire(value)
        cache.putIfAbsent(key, value) match {
          case None =>
            keySet.foreach(k => cache.put(k, value))
            valuesToKeys.put(value, keySet)
            value
          case Some(otherValue) =>
            throw new ConcurrentModificationException("It shouldn't reach here as it is synchronized")
        }
    }
  }

  private def releaseImmediately(value: V, count: Int = 1): Unit = {
    if (refCounter.release(value, count) == 0) {
      // Here we're sure no-one else has the value.
      // Even though it is still in cache, it is not possible to get it from there,
      // because it won't be possible to increase the reference counter from 0.
      valuesToKeys(value).foreach(k => cache.remove(k, value))
      valuesToKeys.remove(value)
      destroy(value)
    }
  }

  @tailrec
  private def releaseDeferred(value: V, releaseDelayMillis: Int, count: Int): Unit = {
    val newTime = System.currentTimeMillis() + releaseDelayMillis
    val newTask =
      deferredReleases.remove(value) match {
        case Some(oldTask) =>
          ReleaseTask(value, oldTask.count + count, math.max(oldTask.scheduledTime, newTime))
        case None =>
          ReleaseTask(value, count, newTime)
      }
    deferredReleases.putIfAbsent(value, newTask) match {
      case Some(oldTask) =>
        releaseDeferred(value, releaseDelayMillis, newTask.count)
      case None =>
    }
  }

  /** Releases previously acquired value. Once the value is released by all threads and
    * the `releaseDelayMillis` timeout passes, the value is destroyed by calling `destroy` function and
    * removed from the cache. */
  def release(value: V, releaseDelayMillis: Int = 0): Unit = {
    if (releaseDelayMillis == 0 || scheduledExecutorService.isShutdown)
      releaseImmediately(value)
    else
      releaseDeferred(value, releaseDelayMillis, 1)
  }

  /** Shuts down the background deferred `release` scheduler and forces all pending release tasks to be executed */
  def shutdown(): Unit = {
    scheduledExecutorService.shutdown()
    while (deferredReleases.nonEmpty)
      for ((value, task) <- deferredReleases.snapshot())
        if (deferredReleases.remove(value, task))
          task.run()
  }

  /** Removes and destroys unused entries from the cache immediately.
    * Entries with reference count > 0 are not removed. */
  def evict(): Unit = {
    for ((value, task) <- deferredReleases)
      if (deferredReleases.remove(value, task))
        task.run()
  }

  /** Returns true if cache contains given key. */
  def contains(key: K): Boolean = {
    val value = cache.get(key)
    value.isDefined && refCounter.get(value.get) > 0
  }

  /** Called periodically by `scheduledExecutorService`*/
  private def processPendingReleases(): Unit = {
    val now = System.currentTimeMillis()
    for ((value, task) <- deferredReleases)
      if (task.scheduledTime <= now)
        if (deferredReleases.remove(value, task))
          task.run()
        // if the task has been replaced in the meantime and remove fails, that's fine, because
        // for sure it has scheduled time set in the future; we'll get to it on the next processPendingReleases call
  }

  private val processPendingReleasesTask = new Runnable() {
    override def run(): Unit = {
      processPendingReleases()
    }
  }

  private val scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
    override def newThread(r: Runnable) = {
      val thread = Executors.defaultThreadFactory().newThread(r)
      thread.setDaemon(true)
      thread
    }
  })

  // This must be high enough so it doesn't cause too much CPU usage,
  // but also low enough to allow for acceptable releaseDelayMillis resolution.
  private val period = 100
  scheduledExecutorService.scheduleAtFixedRate(processPendingReleasesTask, period, period, TimeUnit.MILLISECONDS)
}
