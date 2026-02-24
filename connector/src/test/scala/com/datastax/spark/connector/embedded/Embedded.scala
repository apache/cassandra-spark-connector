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

package com.datastax.spark.connector.embedded

/** INTERNAL API.
  * Helper class for demos and tests. Useful for quick prototyping. */
private[embedded] trait Embedded extends EmbeddedIO with Serializable with Assertions


private[embedded] trait EmbeddedIO {

  import java.io._
  import java.net.{InetAddress, Socket}
  import java.util.UUID

  import scala.io.Source
  import scala.language.reflectiveCalls
  import scala.util.Try
  import org.apache.commons.lang3.SystemUtils

  val shutdownDeletePaths = new scala.collection.mutable.HashSet[String]()

  /** Automatically closes resource after use. Handy for closing streams, files, sessions etc.
    * Similar to try-with-resources in Java 7. */
  def closeAfterUse[T, C <: { def close(): Unit }](closeable: C)(code: C => T): T =
    try code(closeable) finally {
      closeable.close()
    }

  /** Copies a text file substituting every occurrence of `$ {VARIABLE}` with a value from the given map */
  def copyTextFileWithVariableSubstitution(source: InputStream, target: OutputStream, map: String => String): Unit = {
    val regex = "\\$\\{([a-zA-Z0-9_]+)\\}".r
    closeAfterUse(new PrintWriter(target)) { writer =>
      val input = Source.fromInputStream(source, "UTF-8")
      for (line <- input.getLines()) {
        val substituted = regex.replaceAllIn(line, m => map(m.group(1)))
        writer.println(substituted)
      }
    }
  }

  def createTempDir: File = {
    val tmpDir = java.nio.file.Files.createTempDirectory("spark").toFile
    val dir = mkdir(new File(tmpDir, "spark-tmp-" + UUID.randomUUID.toString))
    registerShutdownDeleteDir(dir)

    Runtime.getRuntime.addShutdownHook(new Thread("delete Spark temp dir " + dir) {
      override def run(): Unit = {
        if (! hasRootAsShutdownDeleteDir(dir)) deleteRecursively(dir)
      }
    })
    dir
  }

  /** Makes a new directory or throws an `IOException` if it cannot be made */
  def mkdir(dir: File): File = {
    if (!dir.mkdir()) throw new IOException(s"Could not create dir $dir")
    dir
  }

  /** Waits until a port at the given address is open or timeout passes.
    * @return true if managed to connect to the port, false if timeout happened first */
  def waitForPortOpen(host: InetAddress, port: Int, timeout: Long, stopIf: () => Boolean = () => false): Boolean = {
    val startTime = System.currentTimeMillis()
    val portProbe = Iterator.continually {
      Try {
        Thread.sleep(100)
        if (stopIf()) throw new RuntimeException
        val socket = new Socket(host, port)
        socket.close()
      }
    }
    portProbe
      .dropWhile(p => p.isFailure && System.currentTimeMillis() - startTime < timeout)
      .next()
      .isSuccess
  }


  def registerShutdownDeleteDir(file: File): Unit = {
    shutdownDeletePaths.synchronized {
      shutdownDeletePaths += file.getAbsolutePath
    }
  }

  def hasRootAsShutdownDeleteDir(file: File): Boolean = {
    val absolutePath = file.getAbsolutePath
    shutdownDeletePaths.synchronized {
      shutdownDeletePaths.exists { path =>
        !absolutePath.equals(path) && absolutePath.startsWith(path)
      }
    }
  }

  def deleteRecursively(file: File): Unit = {
    if (file != null) {
      if (file.isDirectory && !isSymlink(file)) {
        for (child <- listFilesSafely(file))
          deleteRecursively(child)
      }
      if (!file.delete()) {
        if (file.exists())
          throw new IOException("Failed to delete: " + file.getAbsolutePath)
      }
    }
  }

  def isSymlink(file: File): Boolean = {
    if (file == null) throw new NullPointerException("File must not be null")
    if (SystemUtils.IS_OS_WINDOWS) return false
    val fcd = if (file.getParent == null) file else new File(file.getParentFile.getCanonicalFile, file.getName)
    if (fcd.getCanonicalFile.equals(fcd.getAbsoluteFile)) false else true
  }

  def listFilesSafely(file: File): Seq[File] = {
    val files = file.listFiles()
    if (files == null) throw new IOException("Failed to list files for dir: " + file)
    files.toIndexedSeq
  }
}

