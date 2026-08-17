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

import java.lang.management.ManagementFactory
import com.sun.management.OperatingSystemMXBean
import sbt.Tests._
import sbt.{ForkOptions, TestDefinition}

import scala.util.Try

object Testing {

  def sparkJavaModuleOptions: Seq[String] = {
    if (Runtime.version().feature() >= 17) {
      Seq(
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-exports=java.base/sun.security.action=ALL-UNNAMED",
        "--add-exports=java.base/sun.util.calendar=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED")
    } else {
      Seq.empty
    }
  }

  private def interfacesImplementingFixture(c: Class[_], fixture: Class[_]): Seq[Class[_]] = {
    c.getInterfaces.toSeq.filter(i => i != fixture && fixture.isAssignableFrom(i)) ++
      c.getInterfaces.flatMap(interfacesImplementingFixture(_, fixture)) ++
      Option(c.getSuperclass).map(x => interfacesImplementingFixture(x, fixture)).getOrElse(Seq())
  }

  def testsWithFixtures(cl: ClassLoader, tests: Seq[TestDefinition]): Map[TestDefinition, Seq[String]] = {
    val fixture = cl.loadClass("com.datastax.spark.connector.cluster.Fixture")
    tests.map { testDefinition =>
      val c = cl.loadClass(testDefinition.name)
      (testDefinition, interfacesImplementingFixture(c, fixture).map(_.getSimpleName).distinct.sorted)
    }.toMap
  }

  def makeTestGroups(testsWithFixtures: Map[TestDefinition, Seq[String]]): Seq[Group] = {
    val (separateJVMTests, groupedTests) = testsWithFixtures
      .partition { case (_, fixtures) => fixtures.contains("SeparateJVM") }

    val testsByGroupName = groupedTests.groupBy(_._2).map { case (fixtures, tests) => (fixtures.mkString("-"), tests.keys) } ++
      separateJVMTests.zipWithIndex.map { case ((test, fixtures), i) => ((fixtures ++ i.toString).mkString("-"), Seq(test)) }

    println(s"All existing tests divided into ${testsByGroupName.size} groups:")
    testsByGroupName.toSeq
      .sortBy(-_._2.size) // biggest groups are executed first
      .zipWithIndex
      .map { case ((groupName, tests), i) =>
        println(s"[T$i] $groupName: ${tests.size} test(s)")
        val envVars = Map("CCM_IP_PREFIX" -> s"127.$i.", "TEST_GROUP_NO" -> i.toString) ++ sys.env +
          ("_JAVA_OPTIONS" -> "-ea") // enable java assertions
        Group(groupName, tests.toSeq, SubProcess(
          ForkOptions()
            .withEnvVars(envVars)
            .withRunJVMOptions((getCCMJvmOptions.flatten ++ sparkJavaModuleOptions).toVector)))
      }
  }

  /**
    * Guesses if the given version is DSE or not.
    * Internally it checks if the major version component is ge 5 which should suffice for a long time.
    * CCM_IS_DSE env setting takes precedence over this guess.
    */
  private def isDse(version: Option[String]): Boolean = {
    version.exists(_.startsWith("dse-"))
  }

  def getCCMJvmOptions = {
    val dbVersion = sys.env.get("CCM_CASSANDRA_VERSION")
    val ccmVersion = dbVersion.map(version => s"-Dccm.version=${version.stripPrefix("dse-")}")
    val cassandraVersion = dbVersion.map(version => s"-Dcassandra.version=${version.stripPrefix("dse-")}")
    val ccmDse = sys.env.get("CCM_IS_DSE").map(_.toLowerCase == "true").orElse(Some(isDse(dbVersion)))
      .map(isDSE => s"-Dccm.dse=$isDSE")
    val cassandraDirectory = sys.env.get("CCM_INSTALL_DIR").map(dir => s"-Dcassandra.directory=$dir")
    val ccmJava = sys.env.get("CCM_JAVA_HOME").orElse(sys.env.get("JAVA_HOME")).map(dir => s"-Dccm.java.home=$dir")
    val ccmPath = sys.env.get("CCM_JAVA_HOME").orElse(sys.env.get("JAVA_HOME")).map(dir => s"-Dccm.path=$dir/bin")
    val options = Seq(ccmVersion, ccmDse, cassandraVersion, cassandraDirectory, ccmJava, ccmPath)
    options
  }

  val MaxParallel = 10

  lazy val parallelTasks: Int = {
    val parallelTasks = sys.env.get("TEST_PARALLEL_TASKS").map(_.toInt).getOrElse {
      val osmxBean = ManagementFactory.getOperatingSystemMXBean.asInstanceOf[OperatingSystemMXBean]
      val sysMemoryInMB = osmxBean.getTotalPhysicalMemorySize >> 20
      val singleRunRequiredMem = 1024 + 1536 // ForkMain + DseModule
      val sbt = 1550
      Math.min(Math.max(1, ((sysMemoryInMB - sbt) / singleRunRequiredMem).toInt), MaxParallel)
    }
    println(s"Running $parallelTasks Parallel Tasks")
    parallelTasks
  }

}
