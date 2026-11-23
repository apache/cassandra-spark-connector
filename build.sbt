import com.timushev.sbt.updates.UpdatesPlugin.autoImport.dependencyUpdatesFilter
import sbt.Keys.parallelExecution
import sbt.{Compile, moduleFilter, _}
import sbt.protocol.testing.TestResult
import sbtassembly.AssemblyPlugin.autoImport.assembly

lazy val scala213 = "2.13.18"
lazy val supportedScalaVersions = List(scala213)

// factor out common settings (in sbt 2 bare settings apply to all subprojects)
scalaVersion := scala213
scalacOptions ++= Seq("-release", "17")

// Publishing Info
credentials ++= Publishing.Creds
homepage := Some(uri("https://github.com/datastax/spark-cassandra-connector"))
licenses := List(License("Apache 2", uri("http://www.apache.org/licenses/LICENSE-2.0.txt")))
organization := "com.datastax.spark"
organizationName := "DataStax"
organizationHomepage := Some(uri("https://www.datastax.com"))
pomExtra := Publishing.OurDevelopers
pomIncludeRepository := { _ => false }
publishMavenStyle := true
publishTo := Publishing.Repository
scmInfo := Publishing.OurScmInfo
version := Publishing.Version

Global / resolvers ++= Seq(
  DefaultMavenRepository
)

lazy val integrationTestsWithFixtures = taskKey[Map[TestDefinition, Seq[String]]]("Evaluates names of all " +
  "Fixtures sub-traits for each test. Sets of fixture sub-traits names are used to form group tests.")

lazy val assemblySettings = Seq(
  // sbt 2 defaults to true; sbt-assembly needs class directories for the shading rules to work
  exportJars := false,
  assembly / parallelExecution := false,
  assembly / test := TestResult.Passed,
  assembly / assemblyMergeStrategy := {
    case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
    case PathList("META-INF", xs*) => MergeStrategy.last
    case "module-info.class" => MergeStrategy.discard
    case x =>
      val oldStrategy = (assembly / assemblyMergeStrategy).value
      oldStrategy(x)
  },
  assemblyPackageScala / assembleArtifact := false,
  assembly / assemblyShadeRules := {
    Seq(
      ShadeRule.rename("com.typesafe.config.**" -> s"shade.com.datastax.spark.connector.@0").inAll
    )
  },
)

// Spark 4.x runs on Java 17, which requires these module open directives for the forked
// (test) JVMs so Spark's use of internal JDK classes (unsafe, nio, etc.) keeps working.
lazy val jdk17Options = Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
)

lazy val commonSettings = Seq(
  // dependency updates check
  dependencyUpdatesFailBuild := true,
  dependencyUpdatesFilter -= moduleFilter(organization = "org.scala-lang" | "org.eclipse.jetty"),
  fork := true,
  parallelExecution := true,
  testForkedParallel := false,
  javaOptions ++= jdk17Options,
  testOptions += Tests.Argument(TestFrameworks.JUnit, "-v"),
  // sbt 2 enforces eviction errors in the Test configuration. ScalaTest 3.0.8 depends on scala-xml 1.x while Spark
  // brings 2.x. Remove once ScalaTest is upgraded.
  libraryDependencySchemes += "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always,
)


val annotationProcessor = Seq(
  "-processor", "com.datastax.oss.driver.internal.mapper.processor.MapperProcessor"
)

def scalacVersionDependantOptions(scalaBinary: String): Seq[String] = scalaBinary match {
  case "2.13" => Seq("-no-java-comments") //Scala Bug on inner classes, CassandraJavaUtil,
  case _ => Seq()
}

lazy val root = (project in file("."))
  .disablePlugins(AssemblyPlugin)
  .aggregate(connector, testSupport, driver, publishableAssembly)
  .settings(
    // crossScalaVersions must be set to Nil on the aggregating project
    crossScalaVersions := Nil,
    publish / skip := true
  )


lazy val connector = (project in file("connector"))
  .settings(commonSettings)
  .settings(assemblySettings)
  .settings(
    crossScalaVersions := supportedScalaVersions,
    name := "spark-cassandra-connector",

    javacOptions ++= Seq("-source", "17", "-target", "17"),

    Test / javacOptions ++= annotationProcessor ++ Seq("-d", (Test / classDirectory).value.toString),

    libraryDependencies ++= Dependencies.Spark.dependencies
      ++ Dependencies.Compatibility.dependencies(scalaVersion.value)
      ++ Dependencies.TestConnector.dependencies
      ++ Dependencies.Jetty.dependencies,

    Compile / doc / scalacOptions ++= scalacVersionDependantOptions(scalaBinaryVersion.value)
  )
  .dependsOn(
    testSupport % "test",
    driver
  )

/** Integration tests (they need a Cassandra cluster started by CCM). sbt 2 removed the built-in `IntegrationTest`
  * configuration, so the tests live in their own subproject as regular tests (`connector-it/src/test`). Run them with
  * `connectorIt/test` or `connectorIt/testOnly <suite>`. */
lazy val connectorIt = (project in file("connector-it"))
  .disablePlugins(AssemblyPlugin)
  .settings(commonSettings)
  .settings(
    crossScalaVersions := supportedScalaVersions,
    name := "spark-cassandra-connector-it",
    publish / skip := true,

    // integration tests talk to external Cassandra clusters: never serve results from the test cache
    Test / test / cacheStores := Nil,

    // test grouping
    integrationTestsWithFixtures := Def.uncached {
      Testing.testsWithFixtures((Test / testLoader).value, (Test / definedTests).value)
    },

    Test / testGrouping := Def.uncached(Testing.makeTestGroups(integrationTestsWithFixtures.value)),
    Test / testOptions += Tests.Argument("-oF"),  // show full stack traces

    Global / concurrentRestrictions := Seq(Tags.limitAll(Testing.parallelTasks)),

    libraryDependencies ++= Dependencies.Spark.dependencies
      ++ Dependencies.Compatibility.dependencies(scalaVersion.value)
      ++ Dependencies.TestConnector.dependencies
  )
  .dependsOn(
    connector % "compile->compile;test->test",
    testSupport % "test",
    driver
  )

lazy val testSupport = (project in file("test-support"))
  .disablePlugins(AssemblyPlugin)
  .settings(commonSettings)
  .settings(
    crossScalaVersions := supportedScalaVersions,
    name := "spark-cassandra-connector-test-support",
    libraryDependencies ++= Dependencies.Compatibility.dependencies(scalaVersion.value)
      ++ Dependencies.TestSupport.dependencies
  )

lazy val driver = (project in file("driver"))
  .disablePlugins(AssemblyPlugin)
  .settings(commonSettings)
  .settings(
    crossScalaVersions := supportedScalaVersions,
    name := "spark-cassandra-connector-driver",
    assembly / test := TestResult.Passed,
    libraryDependencies ++= Dependencies.Compatibility.dependencies(scalaVersion.value)
      ++ Dependencies.Driver.dependencies
      ++ Dependencies.TestDriver.dependencies
      :+ ("org.scala-lang" % "scala-reflect" % scalaVersion.value)
  )

/** The following project defines an extra artifact published alongside main 'spark-cassandra-connector'.
  * It's an assembled version of the main artifact. It contains all of the dependent classes, some of them
  * are shaded. */
lazy val publishableAssembly = project
  .disablePlugins(AssemblyPlugin)
  .settings(
    crossScalaVersions := supportedScalaVersions,
    name := "spark-cassandra-connector-assembly",
    Compile / packageBin := (connector / Compile / assembly).value
  )
