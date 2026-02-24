import com.timushev.sbt.updates.UpdatesPlugin.autoImport.dependencyUpdatesFilter
import sbt.Keys.parallelExecution
import sbt.{Compile, moduleFilter, _}
import sbtassembly.AssemblyPlugin.autoImport.assembly

lazy val scala213 = "2.13.13"
lazy val supportedScalaVersions = List(scala213)

// factor out common settings
ThisBuild / scalaVersion := scala213
ThisBuild / scalacOptions ++= Seq("-release:17", "-Werror")
ThisBuild / semanticdbEnabled := true
ThisBuild / semanticdbVersion := scalafixSemanticdb.revision

// Publishing Info
ThisBuild / homepage := Some(url("https://github.com/scylladb/spark-scylladb-connector"))
ThisBuild / licenses := List("Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt") )
ThisBuild / organization := "com.scylladb"
ThisBuild / organizationName := "ScyllaDB"
ThisBuild / organizationHomepage := Some(url("https://scylladb.com"))
ThisBuild / pomExtra := Publishing.OurDevelopers
ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / scmInfo := Publishing.OurScmInfo

// Publishing to Sonatype Central Portal (sbt 1.11+)
ThisBuild / publishTo := {
  val centralSnapshots = "https://central.sonatype.com/repository/maven-snapshots/"
  if (isSnapshot.value) Some("central-snapshots" at centralSnapshots)
  else localStaging.value
}
ThisBuild / publishMavenStyle := true

lazy val IntegrationTest = config("it") extend Test

lazy val integrationTestsWithFixtures = taskKey[Map[TestDefinition, Seq[String]]]("Evaluates names of all " +
  "Fixtures sub-traits for each test. Sets of fixture sub-traits names are used to form group tests.")

lazy val assemblySettings = Seq(
  assembly / parallelExecution := false,
  assembly / test := {},
  assembly / assemblyMergeStrategy := {
    case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
    case PathList("META-INF", xs @ _*) => MergeStrategy.last
    case "module-info.class" => MergeStrategy.discard
    case x =>
      val oldStrategy = (assembly / assemblyMergeStrategy).value
      oldStrategy(x)
  },
  assembly / assemblyPackageScala / assembleArtifact := false,
  assembly / assemblyShadeRules := {
    Seq(
      ShadeRule.rename("com.typesafe.config.**" -> s"shade.com.datastax.spark.connector.@0").inAll
    )
  },
)

lazy val commonSettings = Seq(
  // dependency updates check
  dependencyUpdatesFailBuild := true,
  dependencyUpdatesFilter -= moduleFilter(organization = "org.scala-lang"),
  fork := true,
  parallelExecution := true,
  testForkedParallel := false,
  testOptions += Tests.Argument(TestFrameworks.JUnit, "-v"),
)


val annotationProcessor = Seq(
  "-processor", "com.datastax.oss.driver.internal.mapper.processor.MapperProcessor"
)

lazy val root = (project in file("."))
  .disablePlugins(AssemblyPlugin)
  .aggregate(connector, testSupport, driver, publishableAssembly)
  .settings(
    // crossScalaVersions must be set to Nil on the aggregating project
    crossScalaVersions := Nil,
    publish / skip := true
  )


lazy val connector = (project in file("connector"))
  .configs(IntegrationTest)
  .settings(Defaults.itSettings: _*) //This and above enables the "it" suite
  .settings(commonSettings)
  .settings(assemblySettings)
  .settings(
    crossScalaVersions := supportedScalaVersions,
    name := "spark-scylladb-connector",

    javacOptions ++= Seq("-source", "17", "-target", "17"),

    // test grouping
    integrationTestsWithFixtures := {
      Testing.testsWithFixtures((IntegrationTest / testLoader).value, (IntegrationTest / definedTests).value)
    },

    IntegrationTest / testGrouping := Testing.makeTestGroups(integrationTestsWithFixtures.value),
    IntegrationTest / testOptions += Tests.Argument("-oF"),  // show full stack traces

    Test / javacOptions ++= annotationProcessor ++ Seq("-d", (Test / classDirectory).value.toString),

    Global / concurrentRestrictions := Seq(Tags.limitAll(Testing.parallelTasks)),

    libraryDependencies ++= Dependencies.Spark.dependencies
      ++ Dependencies.Compatibility.dependencies
      ++ Dependencies.TestConnector.dependencies,

    Compile / doc / scalacOptions ++= Seq("-no-java-comments")
  )
  .dependsOn(
    testSupport % "test",
    driver
  )

lazy val testSupport = (project in file("test-support"))
  .disablePlugins(AssemblyPlugin)
  .settings(commonSettings)
  .settings(
    crossScalaVersions := supportedScalaVersions,
    name := "spark-scylladb-connector-test-support",
    libraryDependencies ++= Dependencies.Compatibility.dependencies
      ++ Dependencies.TestSupport.dependencies
  )

lazy val driver = (project in file("driver"))
  .disablePlugins(AssemblyPlugin)
  .settings(commonSettings)
  .settings(
    crossScalaVersions := supportedScalaVersions,
    name := "spark-scylladb-connector-driver",
    assembly /test := {},
    libraryDependencies ++= Dependencies.Compatibility.dependencies
      ++ Dependencies.Driver.dependencies
      ++ Dependencies.TestDriver.dependencies
      :+ ("org.scala-lang" % "scala-reflect" % scalaVersion.value)
  )

/** The following project defines an extra artifact published alongside main 'spark-scylladb-connector'.
  * It's an assembled version of the main artifact. It contains all of the dependent classes, some of them
  * are shaded. */
lazy val publishableAssembly = project
  .disablePlugins(AssemblyPlugin)
  .settings(
    crossScalaVersions := supportedScalaVersions,
    name := "spark-scylladb-connector-assembly",
    Compile / packageBin := (connector / Compile / assembly).value
  )
