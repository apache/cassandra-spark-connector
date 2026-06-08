import com.timushev.sbt.updates.UpdatesPlugin.autoImport.dependencyUpdatesFilter
import sbt.Keys.parallelExecution
import sbt.{Compile, moduleFilter, _}
import sbtassembly.AssemblyPlugin.autoImport.assembly

lazy val scala213 = "2.13.18"
lazy val supportedScalaVersions = List(scala213)

// factor out common settings
ThisBuild / scalaVersion := scala213
ThisBuild / scalacOptions ++= Seq("-release", "17")

// Publishing Info
ThisBuild / credentials ++= Publishing.Creds
ThisBuild / homepage := Some(url("https://github.com/datastax/spark-cassandra-connector"))
ThisBuild / licenses := List("Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt") )
ThisBuild / organization := "com.datastax.spark"
ThisBuild / organizationName := "Datastax"
ThisBuild / organizationHomepage := Some(url("https://www.datastax.com"))
ThisBuild / pomExtra := Publishing.OurDevelopers
ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / publishMavenStyle := true
ThisBuild / publishTo := Publishing.Repository
ThisBuild / scmInfo := Publishing.OurScmInfo
ThisBuild / version := Publishing.Version

Global / resolvers ++= Seq(
  DefaultMavenRepository,
  Resolver.sonatypeRepo("public")
)

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
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  },
  assembly / assemblyOption := (assemblyOption in assembly).value.copy(includeScala = false),
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
  .configs(IntegrationTest)
  .settings(Defaults.itSettings: _*) //This and above enables the "it" suite
  .settings(commonSettings)
  .settings(assemblySettings)
  .settings(
    crossScalaVersions := supportedScalaVersions,
    name := "spark-cassandra-connector",

    javacOptions ++= Seq("-source", "17", "-target", "17"),

    // test grouping
    integrationTestsWithFixtures := {
      Testing.testsWithFixtures((testLoader in IntegrationTest).value, (definedTests in IntegrationTest).value)
    },

    IntegrationTest / testGrouping := Testing.makeTestGroups(integrationTestsWithFixtures.value),
    IntegrationTest / testOptions += Tests.Argument("-oF"),  // show full stack traces

    Test / javacOptions ++= annotationProcessor ++ Seq("-d", (classDirectory in Test).value.toString),

    Global / concurrentRestrictions := Seq(Tags.limitAll(Testing.parallelTasks)),

    libraryDependencies ++= Dependencies.Spark.dependencies
      ++ Dependencies.Compatibility.dependencies(scalaVersion.value)
      ++ Dependencies.TestConnector.dependencies
      ++ Dependencies.Jetty.dependencies,

    scalacOptions in (Compile, doc) ++= scalacVersionDependantOptions(scalaBinaryVersion.value)
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
    assembly /test := {},
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
    Compile / packageBin := (assembly in (connector, Compile)).value
  )
