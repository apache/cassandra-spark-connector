# Documentation

## Building

### Scala Versions
You can choose to build, assemble and run both Spark and the Spark Cassandra Connector against Scala 2.11, 2.12 or 2.13.

As of Spark Cassandra Connector 2.5.0 the build defaults to Scala 2.12. To build 2.11 artifacts, use `++2.11.12` switch 
in your sbt commands, e.g. `sbt/sbt ++2.11.12 package`.

### Building The Main Artifacts

All artifacts are generated to the standard output directories based on the Scala binary version you use.

In the root directory run:

    sbt/sbt package

The library package jars will be generated to:

    arizona-spark-connector/connector/target/scala-{binary.version}/
    arizona-spark-connector/driver/target/scala-{binary.version}/

The command generates also The Assembly Jar discussed below.

### Building The Assembly Jar

The Assembly Jar is built by the `sbt/sbt package` command mentioned above and by the dedicated 
command `sbt/sbt assembly`.

In the root directory run:

    sbt/sbt assembly

A fat jar with `assembly` suffix will be generated to:

    arizona-spark-connector/connector/target/scala-{binary.version}/

The jar contains the Spark Cassandra Connector and its dependencies. Some of the dependencies are shaded to avoid 
classpath conflicts.  
It is recommended to use the main artifact when possible.

[Next - The Spark Shell](13_spark_shell.md)    
