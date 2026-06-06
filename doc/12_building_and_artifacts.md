# Documentation

## Building

### Scala Versions
You can choose to build, assemble and run both Spark and the Spark Cassandra Connector against Scala 2.12 or 2.13.

As of Spark Cassandra Connector 4.1 (Apache Spark 4.x) the build supports Scala 2.13 only and requires
Java 17; Spark 4.x dropped Scala 2.12 and Java 8/11 support. Earlier connector releases (3.x) default to
Scala 2.12 and support Java 8.

### Building The Main Artifacts

All artifacts are generated to the standard output directories based on the Scala binary version you use.

In the root directory run:

    sbt/sbt package

The library package jars will be generated to:

    spark-cassandra-connector/connector/target/scala-{binary.version}/
    spark-cassandra-connector/driver/target/scala-{binary.version}/

The command generates also The Assembly Jar discussed below.

### Building The Assembly Jar

The Assembly Jar is built by the `sbt/sbt package` command mentioned above and by the dedicated 
command `sbt/sbt assembly`.

In the root directory run:

    sbt/sbt assembly

A fat jar with `assembly` suffix will be generated to:

    spark-cassandra-connector/connector/target/scala-{binary.version}/

The jar contains the Spark Cassandra Connector and its dependencies. Some of the dependencies are shaded to avoid 
classpath conflicts.  
It is recommended to use the main artifact when possible.

[Next - The Spark Shell](13_spark_shell.md)    
