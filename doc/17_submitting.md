# Documentation

## Submitting Spark applications with Spark Cassandra Connector

Spark Cassandra Connector (SCC) may be included with a submitted Spark application in 3 ways.
There are other ways too, but the following approaches are the most convenient, and most commonly used.

### Submitting with automatically resolved Spark Cassandra Connector jars

Spark may automatically resolve Spark Cassandra Connector and all of its dependencies (like Cassandra 
Java Driver). The resolved jars are then placed on the Spark application classpath. With this approach 
there is no need to manually download SCC from a repository nor tinker with fat (uber) jar assembly process.

`--packages` option with full SCC coordinate places SCC 
[main artifact](https://search.maven.org/artifact/com.datastax.spark/spark-cassandra-connector_2.13)
and all of its dependencies on the app's classpath.
```
spark-submit --packages com.datastax.spark:spark-cassandra-connector_<scala_version>:<scc_version> ...
```
See Spark [documentation](https://spark.apache.org/docs/latest/submitting-applications.html#advanced-dependency-management) for details.

Note that the application has to be compiled against the matching version of the connector, 
and that the connector classes should not be assembled into the application jar.

Note that this approach works with `spark-shell` as well.

### Submitting with locally available Spark Cassandra Connector jar 

Spark places jars provided with `--jars <url>` on the Spark application classpath. The jars are placed 
on the classpath without resolving any the dependencies as jar files do not contain information about the
dependencies. That is why using the
[main artifact](https://search.maven.org/artifact/com.datastax.spark/spark-cassandra-connector_2.13) with 
`--jars` is not effective - additional dependencies (like Cassandra Java Driver) are crucial for SCC 
functioning. Using `--jars` with the main artifact results in `NoClassDefFoundError`.

Spark Cassandra Connector 2.5 and newer are released with an alternative artifact - 
[assembly](https://search.maven.org/artifact/com.datastax.spark/spark-cassandra-connector-assembly_2.12).
It's a single jar with all the needed dependency classes included. It is suitable for using with `--jars` 
option.

```
spark-submit --jars com.datastax.spark:spark-cassandra-connector-assembly_<scala_version>:<scc_version> ...
```

Some of the dependencies included in the assembly are shaded to avoid classpath conflicts in 
some of the cloud environments.

Note that the application has to be compiled against the matching version of the connector, and that the
connector classes should not be assembled into the application jar.

Note that this approach works with `spark-shell` as well.

### Building and submitting a fat jar containing the connector

Build tools like Apache Maven™ may create a fat (uber) jar that contain all of the dependencies.
This functionality may be used to create a Spark application that contains Spark Cassandra Connector main 
artifact and all of its dependencies. The resulting Spark application may be submitted without any 
extra `spark-submit` options.

Refer to your build tools documentation for details.

Note that this approach isn't well suited for `spark-shell`.