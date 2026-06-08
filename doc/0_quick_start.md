# Documentation

## 5-minute quick start guide for Spark 4.1

In this tutorial, you'll learn how to setup a very simple Spark application for reading and writing data from/to Cassandra.
Before you start, you need to have basic knowledge of Apache Cassandra and Apache Spark.
Refer to [Datastax](https://docs.datastax.com/en/) and [Cassandra documentation](https://cassandra.apache.org/doc/latest/getting_started/index.html)
and [Spark documentation](https://spark.apache.org/docs/latest/). 

### Prerequisites

Install and launch a Cassandra cluster and a Spark cluster.   

Configure a new Scala project with the Apache Spark and dependency.

The dependencies are easily retrieved via Maven Central 

    libraryDependencies += "com.datastax.spark" % "spark-cassandra-connector_2.13" % "4.1.0"
 
The spark-packages libraries can also be used with spark-submit and spark shell, these
commands will place the connector and all of its dependencies on the path of the
Spark Driver and all Spark Executors.
   
    $SPARK_HOME/bin/spark-shell --packages com.datastax.spark:spark-cassandra-connector_2.13:4.1.0
    $SPARK_HOME/bin/spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.13:4.1.0

For the list of available versions, see:
- https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector_2.13/
 
This Connector does not depend on the Cassandra server code.

 - For a detailed dependency list, see [project/Dependencies.scala](../project/Dependencies.scala)
 - For dependency versions, see [project/Versions.scala](../project/Versions.scala)

### Building
See [Building And Artifacts](12_building_and_artifacts.md)

### Loading up the Spark-Shell

Run the `spark-shell` with the packages line for your version. This will include the connector
and *all* of its dependencies on the Spark Class PathTo configure
the default Spark Configuration pass key value pairs with `--conf`

    $SPARK_HOME/bin/spark-shell --conf spark.cassandra.connection.host=127.0.0.1 \
                                --packages com.datastax.spark:spark-cassandra-connector_2.13:4.1.0
                                --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions

This command would set the Spark Cassandra Connector parameter 
`spark.cassandra.connection.host` to `127.0.0.1`. Change this
to the address of one of the nodes in your Cassandra cluster.

The extensions configuration option enables Cassandra Specific Catalyst
optimizations and functions.
 
Create a Catalog Reference to your Cassandra Cluster

```scala
spark.conf.set(s"spark.sql.catalog.mycatalog", "com.datastax.spark.connector.datasource.CassandraCatalog")
```

### Create a keyspace and table in Cassandra
These lines will create an actual Keyspace and Table in Cassandra.
```scala
spark.sql("CREATE DATABASE IF NOT EXISTS mycatalog.testks WITH DBPROPERTIES (class='SimpleStrategy',replication_factor='1')")
spark.sql("CREATE TABLE mycatalog.testks.testtab (key Int, value STRING) USING cassandra PARTITIONED BY (key)")

//List their contents
spark.sql("SHOW NAMESPACES FROM mycatalog").show
spark.sql("SHOW TABLES FROM mycatalog.testks").show
```

### Loading and analyzing data from Cassandra
Use the SparkSQL or a DataframeReader to Load a table

```scala
val df = spark.read.table("mycatalog.testks.testtab")
println(df.count)
df.show

//or

spark.sql("SELECT * FROM mycatalog.testks.testtab").show
```

### Saving data from a dataframe to Cassandra  
Add 10 more rows to the table:

```scala
spark
  .range(1, 10)
  .withColumnRenamed("id", "key")
  .withColumn("value", col("key").cast("string"))
  .writeTo("mycatalog.testks.testtab")
  .append
```

[Accessing data with DataFrames](14_data_frames.md)
[More details on Connecting to Cassandra](1_connecting.md)

