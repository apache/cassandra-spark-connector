# Documentation

## Connecting to Cassandra 
This section describes how Spark connects to Cassandra and 
how to execute CQL statements from Spark applications.

See the reference section for [Cassandra Connection Parameters](reference.md#cassandra-connection-parameters).

### Configuring Catalogs to Cassandra

DatasourceV2 introduced in Spark 3.0 makes connecting to Cassandra now easier than ever. Parameters for configuring your connection
can be done in the SparkConf, SparkSession, spark-defaults file or individually for the Catalog.  Once a catalog is configured it can 
be accessed through both SparkSql and DataFrames to read from, write to, create, and drop Cassandra tables.

While setting up a Catalog can be done before starting your application it can also be done at 
runtime by setting  ```spark.sql.catalog.anyname``` to  ```io.teknek.arizona.spark.connector.datasource.CassandraCatalog```
In your SparkSession. The "anyname" portion can be any name you would like to specify for this 
particular catalog.

Then any parameter for that catalog can be set by just appending the parameter name to the catalog name as in
```spark.sql.catalog.anyname.propertykey``` to ```propertyvalue```

Example: Manually configuring a Catalog named Cass100

```scala
spark.conf.set(s"spark.sql.catalog.cass100", "io.teknek.arizona.spark.connector.datasource.CassandraCatalog")
spark.conf.set(s"spark.sql.catalog.cass100.spark.cassandra.connection.host", "127.0.0.100")
```

Once this is set, we can access the catalog by using the triple part identifier of 
```catalog.keyspace.table``` which in this case would be ```cass100.ks.tab```

#### Example of reading from one cluster and writing to another

Using multiple clusters can be done by setting up one catalog per underlying cluster.

```scala
//Catalog Cass100 for Cluster at 127.0.0.100
spark.conf.set(s"spark.sql.catalog.cass100", "io.teknek.arizona.spark.connector.datasource.CassandraCatalog")
spark.conf.set(s"spark.sql.catalog.cass100.spark.cassandra.connection.host", "127.0.0.100")

//Catalog Cass200 for Cluster at 127.0.0.200
spark.conf.set(s"spark.sql.catalog.cass200", "io.teknek.arizona.spark.connector.datasource.CassandraCatalog")
spark.conf.set(s"spark.sql.catalog.cass200.spark.cassandra.connection.host", "127.0.0.200")

spark.sql("INSERT INTO cass200.ks.tab SELECT * from cass100.ks.tab")
//Or
spark.read.table("cass100.ks.tab").writeTo("cass200.ks.tab").append
```

### Connecting using an Astra Cloud Bundle or Driver Profile File (Since SCC 2.5)

Using a separate DSE Java Driver configuration file can also be used for your Catalog as long as 

* The file is either already accessible on a distributed file system (hdfs:// or s3a:// for example). 

or

* The file is distributed by Spark or is already on the Spark Classpath on Driver and Executor Nodes

If your file needs to be distributed by Spark use the `--files` option in Spark Submit or `SparkContext.addFile`. For
files added in this way just pass the file name to either of the following parameters without any other path info.

Files are then referenced through one of the following parameters

  1. `spark.cassandra.connection.config.cloud.path` for use with a Cloud Secure Connect bundle from [Datastax Astra]("https://astra.datastax.com/").  Please note that you must provide user name and password as well using corresponding configuration properties;
  2. `spark.cassandra.connection.config.profile.path` for use with a Java Driver [Profile](https://docs.datastax.com/en/developer/java-driver/4.2/manual/core/configuration/) 
  
When using a profile file all other configuration will be ignored. We are working on improving this behavior but at the moment,
using a profile supersedes all other config.

### Connection management

Whenever you call a method requiring access to Cassandra, the options in the `SparkConf` object will be used
to create a new connection or to borrow one already open from the global connection cache. 

#### Initial Contact

The initial contact node given in`spark.cassandra.connection.host` can 
be any node of the cluster. The driver will fetch the cluster topology 
from the contact node and will always try to connect to the closest node
in the same data center. If possible, connections are established to the 
same node the task is running on. Consequently, good locality of data 
can be achieved and the amount of data sent across the network is minimized. 

#### Inter-DataCenter Communication is forbidden by default

Connections are never made to data centers other than the data center 
of `spark.cassandra.connection.host`. If some nodes in the local data 
center are down and a read or write operation fails, the operation won't
be retried on nodes in a different data center. This technique guarantees 
proper workload isolation so that a huge analytics job won't disturb
the realtime part of the system.


#### Connection Pooling
Connections are cached internally. If you call two methods needing 
access to the same Cassandra cluster quickly, one after another, or in 
parallel from different threads, they will share the same logical connection 
represented by the underlying Java Driver `Cluster` object.  

This means code like
```scala
  val connector = CassandraConnector(sc.getConf)
  connector.withSessionDo(session => ...)
  connector.withSessionDo(session => ...)
```
or 
```scala
val connector = CassandraConnector(sc.getConf)
sc.parallelize(1 to 100).mapPartitions( it => connector.withSessionDo( session => ...))
```
Will not use more than one `Cluster` object or `Session` object per JVM

Eventually, when all the tasks needing Cassandra connectivity terminate,
the connection to the Cassandra cluster will be closed shortly thereafter. 
The period of time for keeping unused connections open is controlled by 
the global `spark.cassandra.connection.keep_alive_ms` system property, 
see [Cassandra Connection Parameters](reference.md#cassandra-connection-parameters).

### Connecting manually to Cassandra

If you ever need to manually connect to Cassandra in order to issue some CQL statements, 
this driver offers a handy `CassandraConnector` class which can be initialized 
from the `SparkConf` object and provides access to the `Session` objects. 
`CassandraConnector` instances are serializable
and therefore can be safely used in lambdas passed to Spark transformations
as seen in the examples above.

Assuming an appropriately configured `SparkConf` object is stored 
in the `conf` variable, the following code creates a keyspace and a table:

```scala
import io.teknek.arizona.spark.connector.cql.CassandraConnector

CassandraConnector(conf).withSessionDo { session =>
  session.execute("CREATE KEYSPACE test2 WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
  session.execute("CREATE TABLE test2.words (word text PRIMARY KEY, count int)")
}
```

[(Recommended) Accessing data with DataFrames](14_data_frames.md)
[(Legacy) Accessing data with RDDs](2_loading.md)

