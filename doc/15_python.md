# Documentation

## PySpark with Data Frames

With the inclusion of the Cassandra Data Source, PySpark can now be used with the Connector to 
access Cassandra data. This does not require DataStax Enterprise but you are limited to DataFrame
only operations.

### Setup

To enable Cassandra access the Spark Cassandra Connector assembly jar must be included on both the
driver and executor classpath for the PySpark Java Gateway. This can be done by starting the PySpark
shell similarly to how the spark shell is started. The preferred method is now to use the maven artifact.

```bash
./bin/pyspark \
  --packages com.datastax.spark:spark-cassandra-connector_2.13:4.1.0 \
  --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions
```

### Catalogs

Spark allows you to manipulate external data with and without a Catalog.
For a short intro and more details about Catalogs see [Quick Start](0_quick_start.md) and 
[Data Frames](14_data_frames.md).

#### Loading a DataFrame

Loading a data set with DatasourceV2 requires creating a Catalog Reference to your Cassandra Cluster.

```python
spark.conf.set("spark.sql.catalog.myCatalog", "com.datastax.spark.connector.datasource.CassandraCatalog")
spark.read.table("myCatalog.myKs.myTab").show()
```

#### Saving a DataFrame to Cassandra

A DataFrame can be saved to an *existing* Cassandra table by using the the `saveAsTable` method with a catalog, keyspace 
and a table name specified.

```python
spark.range(1, 10)\
    .selectExpr("id as k")\
    .write\
    .mode("append")\
    .partitionBy("k")\
    .saveAsTable("myCatalog.myKs.myTab")
```

### Manipulating data without a Catalog

#### Loading a DataFrame

A DataFrame can be created which links to Cassandra by using the the `org.apache.spark.sql.cassandra` 
source and by specifying keyword arguments for `keyspace` and `table`.

#### Example Loading a Cassandra Table as a Pyspark DataFrame
```python
 spark.read\
    .format("org.apache.spark.sql.cassandra")\
    .options(table="kv", keyspace="test")\
    .load().show()
```

```
+-+-+
|k|v|
+-+-+
|5|5|
|1|1|
|2|2|
|4|4|
|3|3|
+-+-+
```

#### Saving a DataFrame to Cassandra

A DataFrame can be saved to an *existing* Cassandra table by using the the `org.apache.spark.sql.cassandra` source and by specifying keyword arguments for `keyspace` and `table` and saving mode (`append`, `overwrite`, `error` or `ignore`, see [Data Sources API doc](https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#save-modes)).

##### Example Saving to a Cassandra Table as a Pyspark DataFrame
```python
 df.write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="kv", keyspace="test")\
    .save()
```

The options and parameters are identical to the Scala Data Frames Api so
please see [Data Frames](14_data_frames.md) for more information.

### Passing options with periods to the DataFrameReader

Python does not support using periods(".") in variable names. This makes it
slightly more difficult to pass SCC options to the DataFrameReader. The `options`
function takes `kwargs**` which means you can't directly pass in keys. There is a 
workaround though. Python allows you to pass a dictionary as a representation of kwargs and dictionaries
can have keys with periods. 

#### Example of using a dictionary as kwargs

    load_options = { "table": "kv", "keyspace": "test", "spark.cassandra.input.split.size_in_mb": "10"}
    spark.read.format("org.apache.spark.sql.cassandra").options(**load_options).load().show()

[Next - Spark Partitioners](16_partitioning.md)
