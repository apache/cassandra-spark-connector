# Documentation

## Datasets (Previously DataFrames)
These documents are relevant to Spark 3.0 and the SCC 3.0 and Greater Please See [Datasource V1](data_source_v1.md) for
documentation of older versions.

Datasets provide a new API for manipulating data within Spark. These provide a more user
friendly experience than pure Scala for common queries. The Spark Cassandra Connector provides
an integrated Data Source Version 2 to make creating Cassandra Datasets and DataFrames easy.

[What happened to DataFrames?](#what-happened-to-dataframes)

Spark Docs:
* [Data Sources](https://spark.apache.org/docs/latest/sql-programming-guide.html#data-sources)
* [Datasets and DataFrames](https://spark.apache.org/docs/latest/sql-programming-guide.html#datasets-and-dataframes)

### Extensions (Spark 2.5 and Greater)

The Spark Cassandra Connector includes a variety of catalyst rules which rewrite internal
Spark plans and provide unique C* specific optimizations. To load these rules you can either
directly add the extensions to your Spark environment or they can be added via a configuration property

```spark.sql.extensions``` to ```com.datastax.spark.connector.CassandraSparkExtensions```

This can also be done programmatically in most Spark Language implementations

Scala Example:
```scala 
SparkSession.builder()
  .config(sparkConf)
  .withExtensions(new CassandraSparkExtensions)
  .getOrCreate()
```

It is equivalent to set the configuration parameter or programmatically add the extensions.

### Catalogs
Spark 3.0 provides an API for connecting an external catalog directly to Spark Catalyst ecosystem. Setting 
up a catalog this way provides a connection to DDL in the underlying Datasource. This means any DDL or 
modifications to schema done in Spark will result in actual transformation in the underlying Cassandra
Schema, Tables and Keyspace.

**Upgrade Note: This is different than DSE's previous CassandraHiveMetastore which only provided facade and could not
perform DDL on the underlying Cluster**

To set up a catalog put the following configuration into your SparkSession configuration (or any other Spark Configuration file or Object)

```spark.sql.catalog.casscatalog``` to  ```com.datastax.spark.connector.datasource.CassandraCatalog```

This will set up an identifier of "casscatalog" to point to the catalog for the default Cassandra Cluster
For information on configuring Cassandra Catalogs see [documentation on Connecting](1_connecting.md)

Because the Catalog connects directly to the Cluster's underlying schema it will allow access to all
underlying keyspaces and tables without any further action. Tables can be accessed using a three part
identifier of `catalogName.keyspaceName.tableName` in any SparkSQL statement of DataFrame method.

A Spark Session can have as many catalogs as a user would like to configure.

### Supported Schema Commands

#### Creating Keyspaces (Called namespaces or databases in SparkSQL)

Creating a keyspace in a Cassandra Catalog can be done via SparkSQL. The only requirements are that
you provide valid options for the replication of the Keyspace. As with CQL you must specify both the
class and it's associated parameters. If no default catalog is set, be sure to set one in the keyspace's
name. Only SimpleStrategy and NetworkTopologyStrategy are currently supported.

`durable_writes` can also be passed as an option

SimpleStrategy
```sql
CREATE DATABASE IF NOT EXISTS casscatalog.ksname 
  WITH DBPROPERTIES (class='SimpleStrategy', replication_factor='5')
```

NetworkTopologyStrategy
```sql
CREATE DATABASE IF NOT EXISTS casscatalog.ntsname 
  WITH DBPROPERTIES (class='NetworkTopologyStrategy', cassandra='1')
```

#### Altering Keyspaces

Altering the replication factor of a keyspace is also allowed but the new replication class must be valid.

```sql
ALTER NAMESPACE casscatalog.ksname SET DBPROPERTIES (replication_factor='2')
```

Similarly replication within NetworkTopologyStrategies can also be altered. 

#### Dropping Keyspaces

Dropping a keyspace from the catalog will also drop the keyspace in Cassandra. The default command will not
allow for dropping a non empty keyspace unless the keyword `CASCADE` is added

```sql 
DROP DATABASE casscatalog.ksname
```

#### Creating Tables

All standard create table syntax can be used with the Cassandra Catalog and will create tables in the 
connected Cassandra Cluster. The only required option is the partitioning which can be defined using the
keyword `PARTITIONED BY`

Clustering key can be set by the table option `clustering_key` which takes a 
list of strings in form `columnName.[asc,desc]`

Any normal Cassandra Table options can be passed as well but those not known to the Java Driver
will be ignored. Map options should be formatted as `'{key=value, key2=value2}'`

```sql 
CREATE TABLE casscatalog.ksname.testTable (key_1 Int, key_2 Int, key_3 Int, cc1 STRING, cc2 String, cc3 String, value String) 
  USING cassandra
  PARTITIONED BY (key_1, key_2, key_3)
  TBLPROPERTIES (
    clustering_key='cc1.asc, cc2.desc, cc3.asc',
    compaction='{class=SizeTieredCompactionStrategy,bucket_high=1001}'
  )
```

Any statements that involve creating a Table are also supported like `CREATE TABLE AS SELECT`

Note that creating columns of Cassandra vector type is not supported yet. Such columns have to 
be created manually with CQL.

#### Altering Tables

All table properties can be changed and normal columns can be added and removed
using alter statements

```sql 
ALTER TABLE casscatalog.ksname.testTable ADD COLUMNS (newCol INT)
```

#### Dropping Tables

Similarly to keyspaces, tables can be dropped directly from Spark. This will drop the underlying
Cassandra Table as well.

```sql 
DROP TABLE casscatalog.ksname.testTable
```

### Reading and Writing

All normal SparkSQL can be used on a Cassandra Catalog for reading and writing and there is 
also a programmatic interface. All properties assigned to the parent catalog
will be inherited by all tables in that catalog.

#### Reading Examples
Reading with SQL
```sql
SELECT * FROM casscatalog.ksname.testTable
```

Reading with Scala
```scala 
spark.read.table("casscatalog.ksname.testTable")
```

Reading vectors, specifying a predicate on vector column
```sql
SELECT name, features FROM casscatalog.test.things WHERE features = array(float(1), float(1.5), float(4))
```


#### Writing Examples

Writing with Sql
```sql
INSERT INTO casscatalog.ksname.testTable SELECT * from casscatalog.ksname.testTable2
```


Writing with Scala
```scala 
df.writeTo("casscatalog.ksname.testTable")
```

Writing vectors
```sql
INSERT INTO casscatalog.test.things (id, name, features) VALUES (9, 'x', array(2, 3, 4))
```

#### Predicate Pushdown and Column Pruning

The connector will automatically pushdown all valid predicates to Cassandra. The
Datasource will also automatically only select columns from Cassandra which are required
to complete the query. This can be monitored with the explain command.

For example in the following query only the `value` column is required and the where clause
is automatically pushed down
```scala
spark.sql("SELECT value FROM mycatalog.ks.tab WHERE key = 1").explain
== Physical Plan ==
*(1) Project [value#54]
+- BatchScan[value#54] Cassandra Scan: ks.tab - Cassandra Filters: [["key" = ?, 1]] - Requested Columns: [value]
```

#### Count Pushdown

Requests for Cassandra table information that do not require actual column values
will be automatically convereted into Cassandra count operations. This will prevent any
data (other than the number of rows satisfying the query) to be sent to Spark. 


In this example we see that only a "RowCountRef" is used in the Cassandra Request, signifying the
count pushdown.
```scala
spark.sql("SELECT Count(*) FROM mycatalog.ks.tab WHERE key = 1").explain
   == Physical Plan ==
   *(1) HashAggregate(keys=[], functions=[count(1)])
   +- *(1) HashAggregate(keys=[], functions=[partial_count(1)])
      +- *(1) Project
         +- BatchScan[] Cassandra Scan: ks.tab - Cassandra Filters: [["key" = ?, 1]] - Requested Columns: [RowCountRef]
```

#### Direct Join

Joins with a Cassandra Table using a Partition Key may be automatically
converted into a `joinWithCassandraTable` style join if it is more efficient to 
query Cassandra that way. 

By default (`directJoinSetting=auto`) the Spark Cassandra Connector converts a 
join to a Direct Join when the following formula is true:

    (table size * directJoinSizeRatio) > size of keys

`directJoinSizeRatio` is a setting thay my be adjusted just like any other
Spark Cassandra Setting. See [Parameters section](reference.md#cassandra-connection-parameters)
for details.

Automatic Direct Join conversion may be permanently disabled or enabled with 
`directJoinSetting=off` and `directJoinSetting=on` settings.

For example to disregard the `directJoinSizeRatio` parameter and convert all the 
suitable joins to Direct Joins start `spark-sql` with:

    spark-sql --conf directJoinSetting=on
    
Note that not all joins are suitable for Direct Join conversion. The following 
conditions must be met for the conversion to happen:
 - At least one side of the join is a CassandraSourceRelation
 - The join condition fully restricts the partition key
 - The join keys types must match

Direct Join example with the following table:

```sql
CREATE TABLE ks.kv (
    k int PRIMARY KEY,
    v int) 
```

```scala
val range = spark.range(1, 1000).selectExpr("cast(id as int) key") // note the key type must match PRIMARY KEY type
val joinTarget = spark.read.table("myCatalog.ks.kv")

range.join(joinTarget, joinTarget("k") === range("key")).explain()
== Physical Plan ==
*(2) Project [key#2, k#8, v#9]
+- Cassandra Direct Join [k = key#2] ks.kv - Reading (k, v) Pushed {} 
   +- *(1) Project [cast(id#0L as int) AS key#2]
      +- *(1) Range (1, 1000, step=1, splits=12)
```

#### Runtime Filtering

Joins on the partition key that are not converted to a Direct Join, most often semi joins such as
`WHERE k IN (SELECT k FROM ...)` or `EXISTS (...)`, and joins with `directJoinSetting=off`, can still avoid a
full table scan. When Spark broadcasts the other side of the join, it passes the join key values to the Cassandra
scan at runtime (Spark's dynamic partition pruning), and the scan reads only what can match:

 - with at most `spark.cassandra.sql.runtimeFiltering.maxKeyLookups` keys (10000 by default) it reads each key
   with one query, the way a Direct Join does, in Spark partitions placed on the keys' replicas;
 - with more keys it scans only the token ranges holding at least one key, unless nearly all of them do;
 - otherwise, and if a key cannot be converted to the column type, it scans the whole table as before.

The filter only reduces what is read: Spark still evaluates the join, so results do not change.
Runtime filtering applies when:
 - the join restricts every partition key column, and the read does not already restrict the partition key;
 - the partition key types are not `decimal`, `float` or `double`, collections, tuples or UDTs (Cassandra
   treats values Spark considers equal, such as `1.5` and `1.50`, as different keys);
 - Spark's `spark.sql.optimizer.dynamicPartitionPruning.enabled` (true by default) is on and pushdown is enabled.

`EXPLAIN` shows the runtime filter in `RuntimeFilters: [dynamicpruningexpression(...)]` of the `BatchScan`.
Set `spark.cassandra.sql.runtimeFiltering.enabled=false` to always scan every token range.

#### Write Clustering

The connector writes rows in single-partition unlogged batches: it keeps up to
`spark.cassandra.output.batch.grouping.buffer.size` (1000) open batches per Spark task, one per partition key,
and sends a batch when it is full. When a task sees many more partition keys than that in random order, rows of
the same partition rarely meet in the buffer and most of them are sent one by one.

`spark.cassandra.sql.writeClustering` (default `off`) makes DataFrame and SQL writes, including streaming
micro-batches, ask Spark to cluster the rows by partition key (a shuffle) and sort them by primary key within
each task, so each partition's rows arrive together and go out in full batches:
 - `on` clusters every write;
 - `auto` clusters writes to tables with clustering columns when rows are batched by partition
   (`spark.cassandra.output.batch.grouping.key=partition` and `spark.cassandra.output.batch.size.rows` not 1),
   except counter tables;
 - `off` writes the rows in the order Spark produces them, as before.

It pays off for tables with clustering columns written from unsorted data with many partition keys per task (in
the connector benchmark, 500k rows in random order into 25k partitions: about 30% faster). It slows writes down
when the data is already grouped by partition key (the shuffle is wasted) and when many rows go to few partition
keys (each task then writes to only a few partitions, and concurrent writes to one partition contend in
Cassandra): about 2x slower in the benchmark. `on` also clusters writes to tables without clustering columns,
where it only adds cost (about 10% in the benchmark), so prefer `auto`. Set it per write with
`.option("spark.cassandra.sql.writeClustering", "auto")`.

#### TTL and WRITETIME

Through Cassandra Spark Extensions special functions are added to SparkSQL.

* `writetime(col)` - If the column represents an actual C* column this will be replaced
with the WriteTime of that column as in CQL.

* `ttl(col)` - Similar to writetime, this will replace a valid C* column reference with a
TTL value instead.

Read Example

```sql
SELECT TTL(v) as t, WRITETIME(t) FROM casscatalog.ksname.testTable WHERE k = 1
```

There are specific write options which can be used to assign WriteTime and TTL. 
These values can be set as either a literal value or a reference to a named column.

Example using another column as ttl
```scala
spark
  .createDataFrame(Seq((10000,-1,-1,-2)))
  .toDF("ttlCol","k","c","v")
  .write
  .format("cassandra")
  .option("keyspace", "ksname")
  .option("table", "tab")
  .option("ttl", "ttlCol") //Use the values in ttlCol as the TTL for these inserts
  .mode("APPEND")
  .save()
```

Example using a literal value to set ttl
```scala
spark
  .createDataFrame(Seq((10000,-1,-1,-2)))
  .toDF("ttlCol","k","c","v")
  .write
  .format("cassandra")
  .option("keyspace", "ksname")
  .option("table", "tab")
  .option("ttl", "1000") //Use 1000 as the TTL
  .mode("APPEND")
  .save()
```

### Reading and Writing without a Catalog

The DatasourceV2 still supports almost all of the same usages that the original
api served. There are a few exceptions but the old pattern of `spark.read.format.options.load` and
`df.write.format.options.save` are both still valid. One addition has been that the 
format string now only needs to be specified as `cassandra` without the full class name.

These methods are still useful if you need to express certain options only for a specific
read or write, and not for the entire catalog.

When using these methods you are required to set an option for `table` and `keyspace`

Example Read
```scala
spark.read
  .format("cassandra")
  .option("keyspace", "ksname")
  .option("table", "tab")
  .load()
```

For More information on configuration and settings using this api check the older
[Datasource V1](data_source_v1.md) which is all still valid for configuration.


[Next - Python DataFrames](15_python.md)
