# Documentation
If you have the option we recommend using [DataFrames](14_data_frames.md) instead of RDDs

## Accessing Cassandra data with CassandraRDD

This section describes how to access data from Cassandra table with Spark.   

### Obtaining a Cassandra table as an `RDD`

To get a Spark RDD that represents a Cassandra table, 
call the `cassandraTable` method on the `SparkContext` object.

```scala
import com.datastax.spark.connector._ //Loads implicit functions
sc.cassandraTable("keyspace name", "table name")
```    

If no explicit type is given to `cassandraTable`, the result of this 
expression is `CassandraRDD[CassandraRow]`. This can be thought of as
a container of `CassandraRow` objects. For information on mapping to
other objects please see the documentation's [Mapping section](4_mapper.md)  

#### Example of loading an RDD from Cassandra
Create this keyspace and table in Cassandra using cqlsh:

```sql
CREATE KEYSPACE test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 };
CREATE TABLE test.words (word text PRIMARY KEY, count int);
```
    
Load data into the table:

```sql
INSERT INTO test.words (word, count) VALUES ('foo', 20);
INSERT INTO test.words (word, count) VALUES ('bar', 20);
```

Now you can read that table as `RDD`:

```scala
val rdd = sc.cassandraTable("test", "words")
// rdd: com.datastax.spark.connector.rdd.CassandraRDD[com.datastax.spark.connector.rdd.reader.CassandraRow] = CassandraRDD[0] at RDD at CassandraRDD.scala:41

rdd.toArray.foreach(println)
// CassandraRow{word: bar, count: 20}
// CassandraRow{word: foo, count: 20}   
```

### Using emptyCassandraRDD implementation

To create an instance of `CassandraRDD` for a table which does **not** 
exist use the emptyCassandraRDD method. `emptyCassandraRDD`s do not 
perform validation or create partitions so they can be used to represent 
absent tables. To create one, either initialize a `CassandraRDD` as 
usual and then call `toEmptyCassandraRDD` method on it or call 
`emptyCassandraTable` method on Spark context.

#### Example of using an EmptyCassandraRDD

```scala
// validation is deferred, so it is not triggered during rdd creation
val rdd = sc.cassandraTable[SomeType]("ks", "not_existing_table")
val emptyRDD = rdd.toEmptyCassandraRDD

val emptyRDD2 = sc.emptyCassandraTable[SomeType]("ks", "not_existing_table"))
```

### Reading primitive column values

You can read columns in a Cassandra table using the get methods 
of the `CassandraRow` object. The get methods access individual column 
values by column name or column index. Type conversions are applied on 
the fly. Use `getOption` variants when you expect to receive Cassandra 
null values.

Continuing with the previous example, follow these steps to access 
individual column values.

#### Example Accessing the values within a CassandraRow

Store the first item of the rdd in the firstRow value.

```scala
val firstRow = rdd.first
// firstRow: com.datastax.spark.connector.rdd.reader.CassandraRow = CassandraRow{word: bar, count: 20}
```

Get the number of columns and column names:

```scala
firstRow.columnNames    // Stream(word, count) 
firstRow.size           // 2 
```

Use one of `getXXX` getters to obtain a column value converted to desired type: 
```scala
firstRow.getInt("count")       // 20       
firstRow.getLong("count")      // 20L  
```

Or use a generic get to query the table by passing the return type directly:

```scala
firstRow.get[Int]("count")                   // 20       
firstRow.get[Long]("count")                  // 20L
firstRow.get[BigInt]("count")                // BigInt(20)
firstRow.get[java.math.BigInteger]("count")  // BigInteger(20)
```

### Working with nullable data

When reading potentially `null` data, use the `Option` type on the 
Scala side to prevent getting a `NullPointerException`.

#### Example accessing values in a CassandraRow that may be null
```scala
firstRow.getIntOption("count")        // Some(20)
firstRow.get[Option[Int]]("count")    // Some(20)    
```

### Reading collections

You can read collection columns in a Cassandra table using the 
`getList`, `getSet`, `getMap` or generic `get` 
methods of the `CassandraRow` object. The `get` methods access 
the collection column and return a corresponding Scala collection. 
The generic `get` method lets you specify the precise type of the 
returned collection.

Assuming you set up the test keyspace earlier, follow these steps 
to access a Cassandra collection.

In the test keyspace, set up a collection set using cqlsh:

```sql
CREATE TABLE test.users (username text PRIMARY KEY, emails SET<text>);
INSERT INTO test.users (username, emails) 
     VALUES ('someone', {'someone@email.com', 's@email.com'});
```

Then in your application, retrieve the first row: 

```scala
val row = sc.cassandraTable("test", "users").first
// row: com.datastax.spark.connector.rdd.reader.CassandraRow = CassandraRow{username: someone, emails: [someone@email.com, s@email.com]}
```

Query the collection set in Cassandra from Spark:

```scala
row.getList[String]("emails")            // Vector(someone@email.com, s@email.com)
row.get[List[String]]("emails")          // List(someone@email.com, s@email.com)    
row.get[Seq[String]]("emails")           // List(someone@email.com, s@email.com)   :Seq[String]
row.get[IndexedSeq[String]]("emails")    // Vector(someone@email.com, s@email.com) :IndexedSeq[String]
row.get[Set[String]]("emails")           // Set(someone@email.com, s@email.com)
```

It is also possible to convert a collection to CQL `String` representation:

```scala
row.get[String]("emails")               // "[someone@email.com, s@email.com]"
```

A `null` collection is equivalent to an empty collection, 
therefore you don't need to use `get[Option[...]]` 
with collections.

### Reading columns of Cassandra User Defined Types from a CassandraRow
UDT column values are represented by `com.datastax.spark.connector.UDTValue` type.
The same set of getters is available on `UDTValue` as on `CassandraRow`. UDT's can
also be mapped to Scala classes see [Mapping section](4_mapper.md)  

Assume the following table definition:
```sql
CREATE TYPE test.address (city text, street text, number int);
CREATE TABLE test.companies (name text PRIMARY KEY, address FROZEN<address>);
```

You can read the address field of the company in the following way:
```scala
val address: UDTValue = row.getUDTValue("address")
val city = address.getString("city")
val street = address.getString("street")
val number = address.getInt("number")
```

### Reading vectors

You can read vector columns in a Cassandra table similarly 
to reading lists using `getList` or generic `get` methods of the 
`CassandraRow` object. 

Assuming you set up the test keyspace earlier, follow these steps
to access a Cassandra collection.

In the test keyspace, set up a collection set using cqlsh:

```sql
CREATE TABLE test.things (id int PRIMARY KEY, name text, features vector<float, 3>);
INSERT INTO test.things (id, name, features) VALUES (1, 'a', [1.0, 2.0, 3.0]);
INSERT INTO test.things (id, name, features) VALUES (2, 'b', [2.2, 2.1, 2.0]);
INSERT INTO test.things (id, name, features) VALUES (3, 'c', [1.0, 1.5, 4.0]);
```

Then in your application, retrieve the first row:

```scala
val row = sc.cassandraTable("test", "things").first
// row: com.datastax.spark.connector.CassandraRow = CassandraRow{id: 2, features: [2.2, 2.1, 2.0], name: b}
```

Query the vectors in Cassandra from Spark:

```scala
row.getList[Float]("features")        // Vector[Float] = Vector(2.2, 2.1, 2.0)
row.get[List[Float]]("features")      // List[Float] = List(2.2, 2.1, 2.0)
row.get[Seq[Double]]("features")      // Seq[Double] = List(2.200000047683716, 2.0999999046325684, 2.0)
row.get[IndexedSeq[Int]]("features")  // IndexedSeq[Int] = Vector(2, 2, 2)
row.get[Set[Long]]("features")        // Set[Long] = Set(2)
```

It is also possible to convert a vector to CQL `String` representation:

```scala
scala> row.get[String]("features")    // String = [2.2, 2.1, 2.0]
```

A `null` vector is equivalent to an empty list. You can also use 
`get[Option[List[...]]]` to get `None` in case of `null`.

### Data type conversions

The following table shows recommended Scala types corresponding to Cassandra column types. 

| Cassandra type    | Scala types
|-------------------|--------------------------------------------
| `ascii`, `text`   | `String`                                         
| `bigint`          | `Long`                                       
| `blob`            | `ByteBuffer`, `Array[Byte]` 
| `boolean`         | `Boolean`, `Int`              
| `counter`         | `Long` 
| `date`            | `Int`, `String` (YYYY-MM-DD), `java.util.Date`, `java.sql.Date`, `org.joda.time.DateTime`
| `decimal`         | `BigDecimal`, `java.math.BigDecimal` 
| `double`          | `Double`    
| `float`           | `Float`    
| `inet`            | `java.net.InetAddress` 
| `int`             | `Int` 
| `list`            | `Vector`, `List`, `Iterable`, `Seq`, `IndexedSeq`, `java.util.List` 
| `map`             | `Map`, `TreeMap`, `java.util.HashMap` 
| `set`             | `Set`, `TreeSet`, `java.util.HashSet` 
| `smallint`        | `Short`
| `text`            | `String` 
| `time`            | `Long`, *Do Not Read this Column as a Date**
| `timestamp`       | `Long`, `java.util.Date`, `java.sql.Date`, `org.joda.time.DateTime` 
| `timeuuid`        | `java.util.UUID` 
| `tinyint`         | `Byte`
| `uuid`            | `java.util.UUID` 
| `varchar`         | `String` 
| `varint`          | `BigInt`, `java.math.BigInteger`
| `vector`          | `Vector`, `List`, `Iterable`, `Seq`, `IndexedSeq`, `java.util.List`
| `frozen<tuple<>>` | `TupleValue`, `scala.Product`, `org.apache.commons.lang3.tuple.Pair`, `org.apache.commons.lang3.tuple.Triple`  
| user defined      | `UDTValue`

*Since `time` is encoded in nanoseconds from epoch rather than milliseconds there will be Scale
error with an automatic conversion to `java.util.Date`*

Other conversions might work, but may cause loss of precision or may not work for all values. 
All types are convertible to strings. Converting strings to numbers, dates, 
addresses or UUIDs is possible as long as the string has proper 
contents, defined by the CQL3 standard. Maps can be implicitly converted to/from sequences of key-value tuples.

## Performing Efficient Joins With Cassandra Tables (since 1.2)
### Repartitioning RDDs based on a Cassandra Table's Replication
The method `repartitionByCassandraReplica` can be used to relocate data 
in an RDD to match the replication strategy of a given table and keyspace.
The method will look for partition key information in the given RDD and 
then use those values to determine which nodes in the Cluster would be 
responsible for that data. You can control the resultant number of partitions
with the parameter `partitionsPerHost`.

```scala
//CREATE TABLE test.shopping_history ( cust_id INT, date TIMESTAMP,  product TEXT, quantity INT, PRIMARY KEY (cust_id, date, product));
case class CustomerID(cust_id: Int) // Defines partition key
val idsOfInterest = sc.parallelize(1 to 1000).map(CustomerID(_))
val repartitioned =  idsOfInterest.repartitionByCassandraReplica("test", "shopping_history", 10)
repartitioned.partitions
//res0: Array[org.apache.spark.Partition] = Array(ReplicaPartition(0,Set(/127.0.0.1)), ...)
repartitioned.partitioner
//res1: Option[org.apache.spark.Partitioner] = Some(com.datastax.spark.connector.rdd.partitioner.ReplicaPartitioner@4484d6c2)
scala> repartitioned
//res2: com.datastax.spark.connector.rdd.partitioner.CassandraPartitionedRDD[CustomerID] = CassandraPartitionedRDD[5] at RDD at CassandraPartitionedRDD.scala:12
```


### Using joinWithCassandraTable
The connector supports using any RDD as a source of a direct join with a
Cassandra Table through `joinWithCassandraTable`. Any RDD which is 
writable to a Cassandra table via the `saveToCassandra` method can be 
used with this procedure as long as the full partition key is specified.

`joinWithCassandraTable` utilizes the java drive to execute a single 
query for every partition required by the source RDD so no un-needed 
data will be requested or serialized. This means a join between any RDD
and a Cassandra Table can be performed without doing a full table scan. 
When performed between two Cassandra Tables which share the same 
partition key this will *not* require movement of data between machines.
In all cases this method will use the source RDD's partitioning and 
placement for data locality.

`joinWithCassandraTable` is not affected by 
`cassandra.input.split.size_in_mb` since partitions are automatically 
inherited from the source RDD. The other input properties have their 
normal effects.

#### Example Join between two Cassandra Tables Sharing a Partition Key
```scala
//CREATE TABLE test.customer_info ( cust_id INT, name TEXT, address TEXT, PRIMARY KEY (cust_id));
val internalJoin = sc.cassandraTable("test","customer_info").joinWithCassandraTable("test","shopping_history")
internalJoin.toDebugString
//res4: String = (1) CassandraJoinRDD[9] at RDD at CassandraRDD.scala:14 []
internalJoin.collect
internalJoin.collect.foreach(println)
//(CassandraRow{cust_id: 3, address: Poland, name: Jacek},CassandraRow{cust_id: 3, date: 2015-03-09 13:59:25-0700, product: Guacamole, quantity: 2})
//(CassandraRow{cust_id: 0, address: West Coast, name: Russ},CassandraRow{cust_id: 0, date: 2015-03-09 13:58:14-0700, product: Scala is Fun, quantity: 1})
//(CassandraRow{cust_id: 0, address: West Coast, name: Russ},CassandraRow{cust_id: 0, date: 2015-03-09 13:59:04-0700, product: Candy, quantity: 3})
```

#### Example Join with Generic RDD
```scala
val joinWithRDD = sc.parallelize(0 to 5).filter(_%2==0).map(CustomerID(_)).joinWithCassandraTable("test","customer_info")
joinWithRDD.collect.foreach(println)
//(CustomerID(0),CassandraRow{cust_id: 0, address: West Coast, name: Russ})
//(CustomerID(2),CassandraRow{cust_id: 2, address: Poland, name: Piotr})
```

The `repartitionByCassandraReplica` method can be used prior to calling 
joinWithCassandraTable to obtain data locality, such that each spark 
partition will only require queries to their local node. This method can
also be used with two Cassandra Tables which have partitioned with 
different partition keys.

#### Example Join with a generic RDD after repartitioning
```scala
val oddIds = sc.parallelize(0 to 5).filter(_%2==1).map(CustomerID(_))
val localQueryRDD = oddIds.repartitionByCassandraReplica("test","customer_info").joinWithCassandraTable("test","customer_info")
repartitionRDD.collect.foreach(println)
//(CustomerID(1),CassandraRow{cust_id: 1, address: East Coast, name: Helena})
//(CustomerID(3),CassandraRow{cust_id: 3, address: Poland, name: Jacek})
```

### Compatibility of joinWithCassandraTable and other CassandraRDD APIs
The result of a joinWithCassandraRDD is compatible with all of the 
standard CassandraRDD api options with one additional function, `.on`. 
Use `.on(ColumnSelector)` for specifying which columns to join on. 
Since `.on` only applies to CassandraJoinRDDs it must immediately follow 
the `joinWithCassandraTable` call.

Joining on any column or columns in the primary key is supported as long 
as it can be made into a valid CQL query. This means the entire partition 
key must be specified and if any clustering key is specified all 
previous clustering keys must be supplied as well.

#### Cassandra Operations on a CassandraJoinRDD
```scala
val recentOrders = internalJoin.where("date > '2015-03-09'") // Where applied to every partition
val someOrders = internalJoin.limit(1) // Returns at most 1 CQL Row per Spark Partition
val numOrders = internalJoin.count() // Sums the total number of cql Rows
val orderQuantities = internalJoin.select("quantity") // Returns only the amount column as the right side of the join
val specifiedJoin = internalJoin.on(SomeColumns("cust_id")) // Joins on the cust_id column
val emptyJoin = internalJoin.toEmptyCassandraRDD // Makes an EmptyRDD
```

## Configuration Options for Adjusting Reads

The following options can be specified in the SparkConf object or as 
 `--conf` flag to spark-submit to adjust the read parameters of a Cassandra table.

See [Reference Section](reference.md#read-tuning-parameters)

### Using Implicits for Configuration

In addition you are able to set these parameters on a per table basis 
by using `implicit vals`. This allows a user to define a set of 
parameters in a separate object and import them into a block of 
code rather than repeatedly passing the same 
[`ReadConf` object](https://github.com/apache/cassandra-spark-connector/blob/trunk/connector/src/main/scala/com/datastax/spark/connector/rdd/ReadConf.scala#L26-L37).

#### Example Using Implicits for Read Configuration
```scala
object ReadConfigurationOne {
  implicit val readConf = ReadConf(100,100)
}
import ReadConfigurationOne._
val rdd = sc.cassandraTable("write_test","collections")
rdd.readConf
//com.datastax.spark.connector.rdd.ReadConf = ReadConf(100,100,LOCAL_ONE,true)
```
   
Or you can define them implicitly in the same block as the `cassandraTable` call

```scala
implicit val anotherConf = ReadConf(200,200)
val rddWithADifferentConf = sc.cassandraTable("write_test","collections")
rddWithADifferentConf.readConf
//com.datastax.spark.connector.rdd.ReadConf = ReadConf(200,200,LOCAL_ONE,true)
```

[Next - Server-side data selection and filtering](3_selection.md)
