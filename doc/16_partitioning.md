# Documentation

## Spark Cassandra Connector and Spark Partitioners

### What are Spark Partitioners

Spark RDDs may have an element known as a [Partitioner](https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/Partitioner.scala);
these special classes are used during shuffles and co-grouping events (joins, groupBy) to map data 
to the correct Spark Partition. When one isn't specified Spark will usually repartition using 
a `HashPartitioner`. A repartition requires taking every value and moving it to the partition 
specified by the given partitioner. This is the reason that many keyed operations are such expensive 
operations.

### What is a Cassandra Partitioner

Cassandra also has a notion of a partitioner. The Cassandra partitioner takes the partition key of
a CQL Row and uses it to determine what node in the Cassandra cluster should host that data. The 
partitioner generates a `Token` which directly maps to the `TokenRange` of the CassandraCluster.
Each node in the Cassandra Cluster is responsible for a specific section of the full `TokenRange`.

For more information on Cassandra Data Modeling check out [DS220 Data Modeling](https://www.youtube.com/watch?v=jYvKiewV-5Q&list=PL2g2h-wyI4SqIigskyJNAeL2vSTJZU_Qp).

### How to take advantage of Cassandra Partitioning in Spark

The Spark Cassandra Connector now implements a `CassandraPartitioner` for specific RDDs when they
have been keyed using `keyBy`. This can only be done when a `CassandraTableScanRDD` has `keyBy` 
called and is keyed by the partition keys of the underlying table. The key may contain other
elements of the table but the partition key is required to build a `CassandraPartitioner`.

*All of these examples are run with Spark in Local Mode and --driver-memory 4g*

#### Example Schema


```sql
CREATE KEYSPACE doc_example WITH replication = 
    {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;

CREATE TABLE doc_example.purchases (
    userid int,
    purchaseid int,
    objectid text,
    amount int,
    PRIMARY KEY (userid, purchaseid, objectid) // Partition Key "userid"
);

CREATE TABLE doc_example.users (
    userid int PRIMARY KEY, // Partition key "userid"
    name text,
    zipcode int
);
```

For a data generation sample, jump see [here](#data-generator).

#### Keying a Table with the Partition Key Produces an RDD with a Partitioner

```scala
import io.teknek.arizona.spark.connector._

val ks = "doc_example"
val rdd = { sc.cassandraTable[(String, Int)](ks, "users")
  .select("name" as "_1", "zipcode" as "_2", "userid")
  .keyBy[Tuple1[Int]]("userid")
}

//16/04/08 11:19:52 DEBUG CassandraPartitioner: Building Partitioner with mapping
//Vector((userid,userid))
//for table TableDef(doc_example,users,ArrayBuffer(ColumnDef(userid,PartitionKeyColumn,IntType)),ArrayBuffer(),ArrayBuffer(ColumnDef(name,RegularColumn,VarCharType), ColumnDef(zipcode,RegularColumn,IntType)),List(),false)
//16/04/08 11:19:52 DEBUG CassandraTableScanRDD: Made partitioner Some(io.teknek.arizona.spark.connector.rdd.partitioner.CassandraPartitioner@6709eccf) for CassandraTableScanRDD[5] at RDD at CassandraRDD.scala:15
//16/04/08 11:19:52 DEBUG CassandraTableScanRDD: Assigning new Partitioner io.teknek.arizona.spark.connector.rdd.partitioner.CassandraPartitioner@6709eccf

rdd.partitioner
//res3: Option[org.apache.spark.Partitioner] = Some(io.teknek.arizona.spark.connector.rdd.partitioner.CassandraPartitioner@94515d3e)
```

#### Reading a table into a (K,V) type or Not Including the Partition Key will not Create a Partitioner

```scala
import io.teknek.arizona.spark.connector._

val ks = "doc_example"
val rdd1 = sc.cassandraTable[(Int,Int)](ks, "users")
rdd1.partitioner
//res8: Option[org.apache.spark.Partitioner] = None


val rdd2 = { sc.cassandraTable[(Int, String)](ks, "users")
  .select("name" as "_2", "zipcode" as "_1", "userid")
  .keyBy[Tuple1[Int]]("zipcode")
}
rdd2.partitioner
//res11: Option[org.apache.spark.Partitioner] = None
```

### Capabilities and limitations of CassandraPartitioner

Having a partitioner allow for groupByKey operations to no longer require a shuffle. Previously
`spanByKey` allowed Spark to take advantage of the natural ordering of Cassandra tables. The new partitioner
allows this to go a step further and group on any combination of the primary key which also contains
the partition key.

#### Grouping/Reducing on out of order Clustering keys

```scala
import io.teknek.arizona.spark.connector._
val ks = "doc_example"
val rdd = { sc.cassandraTable[(Int, Int)](ks, "purchases")
  .select("purchaseid" as "_1", "amount" as "_2", "userid", "objectid")
  .keyBy[(Int, String)]("userid","objectid")
}

rdd.groupByKey.toDebugString
//res28: String =
//(4) MapPartitionsRDD[50] at groupByKey at <console>:43 []
// |  CassandraTableScanRDD[41] at RDD at CassandraRDD.scala:15 []
//
// Note that there are no shuffles present in this tree
rdd.groupByKey.count
// 16/04/08 16:23:42 INFO DAGScheduler: Job 0 finished: count at <console>:35, took 34.171709 s
```

Doing the same thing without a partitioner requires a shuffle:

```scala
val ks = "doc_example"

//Empty map will remove the partitioner
val rdd = { sc.cassandraTable[(Int, Int)](ks, "purchases")
  .select("purchaseid" as "_1", "amount" as "_2", "userid", "objectid")
  .keyBy[(Int, String)]("userid","objectid")
}.map( x => x)

rdd.partitioner
//res2: Option[org.apache.spark.Partitioner] = None

rdd.groupByKey.toDebugString
//res3: String =
//(4) ShuffledRDD[11] at groupByKey at <console>:35 []
// +-(4) MapPartitionsRDD[10] at map at <console>:35 []
//    |  CassandraTableScanRDD[9] at RDD at CassandraRDD.scala:15 []

rdd.groupByKey.count
//16/04/08 16:28:23 INFO DAGScheduler: Job 1 finished: count at <console>:35, took 46.550789 s
```

In this example the partitioner example takes 70% of the time of the partitioned one.  This is even
with the entire load taking place on a single machine without any network traffic. When 
shuffles have to spill to disk the difference in times can become even greater. With very
large examples we've seen the cost of the shuffle even double the total compute time. 

*Limitations*

This can only be applied to RDDs that are keyed with their partition key, so it can not be used
to group or reduce on a generic Cassandra column. As long as you are grouping/reducing within a Cassandra partition this approach should save significant time.


#### Joining to Cassandra RDDs from non Cassandra RDDs

When joining two RDDs they are both required to be partitioned before the join can occur. Previously
this meant that when joining against a Cassandra both the Cassandra RDD and the RDD to be joined
would need to be shuffled. Now if the Cassandra RDD is keyed on the partition key you can join 
without the Cassandra RDD being shuffled.

```scala
import io.teknek.arizona.spark.connector._

val ks = "doc_example"
val rdd = { sc.cassandraTable[(String, Int)](ks, "users")
  .select("name" as "_1", "zipcode" as "_2", "userid")
  .keyBy[Tuple1[Int]]("userid")
}

val joinrdd = sc.parallelize(1 to 10000).map(x => (Tuple1(x),x) ).join(rdd)
joinrdd.toDebugString
//(1) MapPartitionsRDD[26] at join at <console>:35 []
// |  MapPartitionsRDD[25] at join at <console>:35 []
// |  CoGroupedRDD[24] at join at <console>:35 []
// +-(8) MapPartitionsRDD[23] at map at <console>:35 []
// |  |  ParallelCollectionRDD[22] at parallelize at <console>:35 []
// |  CassandraTableScanRDD[16] at RDD at CassandraRDD.scala:15 []
joinrdd.count
//16/04/08 17:25:47 INFO DAGScheduler: Job 14 finished: count at <console>:37, took 5.339490 s

//Use an empty map to drop the partitioner
val rddnopart = { sc.cassandraTable[(String, Int)](ks, "users")
                  .select("name" as "_1", "zipcode" as "_2", "userid")
                  .keyBy[Tuple1[Int]]("userid").map( x => x)
}

val joinnopart = sc.parallelize(1 to 10000).map(x => (Tuple1(x),x) ).join(rddnopart)
joinnopart.toDebugString
//res18: String =
//(8) MapPartitionsRDD[73] at join at <console>:36 []
// |  MapPartitionsRDD[72] at join at <console>:36 []
// |  CoGroupedRDD[71] at join at <console>:36 []
// +-(8) MapPartitionsRDD[70] at map at <console>:36 []
// |  |  ParallelCollectionRDD[69] at parallelize at <console>:36 []
// +-(1) MapPartitionsRDD[68] at map at <console>:34 []
//    |  CassandraTableScanRDD[61] at RDD at CassandraRDD.scala:15 []

joinnopart.count
//16/04/08 17:30:04 INFO DAGScheduler: Job 19 finished: count at <console>:37, took 6.308165 s
```

So again we can save the time of a shuffle if we are joining on a partition key of a Cassandra table. The
advantages here scale as well, the larger the data to be joined the greater the advantage to be 
gained.

*Limitations*

Again it is important to note that this is only possible when using the Partition key as the key
of the RDD.

#### Joining to Cassandra RDDs on Common Partition Keys

One area that can benefit greatly from the Partitioner is joins between Cassandra tables on a 
common partition key. This previously would requiring both RDDs but now requires no shuffles at
all. To use this both RDDs must have the *same partitioner*. A helper method `applyPartitionerFrom`
has been added to make it easy to share a partitioner between Cassandra RDDs.

```scala
val ks = "doc_example"
val rdd1 = { sc.cassandraTable[(Int, Int, String)](ks, "purchases")
  .select("purchaseid" as "_1", "amount" as "_2", "objectid" as "_3", "userid")
  .keyBy[Tuple1[Int]]("userid")
}

val rdd2 = { sc.cassandraTable[(String, Int)](ks, "users")
  .select("name" as "_1", "zipcode" as "_2", "userid")
  .keyBy[Tuple1[Int]]("userid")
}.applyPartitionerFrom(rdd1) // Assigns the partitioner from the first rdd to this one

val joinRDD = rdd1.join(rdd2)
joinRDD.toDebugString
//res37: String =
//(1) MapPartitionsRDD[123] at join at <console>:36 []
// |  MapPartitionsRDD[122] at join at <console>:36 []
// |  CoGroupedRDD[121] at join at <console>:36 []
// |  CassandraTableScanRDD[115] at RDD at CassandraRDD.scala:15 []
// |  CassandraTableScanRDD[120] at RDD at CassandraRDD.scala:15 []

joinRDD.count
//16/04/08 17:53:45 INFO DAGScheduler: Job 24 finished: count at <console>:39, took 27.583355 s

//Unpartitioned join
val rdd1nopart = { sc.cassandraTable[(Int, Int, String)](ks, "purchases")
  .select("purchaseid" as "_1", "amount" as "_2", "objectid" as "_3", "userid")
  .keyBy[Tuple1[Int]]("userid")
}.map(x => x)

val rdd2nopart = { sc.cassandraTable[(String, Int)](ks, "users")
  .select("name" as "_1", "zipcode" as "_2", "userid")
  .keyBy[Tuple1[Int]]("userid")
}.map(x => x)

val joinnopart = rdd1nopart.join(rdd2nopart)
joinnopart.toDebugString
//   res41: String =
//   (4) MapPartitionsRDD[136] at join at <console>:36 []
//    |  MapPartitionsRDD[135] at join at <console>:36 []
//    |  CoGroupedRDD[134] at join at <console>:36 []
//    +-(1) MapPartitionsRDD[128] at map at <console>:35 []
//    |  |  CassandraTableScanRDD[127] at RDD at CassandraRDD.scala:15 []
//    +-(4) MapPartitionsRDD[133] at map at <console>:35 []
//       |  CassandraTableScanRDD[132] at RDD at CassandraRDD.scala:15 []

joinnopart.count
//16/04/08 17:54:58 INFO DAGScheduler: Job 25 finished: count at <console>:39, took 40.040341 s
```

Again we can save a significant portion of time by eliminating the need for a shuffle.


### Caveats
The partitioning mechanism is very sensitive to the underlying Cassandra tables partitioning so this is
not a generic solution to all joins and groupBys. In addition it is important to note 

`applyPartitionerFrom` will copy the partitioning exactly from the host RDD to the target. 
This means it will only work if the key portion of both RDDs are the same. When applied, the host
partitioning takes over. This means it often makes sense to apply the partitioner from the RDD with
more partitions to get proper parallelism.

To key and apply a partitioner at the same time you can use `keyAndApplyPartitionerFrom`. These 
APIs are documented in `CassandraTableScanRDDFunctions` and are provided as implicits on 
`CassandraTableScanRDD`s.

Due to the way RDDs are constructed, the CassandraPartitioner cannot be applied without 
an explicit `KeyBy` call.

The class of the Key must be a true class and not a primitive. The Partitioner uses the a `RowWriter`
like the `saveToCassandra` method to convert key data to a Cassandra token. This `RowWriter` cannot be 
built for primitives which is why the above examples use `Tuple1` so frequently.

This functionality does not currently function in DataFrames.


### Data Generator
```scala

import io.teknek.arizona.spark.connector._
import java.util.Random
val ks = "doc_example"
val r = new Random(100)
val zipcodes = (1 to 100).map(_ => r.nextInt(99999)).distinct
val objects = Seq("pepper", "lemon", "pear", "squid", "beet", "iron", "grass", "axe", "grape")

case class RandomListSelector[T](list: Seq[T]) {
    val r = { new Random() }
    def next(): T = list(r.nextInt(list.length))
}

val randomObject = RandomListSelector(objects)
val randomZipCode = RandomListSelector(zipcodes)

//About 25 Seconds on my MBP 
sc.parallelize(1 to 1000000).map(x => 
    (x, s"User $x", randomZipCode.next)
  ).saveToCassandra(ks, "users")
 
//About 60 Seconds on my MBP
sc.parallelize(1 to 1000000).flatMap(x => 
    (1 to 10).map( p => (x, p, randomObject.next, p))
  ).saveToCassandra(ks, "purchases")

```
