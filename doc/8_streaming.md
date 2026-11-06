# Documentation
If you have the option we recommend using [DataFrames and Structured Streaming](14_data_frames.md) instead of DStreams

## Spark Streaming with Cassandra
Spark Streaming extends the core API to allow high-throughput, fault-tolerant stream processing of live data streams.
Data can be ingested from many sources such as Akka, Kafka, Flume, ZeroMQ, TCP sockets, etc. Results can be stored in Cassandra.

### The Basic Idea

#### Spark Streaming
Here is a basic Spark Streaming sample which writes to the console with `wordCounts.print()`:

Create a StreamingContext with a SparkConf configuration
```scala
    val ssc = new StreamingContext(sparkConf, Seconds(1))
```

Create a DStream that will connect to serverIP:serverPort 
```scala
    val lines = ssc.socketTextStream(serverIP, serverPort)
```

Count each word in each batch
```scala
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
```

Print a few of the counts to the console.
Start the computation.
```scala
    wordCounts.print()
    ssc.start()  
    ssc.awaitTermination() // Wait for the computation to terminate
```
 
#### Spark Streaming With Cassandra
Now let's add the Cassandra-specific functions on the `StreamingContext` and `RDD` into scope,
and we simply replace the print to console with pipe the output to Cassandra:
 
```scala
    import io.teknek.arizona.spark.connector.streaming._
    wordCounts.saveToCassandra("streaming_test", "words")
```

### Setting up Streaming
Follow the directions for [creating a `SparkConf`](0_quick_start.md)

#### Create A `StreamingContext`  
The second required parameter is the `batchDuration` which sets the interval streaming data will be divided into batches:
Note the Spark API provides a Milliseconds, Seconds, Minutes, all of which are accepted as this `Duration`.
This `Duration` is not to be confused with the [scala.concurrent.duration.Duration](https://www.scala-lang.org/api/current/index.html#scala.concurrent.duration.Duration).
 
```scala
    val ssc = new StreamingContext(conf, Seconds(n))
```


#### Creating A Stream
Create any of the available or custom Spark streams. The connector supports Akka Actor streams so far, but 
will be supporting many more in the next release. You can extend the provided `import io.teknek.arizona.spark.connector.streaming.TypedStreamingActor`:

Kafka Stream: creates an input stream that pulls messages from a Kafka Broker

```scala
    val stream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
          ssc, kafka.kafkaParams, Map(topic -> 1), StorageLevel.MEMORY_ONLY)
```

Actor Stream

```scala
    val stream = ssc.actorStream[String](Props[TypedStreamingActor[String]], "stream", StorageLevel.MEMORY_AND_DISK)
```
 
      
#### Enable Spark Streaming With Cassandra
Enable Cassandra-specific functions on the `StreamingContext`, `DStream` and `RDD`:

```scala
    import io.teknek.arizona.spark.connector.streaming._
```

<a id="structured-streaming-update-mode"></a>

### Structured Streaming Update Mode

The connector supports Spark Structured Streaming `Update` output mode for
stateful aggregations. Changed aggregate rows are written on each microbatch,
so windowed metrics do not need to wait for the watermark to close the window.

```scala
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

val metrics = events
  .withWatermark("event_time", "10 minutes")
  .groupBy(window(col("event_time"), "10 minutes"), col("content_id"))
  .count()

metrics.writeStream
  .format("org.apache.spark.sql.cassandra")
  .option("keyspace", "streaming_metrics")
  .option("table", "clickstream_metrics")
  .option("checkpointLocation", "/var/lib/spark/checkpoints/clickstream-metrics")
  .outputMode(OutputMode.Update())
  .start()
```

Rows are written as Cassandra upserts. The Cassandra primary key must identify
the aggregate row, typically the grouping key plus the window start time. A
later update for the same key replaces the prior aggregate values. Repeated
writes are expected while a window remains active and within the watermark.

If multiple streaming queries write different metrics into the same Cassandra
row, each query should include the primary-key columns and only the regular
columns it owns. Omitted columns are preserved; null columns require the
connector's null-handling option to be configured appropriately.

##### Writing to Cassandra From A Stream
Where `streaming_test` is the keyspace name and `words` is the table name:

Saving data:
```scala
    val wc = stream.flatMap(_.split("\\s+"))
        .map(x => (x, 1))
        .reduceByKey(_ + _)
        .saveToCassandra("streaming_test", "words", SomeColumns("word", "count")) 
```

Start the computation:
```scala         
    ssc.start()
```

##### Reading From Cassandra From The `StreamingContext`

 ```scala
     val rdd = ssc.cassandraTable("streaming_test", "key_value").select("key", "value").where("fu = ?", 3)
 ```
 
For a more detailed description as well as tuning writes, see [Saving Data to Cassandra](5_saving.md).

### Find out more
https://spark.apache.org/docs/latest/streaming-programming-guide.html

[Next - The arizona-spark-connector-embedded Artifact](10_embedded.md)
