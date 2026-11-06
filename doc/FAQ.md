# Documentation

## Frequently Asked Questions

### Why am I seeing (Scala Version Mismatch Error)

#### Non Exclusive List of Scala Version Mismatch Errors
* `NoClassDefFoundError: scala/collection/GenTraversableOnce$class?`
* `NoSuchMethodError: scala.reflect.api.JavaUniverse.runtimeMirror(Ljava/lang/ClassLoader;)`
* `missing or invalid dependency detected while loading class file 'ScalaGettableByIndexData.class'`
* `java.lang.NoClassDefFoundError: scala/runtime/AbstractPartialFunction`

This means that there is a mix of Scala versions in the libraries used in your
code. The collection API is different between Scala 2.10 and 2.11 and this the 
most common error which occurs if a Scala 2.10 library is attempted to be loaded
in a Scala 2.11 runtime. To fix this make sure that the name has the correct
Scala version suffix to match your Scala version. 

##### Spark Cassandra Connector built Against Scala 2.10
```xml
<artifactId>arizona-spark-connector_2.10</artifactId>
```
2.10 needs to match the Scala version of Spark and all other
Scala libs.


##### Spark Cassandra Connector dependency in SBT
In sbt `%%` means append the suffix of Scala version in use for
compilation.

```
  "io.teknek.arizona.spark" %% "arizona-spark-connector" % connectorVersion % "provided"
```


For reference the defaults of Spark as downloaded from the Apache Website are

| Spark Version | Scala Version Default | Supported Scala Version|
----------------|-----------------------|------------------------|
| 0 -> 1.6      | 2.10                  | 2.10, 2.11             |
| 2.0 ->        | 2.11                  | 2.10(*), 2.11          |

\* Deprecated

### How do I Fix Guava Classpath Errors

Guava errors come from a conflict between Guava brought in by some 
dependency (like Hadoop 2.7) and the Cassandra Java Driver. 
The Cassandra Java Driver is unable to function correctly if an 
earlier version of Guava is preempting the required version. The 
Java Driver will throw errors if it determines 
this is the case.

To simplest fix is to move to an artifact which contains the 
Driver and Guava shaded together.
 
[SPARKC-355](https://datastax-oss.atlassian.net/browse/SPARKC-355) 
fixes this in the Spark Cassandra Connector 1.6.2 and 2.0.0-M3 and 
greater releases. 

The artifacts at 
[Spark Packages](https://spark-packages.org/package/datastax/arizona-spark-connector) 
and on 
[Maven Central](https://mvnrepository.com/artifact/io.teknek.arizona.spark/arizona-spark-connector_2.10)
will now automatically have Guava shaded and the driver included.

If you are using these artifacts you must remove any other dependencies 
on the Cassandra Java Driver from build files.

### Why is the Cassandra Java Driver embedded in Spark Cassandra Connector artifacts?

To avoid Guava errors we must make sure that the Cassandra Java Driver can
only possibly reference our chosen Guava. We force this by shading 
(essentially renaming) the Guava referenced by the Cassandra Java Driver
so there is no possible ambiguity. Since these references are within
the Java Driver, the Java Driver must be included for these changes to
take effective.

### How do I use libraries which depend on the Cassandra Java Driver?

Since the default artifacts have the Java Driver included with shaded 
references, it will be very difficult to use other libraries which utilize
the Cassandra Java Driver. To enable this an additional artifact has
been published `arizona-spark-connector-unshaded` which can be used in
manually shaded projects. Using these unshaded artifacts will require you to 
 manually shade the Guava references inside your code and to launch
with an "uber-jar." Using `--packages` will no longer work.

For some hints on shading see how the Cassandra Connector does this in
the [settings file.](https://github.com/datastax/arizona-spark-connector/blob/v2.0.0/project/Settings.scala#L329-L347)

### Why is my job running on a single executor? Why am I not seeing any parallelism?

The first thing to check when you see that a Spark job is not being parallelized is to
determine how many tasks have been generated. To check this look at the UI for your spark
job and see how many tasks are being run. In the current Shell a small progress bar is shown
when running stages, the numbers represent (Completed Tasks + Running Tasks) / Total Tasks 

    [Stage 2:=============================================>  (121 + 1) / 200]

If you see that only a single task has been created this means that the Cassandra Token range has
not been split into a enough tasks to be well parallelized on your cluster. The number of 
Spark partitions(tasks) created is directly controlled by the setting 
`spark.cassandra.input.split.size_in_mb`.
This number reflects the approximate amount of Cassandra Data in any given Spark partition.
To increase the number of Spark Partitions decrease this number from the default (64mb) to one that
will sufficiently break up your Cassandra token range. This can also be adjusted on a per cassandraTable basis
with the function `withReadConf` and specifying a new `ReadConf` object.

If there is more than one task but only a single machine is working, make sure that the job itself
has been allocated multiple executor slots to work with. This is set at the time of SparkContext
creation with `spark.cores.max` in the `SparkConf` and cannot be changed during the job.

One last thing to check is whether there is a `where` clause with a partition-key predicate. Currently 
the Spark Cassandra Connector creates Spark Tasks which contain entire Cassandra partitions. This method 
ensures a single Cassandra partition request will always create a single Spark task. `where` clauses with
an `in` will also generate a single Spark Partition.

### Why can't the spark job find Spark Cassandra Connector Classes? (ClassNotFound Exceptions for SCC Classes)
* java.lang.NoClassDefFoundError: com/twitter/jsr166e/LongAdder
* java.lang.ClassNotFoundException: io.teknek.arizona.spark.connector.rdd.partitioner.CassandraPartition

These errors are commonly thrown when the Spark Cassandra Connector or its dependencies are not
on the runtime classpath of the Spark Application. This is usually caused by not using the
prescribed `--packages` method of adding the Spark Cassandra Connector and its dependencies
to the runtime classpath. Fix this by following the launch guidelines as shown in the 
[quick start guide](0_quick_start.md). Not using this method means it is up to the user to manually 
ensure that the SCC and all of its dependencies wind up on the execution classpath.

### Where should I set configuration options for the connector?

The suggested location is to use the `spark-defaults.conf` file in your spark/conf directory but 
this file is ONLY used by spark-submit. Any applications not running through spark submit will ignore
this file. You can also specify Spark-Submit conf options with `--conf option=value` on the command
line.
 
For applications not running through spark submit, set the options in the `SparkConf` object used to 
create your `SparkContext`. Usually this will take the form of a series of statements that look like

```scala
val conf = SparkConf()
  .set("Option","Value")
  ...
  
val sc = SparkContext(conf)
```

### Why are my write tasks timing out/ failing?

The most common cause of this is that Spark is able to issue write requests much more quickly than
Cassandra can handle them. This can lead to GC issues and build up of hints. If this is the case
with your application, try lowering the number of concurrent writes and the current batch size using
the following options.

   spark.cassandra.output.batch.size.rows
   spark.cassandra.output.concurrent.writes
   
or in versions of the Spark Cassandra Connector greater than or equal to  1.2.0 set

   spark.cassandra.output.throughput_mb_per_sec
   
which will allow you to control the amount of data written to Cassandra per Spark core per second.
   
### Why are my executors throwing `OutOfMemoryException`s while Reading from Cassandra?

This usually means that the size of the partitions you are attempting to create are larger than
the executor's heap can handle. Remember that all of the executors run in the same JVM so the size
of the data is multiplied by the number of executor slots.

To fix this either increase the heap size of the executors `spark.executor.memory`
 or shrink the size of the partitions by decreasing `spark.cassandra.input.split.size_in_mb`
 
### Why can't my spark job find My Application Classes / Anonymous Functions?
 
This occurs when your application code hasn't been placed on the classpath of the Spark Executor. When using
Spark Submit make sure that the jar contains all of the classes and dependencies for running your code. 
To build a fat jar look into using sbt assembly, or look for instructions for your build tool of choice.

If you are not using the recommended approach with Spark Submit, make sure that your dependencies 
have been set in the `SparkConf` using `setJars` or by distributing the jars yourself and modifying 
the executor classpath.
 
### Why don't my case classes work? 
Usually this is because they have been defined within another object/class. Try moving the definition
outside of the scope of other classes.
 
### Why can't my spark job connect to Cassandra?

Check that your Cassandra instance is on and responds to cqlsh. Make sure that the rpc address also
accepts incoming connections on the interface you are setting as `rpc_address` in the cassandra.yaml file.
Make sure that you are setting the `spark.cassandra.connection.host` property to the interface which
the rpc_address is set to.

When troubleshooting Cassandra connections it is sometimes useful to set the rpc_address in the
cassandra.yaml file to `0.0.0.0` so any incoming connection will work.

### How does the connector evaluate number of Spark partitions?

The Connector evaluates the number of Spark partitions by dividing table size estimate by 
`input.split.size_in_mb` value. The resulting number of partitions in never smaller than 
`1 + 2 * SparkContext.defaultParallelism`.

### What does input.split.size_in_mb use to determine size?

Input.split.size_in_mb uses a internal system table in Cassandra ( >= 2.1.5) to determine the size
of the data in Cassandra. The table is called system.size_estimates is not meant to be absolutely accurate 
so there will be some inaccuracy with smaller tables and split sizes.




### Can I contribute to the Spark Cassandra Connector?

YES! Feel free to start a JIRA and detail the changes you would like to make or the feature you
would like to add. We would be happy to discuss it with you and see your work. Feel free to create
a JIRA before you have started any work if you would like feedback on an idea. When you have a branch
that you are satisfied with and passes all the tests (`/dev/run_tests.sh`) make a GitHub PR against
your target Connector Version and set your JIRA to Reviewing.

### Is there a CassandraRDDMock I can use in my tests?

Yes. Please see CassandraRDDMock.scala for the class and CassandraRDDMockSpec.scala for example
usage.

### What should I do if I find a bug? 

Feel free to post a repo on the Mailing List or if you are feeling ambitious file a Jira with
steps for reproduction and we'll get to it as soon as possible. Please remember to include a full
stack trace (if any) and the versions of Spark, The Connector, and Cassandra that you are using.
