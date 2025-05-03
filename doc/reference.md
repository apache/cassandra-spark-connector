# Configuration Reference


    
## Alternative Connection Configuration Options
<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Description</th></tr>
<tr>
  <td><code>spark.cassandra.connection.config.cloud.path</code></td>
  <td>None</td>
  <td>Path to Secure Connect Bundle to be used for this connection. Accepts URLs and references to files
distributed via spark.files (--files) setting.<br/>
Provided URL must by accessible from each executor.<br/>
Using spark.files is recommended as it relies on Spark to distribute the bundle to every executor and
leverages Spark capabilities to access files located in distributed file systems like HDFS, S3, etc.
For example, to use a bundle located in HDFS in spark-shell:

    spark-shell --conf spark.files=hdfs:///some_dir/bundle.zip \
       --conf spark.cassandra.connection.config.cloud.path=bundle.zip \
       --conf spark.cassandra.auth.username=<name> \
       --conf spark.cassandra.auth.password=<pass> ...

</td>
</tr>
<tr>
  <td><code>spark.cassandra.connection.config.profile.path</code></td>
  <td>None</td>
  <td>Specifies a default Java Driver 4.0 Profile file to be used for this connection. Accepts
URLs and references to files distributed via spark.files (--files) setting.</td>
</tr>
</table>


## Cassandra Authentication Parameters
<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Description</th></tr>
<tr>
  <td><code>spark.cassandra.auth.conf.factory</code></td>
  <td>DefaultAuthConfFactory</td>
  <td>Name of a Scala module or class implementing AuthConfFactory providing custom authentication configuration</td>
</tr>
</table>


## Cassandra Connection Parameters
<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Description</th></tr>
<tr>
  <td><code>spark.cassandra.connection.compression</code></td>
  <td>NONE</td>
  <td>Compression to use (LZ4, SNAPPY or NONE)</td>
</tr>
<tr>
  <td><code>spark.cassandra.connection.factory</code></td>
  <td>DefaultConnectionFactory</td>
  <td>Name of a Scala module or class implementing
CassandraConnectionFactory providing connections to the Cassandra cluster</td>
</tr>
<tr>
  <td><code>spark.cassandra.connection.host</code></td>
  <td>localhost</td>
  <td>Contact point to connect to the Cassandra cluster. A comma separated list
may also be used. Ports may be provided but are optional. If Ports are missing spark.cassandra.connection.port will
 be used ("127.0.0.1:9042,192.168.0.1:9051")
      </td>
</tr>
<tr>
  <td><code>spark.cassandra.connection.keepAliveMS</code></td>
  <td>3600000</td>
  <td>Period of time to keep unused connections open</td>
</tr>
<tr>
  <td><code>spark.cassandra.connection.localConnectionsPerExecutor</code></td>
  <td>None</td>
  <td>Number of local connections set on each Executor JVM. Defaults to the number
 of available CPU cores on the local node if not specified and not in a Spark Env</td>
</tr>
<tr>
  <td><code>spark.cassandra.connection.localDC</code></td>
  <td>None</td>
  <td>The local DC to connect to (other nodes will be ignored)</td>
</tr>
<tr>
  <td><code>spark.cassandra.connection.port</code></td>
  <td>9042</td>
  <td>Cassandra native connection port, will be set to all hosts if no individual ports are given</td>
</tr>
<tr>
  <td><code>spark.cassandra.connection.quietPeriodBeforeCloseMS</code></td>
  <td>0</td>
  <td>The time in seconds that must pass without any additional request after requesting connection close (see Netty quiet period)</td>
</tr>
<tr>
  <td><code>spark.cassandra.connection.reconnectionDelayMS.max</code></td>
  <td>60000</td>
  <td>Maximum period of time to wait before reconnecting to a dead node</td>
</tr>
<tr>
  <td><code>spark.cassandra.connection.reconnectionDelayMS.min</code></td>
  <td>1000</td>
  <td>Minimum period of time to wait before reconnecting to a dead node</td>
</tr>
<tr>
  <td><code>spark.cassandra.connection.remoteConnectionsPerExecutor</code></td>
  <td>None</td>
  <td>Minimum number of remote connections per Host set on each Executor JVM. Default value is
 estimated automatically based on the total number of executors in the cluster</td>
</tr>
<tr>
  <td><code>spark.cassandra.connection.resolveContactPoints</code></td>
  <td>true</td>
  <td>Controls, if we need to resolve contact points at start (true), or at reconnection (false).
Helpful for usage with Kubernetes or other systems with dynamic endpoints which may change
while the application is running.</td>
</tr>
<tr>
  <td><code>spark.cassandra.connection.timeoutBeforeCloseMS</code></td>
  <td>15000</td>
  <td>The time in seconds for all in-flight connections to finish after requesting connection close</td>
</tr>
<tr>
  <td><code>spark.cassandra.connection.timeoutMS</code></td>
  <td>5000</td>
  <td>Maximum period of time to attempt connecting to a node</td>
</tr>
<tr>
  <td><code>spark.cassandra.query.retry.count</code></td>
  <td>60</td>
  <td>Number of times to retry a timed-out query
Setting this to -1 means unlimited retries
      </td>
</tr>
<tr>
  <td><code>spark.cassandra.read.timeoutMS</code></td>
  <td>120000</td>
  <td>Maximum period of time to wait for a read to return </td>
</tr>
</table>


## Cassandra Datasource Parameters
<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Description</th></tr>
<tr>
  <td><code>spark.cassandra.sql.inClauseToFullScanConversionThreshold</code></td>
  <td>20000000</td>
  <td>Queries with `IN` clause(s) are not converted to JoinWithCassandraTable operation if the size of cross
product of all `IN` value sets exceeds this value. It is meant to stop conversion for huge `IN` values sets
that may cause memory problems. If this limit is exceeded full table scan is performed.
This setting takes precedence over spark.cassandra.sql.inClauseToJoinConversionThreshold.
Query `select * from t where k1 in (1,2,3) and k2 in (1,2) and k3 in (1,2,3,4)` has 3 sets of `IN` values.
Cross product of these values has size of 24.
         </td>
</tr>
<tr>
  <td><code>spark.cassandra.sql.inClauseToJoinConversionThreshold</code></td>
  <td>2500</td>
  <td>Queries with `IN` clause(s) are converted to JoinWithCassandraTable operation if the size of cross
product of all `IN` value sets exceeds this value. To disable `IN` clause conversion, set this setting to 0.
Query `select * from t where k1 in (1,2,3) and k2 in (1,2) and k3 in (1,2,3,4)` has 3 sets of `IN` values.
Cross product of these values has size of 24.
         </td>
</tr>
<tr>
  <td><code>spark.cassandra.sql.pushdown.additionalClasses</code></td>
  <td></td>
  <td>A comma separated list of classes to be used (in order) to apply additional
 pushdown rules for Cassandra Dataframes. Classes must implement CassandraPredicateRules
      </td>
</tr>
<tr>
  <td><code>spark.cassandra.table.size.in.bytes</code></td>
  <td>None</td>
  <td>Used by DataFrames Internally, will be updated in a future release to
retrieve size from Cassandra. Can be set manually now</td>
</tr>
<tr>
  <td><code>spark.sql.dse.search.autoRatio</code></td>
  <td>0.03</td>
  <td>When Search Predicate Optimization is set to auto, Search optimizations will be preformed if this parameter * the total number of rows is greater than the number of rows to be returned by the solr query</td>
</tr>
<tr>
  <td><code>spark.sql.dse.search.enableOptimization</code></td>
  <td>auto</td>
  <td>Enables SparkSQL to automatically replace Cassandra Pushdowns with DSE Search
Pushdowns utilizing lucene indexes. Valid options are On, Off, and Auto. Auto enables
optimizations when the solr query will pull less than spark.sql.dse.search.autoRatio * the
total table record count</td>
</tr>
</table>


## Cassandra Datasource Table Options
<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Description</th></tr>
<tr>
  <td><code>directJoinSetting</code></td>
  <td>auto</td>
  <td>Acceptable values, "on", "off", "auto"
"on" causes a direct join to happen if possible regardless of size ratio.
"off" disables direct join even when possible
"auto" only does a direct join when the size ratio is satisfied see directJoinSizeRatio
      </td>
</tr>
<tr>
  <td><code>directJoinSizeRatio</code></td>
  <td>0.9</td>
  <td>
 Sets the threshold on when to perform a DirectJoin in place of a full table scan. When
 the size of the (CassandraSource * thisParameter) > The other side of the join, A direct
 join will be performed if possible.
      </td>
</tr>
<tr>
  <td><code>ignoreMissingMetaColumns</code></td>
  <td>false</td>
  <td>Acceptable values, "true", "false"
"true" ignore missing meta properties
"false" throw error if missing property is requested
      </td>
</tr>
<tr>
  <td><code>ttl</code></td>
  <td>None</td>
  <td>Surfaces the Cassandra Row TTL as a Column
with the named specified. When reading use ttl.columnName=aliasForTTL. This
can be done for every column with a TTL. When writing use ttl=columnName and the
columname will be used to set the TTL for that row.</td>
</tr>
<tr>
  <td><code>writetime</code></td>
  <td>None</td>
  <td>Surfaces the Cassandra Row Writetime as a Column
with the named specified. When reading use writetime.columnName=aliasForWritetime. This
can be done for every column with a writetime. When Writing use writetime=columnName and the
columname will be used to set the writetime for that row.</td>
</tr>
</table>


## Cassandra SSL Connection Options
<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Description</th></tr>
<tr>
  <td><code>spark.cassandra.connection.ssl.clientAuth.enabled</code></td>
  <td>false</td>
  <td>Enable 2-way secure connection to Cassandra cluster</td>
</tr>
<tr>
  <td><code>spark.cassandra.connection.ssl.enabled</code></td>
  <td>false</td>
  <td>Enable secure connection to Cassandra cluster</td>
</tr>
<tr>
  <td><code>spark.cassandra.connection.ssl.enabledAlgorithms</code></td>
  <td>Set(TLS_RSA_WITH_AES_128_CBC_SHA, TLS_RSA_WITH_AES_256_CBC_SHA)</td>
  <td>SSL cipher suites</td>
</tr>
<tr>
  <td><code>spark.cassandra.connection.ssl.keyStore.password</code></td>
  <td>None</td>
  <td>Key store password</td>
</tr>
<tr>
  <td><code>spark.cassandra.connection.ssl.keyStore.path</code></td>
  <td>None</td>
  <td>Path for the key store being used</td>
</tr>
<tr>
  <td><code>spark.cassandra.connection.ssl.keyStore.type</code></td>
  <td>JKS</td>
  <td>Key store type</td>
</tr>
<tr>
  <td><code>spark.cassandra.connection.ssl.protocol</code></td>
  <td>TLS</td>
  <td>SSL protocol</td>
</tr>
<tr>
  <td><code>spark.cassandra.connection.ssl.trustStore.password</code></td>
  <td>None</td>
  <td>Trust store password</td>
</tr>
<tr>
  <td><code>spark.cassandra.connection.ssl.trustStore.path</code></td>
  <td>None</td>
  <td>Path for the trust store being used</td>
</tr>
<tr>
  <td><code>spark.cassandra.connection.ssl.trustStore.type</code></td>
  <td>JKS</td>
  <td>Trust store type</td>
</tr>
</table>


## Continuous Paging
<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Description</th></tr>
<tr>
  <td><code>spark.dse.continuousPagingEnabled</code></td>
  <td>true</td>
  <td>Enables DSE Continuous Paging which improves scanning performance</td>
</tr>
</table>


## Default Authentication Parameters
<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Description</th></tr>
<tr>
  <td><code>spark.cassandra.auth.password</code></td>
  <td>None</td>
  <td>password for password authentication</td>
</tr>
<tr>
  <td><code>spark.cassandra.auth.username</code></td>
  <td>None</td>
  <td>Login name for password authentication</td>
</tr>
</table>


## Read Tuning Parameters
<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Description</th></tr>
<tr>
  <td><code>spark.cassandra.concurrent.reads</code></td>
  <td>512</td>
  <td>Sets read parallelism for joinWithCassandra tables</td>
</tr>
<tr>
  <td><code>spark.cassandra.input.consistency.level</code></td>
  <td>LOCAL_ONE</td>
  <td>Consistency level to use when reading	</td>
</tr>
<tr>
  <td><code>spark.cassandra.input.fetch.sizeInRows</code></td>
  <td>1000</td>
  <td>Number of CQL rows fetched per driver request</td>
</tr>
<tr>
  <td><code>spark.cassandra.input.metrics</code></td>
  <td>true</td>
  <td>Sets whether to record connector specific metrics on write</td>
</tr>
<tr>
  <td><code>spark.cassandra.input.readsPerSec</code></td>
  <td>None</td>
  <td>Sets max requests or pages per core per second, unlimited by default.</td>
</tr>
<tr>
  <td><code>spark.cassandra.input.split.sizeInMB</code></td>
  <td>512</td>
  <td>Approx amount of data to be fetched into a Spark partition. Minimum number of resulting Spark partitions is <code>1 + 2 * SparkContext.defaultParallelism</code></td>
</tr>
<tr>
  <td><code>spark.cassandra.input.throughputMBPerSec</code></td>
  <td>None</td>
  <td>*(Floating points allowed)* <br> Maximum read throughput allowed
 per single core in MB/s. Effects point lookups as well as full
 scans.</td>
</tr>
</table>


## Write Tuning Parameters
<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Description</th></tr>
<tr>
  <td><code>spark.cassandra.output.batch.grouping.buffer.size</code></td>
  <td>1000</td>
  <td> How many batches per single Spark task can be stored in
memory before sending to Cassandra</td>
</tr>
<tr>
  <td><code>spark.cassandra.output.batch.grouping.key</code></td>
  <td>Partition</td>
  <td>Determines how insert statements are grouped into batches. Available values are
<ul>
  <li> <code> none </code> : a batch may contain any statements </li>
  <li> <code> replica_set </code> : a batch may contain only statements to be written to the same replica set </li>
  <li> <code> partition </code> : a batch may contain only statements for rows sharing the same partition key value </li>
</ul>
</td>
</tr>
<tr>
  <td><code>spark.cassandra.output.batch.size.bytes</code></td>
  <td>1024</td>
  <td>Maximum total size of the batch in bytes. Overridden by
spark.cassandra.output.batch.size.rows
    </td>
</tr>
<tr>
  <td><code>spark.cassandra.output.batch.size.rows</code></td>
  <td>None</td>
  <td>Number of rows per single batch. The default is 'auto'
which means the connector will adjust the number
of rows based on the amount of data
in each row</td>
</tr>
<tr>
  <td><code>spark.cassandra.output.concurrent.writes</code></td>
  <td>5</td>
  <td>Maximum number of batches executed in parallel by a
 single Spark task</td>
</tr>
<tr>
  <td><code>spark.cassandra.output.consistency.level</code></td>
  <td>LOCAL_QUORUM</td>
  <td>Consistency level for writing</td>
</tr>
<tr>
  <td><code>spark.cassandra.output.ifNotExists</code></td>
  <td>false</td>
  <td>Determines that the INSERT operation is not performed if a row with the same primary
key already exists. Using the feature incurs a performance hit.</td>
</tr>
<tr>
  <td><code>spark.cassandra.output.ignoreNulls</code></td>
  <td>false</td>
  <td> In Cassandra >= 2.2 null values can be left as unset in bound statements. Setting
this to true will cause all null values to be left as unset rather than bound. For
finer control see the CassandraOption class</td>
</tr>
<tr>
  <td><code>spark.cassandra.output.metrics</code></td>
  <td>true</td>
  <td>Sets whether to record connector specific metrics on write</td>
</tr>
<tr>
  <td><code>spark.cassandra.output.throughputMBPerSec</code></td>
  <td>None</td>
  <td>*(Floating points allowed)* <br> Maximum write throughput allowed
 per single core in MB/s. <br> Limit this on long (+8 hour) runs to 70% of your max throughput
 as seen on a smaller job for stability</td>
</tr>
<tr>
  <td><code>spark.cassandra.output.timestamp</code></td>
  <td>0</td>
  <td>Timestamp (microseconds since epoch) of the write. If not specified, the time that the
 write occurred is used. A value of 0 means time of write.</td>
</tr>
<tr>
  <td><code>spark.cassandra.output.ttl</code></td>
  <td>0</td>
  <td>Time To Live(TTL) assigned to writes to Cassandra. A value of 0 means no TTL</td>
</tr>
</table>
