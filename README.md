# Apache Cassandra Spark Connector

*Lightning-fast cluster computing with Apache Spark&trade; and Apache Cassandra&reg;.*

[![CI](https://github.com/apache/cassandra-spark-connector/actions/workflows/main.yml/badge.svg?branch=trunk)](https://github.com/apache/cassandra-spark-connector/actions?query=branch%3Atrunk)

## Quick Links

| What       | Where                                                                                                                                                                                                                                                                                                               |
| ---------- |---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Community  | Chat with us at [Apache Cassandra](https://cassandra.apache.org/_/community.html#discussions)                                                                                                                                                                                                                       |
| Scala Docs | Most Recent Release (3.5.1): [Connector API docs](https://apache.github.io/cassandra-spark-connector/ApiDocs/3.5.1/connector/com/datastax/spark/connector/index.html), [Connector Driver docs](https://apache.github.io/cassandra-spark-connector/ApiDocs/3.5.1/driver/com/datastax/spark/connector/index.html) |
| Latest Production Release | [3.5.1](https://search.maven.org/artifact/com.datastax.spark/spark-cassandra-connector_2.12/3.5.1/jar)                                                                                                                                                                                                              |

## News
### 3.5.1
 - The latest release of the Spark-Cassandra-Connector introduces support for vector types, greatly enhancing its capabilities. This new feature allows developers to seamlessly integrate and work with Cassandra 5.0 and Astra vectors within the Spark ecosystem. By supporting vector types, the connector now provides insights into AI and Retrieval-Augmented Generation (RAG) data, enabling more advanced and efficient data processing and analysis.
 
## Features

This library lets you expose Cassandra tables as Spark RDDs and Datasets/DataFrames, write
Spark RDDs and Datasets/DataFrames to Cassandra tables, and execute arbitrary CQL queries
in your Spark applications.

 - Compatible with Apache Cassandra version 2.1 or higher (see table below)
 - Compatible with Apache Spark 1.0 through 3.5 ([see table below](#version-compatibility))
 - Compatible with Scala 2.11, 2.12 and 2.13
 - Exposes Cassandra tables as Spark RDDs and Datasets/DataFrames
 - Maps table rows to CassandraRow objects or tuples
 - Offers customizable object mapper for mapping rows to objects of user-defined classes
 - Saves RDDs back to Cassandra by implicit `saveToCassandra` call
 - Delete rows and columns from cassandra by implicit `deleteFromCassandra` call
 - Join with a subset of Cassandra data using `joinWithCassandraTable` call for RDDs, and
   optimizes join with data in Cassandra when using Datasets/DataFrames
 - Partition RDDs according to Cassandra replication using `repartitionByCassandraReplica` call
 - Converts data types between Cassandra and Scala
 - Supports all Cassandra data types including collections
 - Filters rows on the server side via the CQL `WHERE` clause
 - Allows for execution of arbitrary CQL statements
 - Plays nice with Cassandra Virtual Nodes
 - Could be used in all languages supporting Datasets/DataFrames API: Python, R, etc.

## Version Compatibility

The connector project has several branches, each of which map into different
supported versions of  Spark and Cassandra. For previous releases the branch is
named "bX.Y" where X.Y is the major+minor version; for example the "b1.6" branch
corresponds to the 1.6 release. The "trunk" branch will normally contain
development for the next connector release in progress.

Currently, the following branches are actively supported: 
3.5.x ([trunk](https://github.com/apache/cassandra-spark-connector/tree/trunk)),
3.4.x ([b3.4](https://github.com/apache/cassandra-spark-connector/tree/b3.4)),
3.3.x ([b3.2](https://github.com/apache/cassandra-spark-connector/tree/b3.3)),
3.2.x ([b3.2](https://github.com/apache/cassandra-spark-connector/tree/b3.2)),
3.1.x ([b3.1](https://github.com/apache/cassandra-spark-connector/tree/b3.1)),
3.0.x ([b3.0](https://github.com/apache/cassandra-spark-connector/tree/b3.0)) and 
2.5.x ([b2.5](https://github.com/apache/cassandra-spark-connector/tree/b2.5)).

| Connector | Spark         | Cassandra                  | Cassandra Java Driver | Minimum Java Version | Supported Scala Versions |
|-----------|---------------|----------------------------|-----------------------|----------------------|--------------------------|
| 3.5.1     | 3.5           | 2.1.5*, 2.2, 3.x, 4.x, 5.0 | 4.18.1                | 8                    | 2.12, 2.13               |  
| 3.5       | 3.5           | 2.1.5*, 2.2, 3.x, 4.x      | 4.13                  | 8                    | 2.12, 2.13               |  
| 3.4       | 3.4           | 2.1.5*, 2.2, 3.x, 4.x      | 4.13                  | 8                    | 2.12, 2.13               |
| 3.3       | 3.3           | 2.1.5*, 2.2, 3.x, 4.x      | 4.13                  | 8                    | 2.12                     |
| 3.2       | 3.2           | 2.1.5*, 2.2, 3.x, 4.0      | 4.13                  | 8                    | 2.12                     |
| 3.1       | 3.1           | 2.1.5*, 2.2, 3.x, 4.0      | 4.12                  | 8                    | 2.12                     |
| 3.0       | 3.0           | 2.1.5*, 2.2, 3.x, 4.0      | 4.12                  | 8                    | 2.12                     |
| 2.5       | 2.4           | 2.1.5*, 2.2, 3.x, 4.0      | 4.12                  | 8                    | 2.11, 2.12               |
| 2.4.2     | 2.4           | 2.1.5*, 2.2, 3.x           | 3.0                   | 8                    | 2.11, 2.12               |
| 2.4       | 2.4           | 2.1.5*, 2.2, 3.x           | 3.0                   | 8                    | 2.11                     |
| 2.3       | 2.3           | 2.1.5*, 2.2, 3.x           | 3.0                   | 8                    | 2.11                     |
| 2.0       | 2.0, 2.1, 2.2 | 2.1.5*, 2.2, 3.x           | 3.0                   | 8                    | 2.10, 2.11               |
| 1.6       | 1.6           | 2.1.5*, 2.2, 3.0           | 3.0                   | 7                    | 2.10, 2.11               |
| 1.5       | 1.5, 1.6      | 2.1.5*, 2.2, 3.0           | 3.0                   | 7                    | 2.10, 2.11               |
| 1.4       | 1.4           | 2.1.5*                     | 2.1                   | 7                    | 2.10, 2.11               |
| 1.3       | 1.3           | 2.1.5*                     | 2.1                   | 7                    | 2.10, 2.11               |
| 1.2       | 1.2           | 2.1, 2.0                   | 2.1                   | 7                    | 2.10, 2.11               |
| 1.1       | 1.1, 1.0      | 2.1, 2.0                   | 2.1                   | 7                    | 2.10, 2.11               |
| 1.0       | 1.0, 0.9      | 2.0                        | 2.0                   | 7                    | 2.10, 2.11               |

**Compatible with 2.1.X where X >= 5*

## Hosted API Docs
API documentation for the Scala and Java interfaces are available online:

### 3.5.1
* [Spark-Cassandra-Connector](https://apache.github.io/cassandra-spark-connector/ApiDocs/3.5.1/connector/com/datastax/spark/connector/index.html)

### 3.5.0
* [Spark-Cassandra-Connector](https://apache.github.io/cassandra-spark-connector/ApiDocs/3.5.0/connector/com/datastax/spark/connector/index.html)

### 3.4.1
* [Spark-Cassandra-Connector](https://apache.github.io/cassandra-spark-connector/ApiDocs/3.4.1/connector/com/datastax/spark/connector/index.html)

### 3.3.0
* [Spark-Cassandra-Connector](https://apache.github.io/cassandra-spark-connector/ApiDocs/3.3.0/connector/com/datastax/spark/connector/index.html)

### 3.2.0
* [Spark-Cassandra-Connector](https://apache.github.io/cassandra-spark-connector/ApiDocs/3.2.0/connector/com/datastax/spark/connector/index.html)

### 3.1.0
* [Spark-Cassandra-Connector](https://apache.github.io/cassandra-spark-connector/ApiDocs/3.1.0/connector/com/datastax/spark/connector/index.html)

### 3.0.1
* [Spark-Cassandra-Connector](https://apache.github.io/cassandra-spark-connector/ApiDocs/3.0.1/connector/com/datastax/spark/connector/index.html)

### 2.5.2
* [Spark-Cassandra-Connector](https://apache.github.io/cassandra-spark-connector/ApiDocs/2.5.2/connector/#package)

### 2.4.2
* [Spark-Cassandra-Connector](http://apache.github.io/cassandra-spark-connector/ApiDocs/2.4.2/spark-cassandra-connector/)
* [Embedded-Cassandra](http://apache.github.io/cassandra-spark-connector/ApiDocs/2.4.2/spark-cassandra-connector-embedded/)

## Download

This project is available on the Maven Central Repository.
For SBT to download the connector binaries, sources and javadoc, put this in your project
SBT config:

    libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "3.5.1"

* The default Scala version for Spark 3.0+ is 2.12 please choose the appropriate build. See the
[FAQ](doc/FAQ.md) for more information.

## Building
See [Building And Artifacts](doc/12_building_and_artifacts.md)

## Documentation

  - [Quick-start guide](doc/0_quick_start.md)
  - [Connecting to Cassandra](doc/1_connecting.md)
  - [Loading datasets from Cassandra](doc/2_loading.md)
  - [Server-side data selection and filtering](doc/3_selection.md)   
  - [Working with user-defined case classes and tuples](doc/4_mapper.md)
  - [Saving and deleting datasets to/from Cassandra](doc/5_saving.md)
  - [Customizing the object mapping](doc/6_advanced_mapper.md)
  - [Using Connector in Java](doc/7_java_api.md)
  - [Spark Streaming with Cassandra](doc/8_streaming.md)
  - [The spark-cassandra-connector-embedded Artifact](doc/10_embedded.md)
  - [Performance monitoring](doc/11_metrics.md)
  - [Building And Artifacts](doc/12_building_and_artifacts.md)
  - [The Spark Shell](doc/13_spark_shell.md)
  - [DataFrames](doc/14_data_frames.md)
  - [Python](doc/15_python.md)
  - [Partitioner](doc/16_partitioning.md)
  - [Submitting applications](doc/17_submitting.md)
  - [Frequently Asked Questions](doc/FAQ.md)
  - [Configuration Parameter Reference Table](doc/reference.md)
  - [Tips for Developing the Spark Cassandra Connector](doc/developers.md)

## Online Training

In [DS320: Analytics with Spark](https://www.youtube.com/watch?v=D6PMEQAfjeU&list=PL2g2h-wyI4Sp0bqsw_IRYu1M4aPkt045x), you will learn how to effectively and efficiently solve analytical problems with Apache Spark, Apache Cassandra, and DataStax Enterprise. You will learn about Spark API, Spark-Cassandra Connector, Spark SQL, Spark Streaming, and crucial performance optimization techniques.

## Community

### Reporting Bugs

New issues may be reported using [JIRA](https://issues.apache.org/jira/projects/CASSANALYTICS). Please include
all relevant details including versions of Spark, Spark Cassandra Connector, Cassandra and/or DSE. A minimal
reproducible case with sample code is ideal.

### Mailing List

Questions and requests for help may be submitted to the [user mailing list](https://cassandra.apache.org/_/community.html#discussions).


## Q/A Exchange

For community help see https://cassandra.apache.org/_/community.html

## Contributing

To protect the community, all contributors are required to sign the Apache Software Foundation's [Contribution License Agreement](https://www.apache.org/licenses/contributor-agreements.html#clas).

[Tips for Developing the Spark Cassandra Connector](doc/developers.md)

Checklist for contributing changes to the project:
* Create a [CASSANALYTICS JIRA](https://issues.apache.org/jira/projects/CASSANALYTICS)
* Make sure that all unit tests and integration tests pass
* Add an appropriate entry at the top of CHANGES.txt
* If the change has any end-user impacts, also include changes to the ./doc files as needed
* Prefix the pull request description with the JIRA number, for example: "SPARKC-123: Fix the ..."
* Open a pull-request on GitHub and await review

Old issues from before the donation to the ASF and the Apache Cassandra project can be found in this [SPARKC JIRA](https://datastax-oss.atlassian.net/projects/SPARKC/issues)

## Testing
To run unit and integration tests:

    ./sbt/sbt test
    ./sbt/sbt it:test

Note that the integration tests require [CCM](https://github.com/apache/cassandra-ccm) to be installed on your machine.
See [Tips for Developing the Spark Cassandra Connector](doc/developers.md) for details.

By default, integration tests start up a separate, single Cassandra instance and run Spark in local mode.
It is possible to run integration tests with your own Spark cluster.
First, prepare a jar with testing code:

    ./sbt/sbt test:package

Then copy the generated test jar to your Spark nodes and run:    

    export IT_TEST_SPARK_MASTER=<Spark Master URL>
    ./sbt/sbt it:test

## Generating Documents
To generate the Reference Document use

    ./sbt/sbt spark-cassandra-connector-unshaded/run (outputLocation)

outputLocation defaults to doc/reference.md

## License

Copyright 2014, Apache Software Foundation

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

Apache Cassandra, Apache Spark, Apache, Tomcat, Lucene, Solr, Hadoop, Spark, TinkerPop, and Cassandra are trademarks of the Apache Software Foundation or its subsidiaries in Canada, the United States and/or other countries.