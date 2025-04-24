# Documentation

## Developers Tips

### Getting Started

The Spark Cassandra Connector is built using sbt. There is a premade
launching script for sbt, so it is unnecessary to download it. To invoke
this script you can run `./sbt/sbt` from a clone of this repository.

For information on setting up your clone please follow the [Github 
Help](https://help.github.com/articles/cloning-a-repository/).

Once in the sbt shell you will be able to build and run tests for the
connector without any Spark or Cassandra nodes running. The integration tests 
require a valid path to java home set in the `JAVA_HOME` env variable and 
[CCM](https://github.com/riptano/ccm) to be installed on your machine. 
This can be accomplished with `pip install ccm`.
 
The most common commands to use when developing the connector are

1. `test` - Runs the the unit tests for the project.
2. `it:test` - Runs the integration tests with Cassandra (started by CCM) and Spark
3. `package` - Builds the project and produces a runtime jar
4. `publishM2` - Publishes a snapshot of the project to your local maven repository allowing for usage with `--packages` in the spark-shell

The integration tests located in `connector/src/it` should
probably be the first place to look for anyone considering adding code.
There are many examples of executing a feature of the connector with
Cassandra and Spark nodes and are the core of our test coverage.

### Merge Path

b2.5 => b3.0 => b3.1 => b3.2 => b3.3 => b3.4 => trunk

New features can be considered for 2.5 as long as they do not break apis.
Once a feature is ready for b2.5, create a feature branch for b3.0 and merge
b2.5 feature branch to b3.0 feature branch. Repeat for b3.1, b3.2, b3.3, b3.4 and trunk. 

Example for imaginary SPARKC-9999.

Let's assume that `apache` is `git@github.com:apache/cassandra-spark-connector.git` remote 
and origin is your personal clone.
```shell
$ git remote -v
apache	git@github.com:apache/cassandra-spark-connector.git (fetch)
apache	git@github.com:apache/cassandra-spark-connector.git (push)
...
```

Here is what the work should look like.

```shell
git fetch apache
git checkout -b SPARKC-9999-b2.5 apache/b2.5
# do the work, commit
git push origin SPARKC-9999-b2.5
 
# Forward merge on the next version:
git checkout -b SPARKC-9999-b3.0 apache/b3.0
git merge SPARKC-9999-b2.5
# Resolve conflict, if any
# Push the new feature branch:
git push origin SPARKC-9999-b3.0

# Forward merge on the next version:
git checkout -b SPARKC-9999-b3.1 apache/b3.1
git merge SPARKC-9999-b3.0
# Resolve conflict, if any
# Push the new feature branch:
git push origin SPARKC-9999-b3.1

# Repeat for b3.2, b3.3, b3.4
 
# Forward merge on the next version:
git checkout -b SPARKC-9999-trunk apache/trunk
git merge SPARKC-9999-b3.2
# Resolve conflict, if any
# Push the new feature branch:
git push origin SPARKC-9999-trunk
```
Repeat for b3.3, b3.4, etc

### Sub-Projects

The connector currently contains several sub-projects

#### connector
This sub-project contains all of the actual connector code and is where
any new features or tests should go. This Scala project also contains the
Java API and related code. Anything related to the actual connecting or modification
of Java Driver code belongs in the next module

#### driver
All of the code relating to the Java Driver. Connection factories, row transformers
anything which could be used for any application even if Spark is not involved.


#### test-support
CCM Wrapper code. Much of this code is based on the Apache Cassandra Java Driver's test code. 
Includes code for spawning CCM as well as several modes for launching clusters
while testing. Together this also defines which tests require seperate clusters to
run and the parallelization used while running tests.

### Test Parallelization

In order to limit the number of test groups running simultaneously use the
`TEST_PARALLEL_TASKS` environment variable. Only applies to `sbt test` tasks.

### Set Cassandra Test Target
Our CI Build runs through the Datastax Infrastructure and tests on all the builds
listed in build.yaml. In addition, the _test-support_ module supports Cassandra
or other CCM Compatible installations.

If using SBT you can set
`CCM_CASSANDRA_VERSION` to propagate a version for CCM to use during tests.

If you are running tests through IntelliJ or through an alternative framework (jUnit)
set the system property `ccm.version` to the version you like.

### CCM Modes
The integration tests have a variety of modes which can be set with `CCM_CLUSTER_MODE`

* Debug - Use to preserve logs of running CCM Clusters as well as the cluster directories themselves
* Standard - Starts a new cluster which is cleaned up entirely on finish the test run
* Developer - Does not clean up cluster on test completion, can be used when running a test multiple times for faster iteration

### Continuous Testing

It's often useful when implementing new features to have the tests run
in a loop on code change. Sbt provides a method for this by using the
`~` operator. With this `~ test` will run the unit tests every time a
change in the source code is detected. This is often useful to use in
conjunction with `testOnly` which runs a single test. So if a new feature
were being added to the integration suite `foo` you may want to run
`~ it:testOnly foo`. Which would only run the suite you are interested in
on a loop while you are modifying the connector code. Use this in conjunction
with "Developer" CCM Mode for the fastest test iteration.

### Packaging

The `spark-shell` and `spark-submit` are able to use local caches to load
libraries and this can be taken advantage of by the SCC. For example
if you wanted to test the maven artifacts produced for your current build
you could run `publishM2` which would generate the needed artifacts and
pom in your local cache. You can then reference this from `spark-shell`
or `spark-submit` using the following code 
```bash
./bin/spark-shell --repositories file:/Users/yourUser/.m2/repository --packages com.datastax.spark:spark-cassandra-connector_2.10:1.6.0-14-gcfca49e
```
Where you would change the revision `1.6.0-14-gcfca49e` to match the output
of your publish command. 

This same method should work with `publishLocal`
after the merging of [SPARK-12666](https://issues.apache.org/jira/browse/SPARK-12666)


### Publishing ScalaDocs

Run the generateDocs script with all the versions to generate docs for

```bash
./generateDocs Version Version Version Version
```
Which will checkout those tags v$Version and run sbt doc for each of them.
The output files will eventually be moved to the gh-pages branch. After the
script has finished inspect the results then if they are good run.

```bash
git add .; git commit
```
