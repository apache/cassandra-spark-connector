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
require a JDK 17 set in the `JAVA_HOME` env variable (the `java` on your `PATH` must be 
the same version) and [CCM](https://github.com/apache/cassandra-ccm) to be installed 
on your machine. The test framework needs a CCM version that supports `--jvm-version`, 
which the PyPI release does not, so install it from trunk, as CI does:
`pip install git+https://github.com/apache/cassandra-ccm.git@trunk`.
CCM must be on the `PATH` of the shell running sbt.

For Cassandra 5.0 the tests start the nodes on Java 11, so also set `JAVA11_HOME`
to a JDK 11. For example:

```shell
JAVA_HOME=/path/to/jdk17 JAVA11_HOME=/path/to/jdk11 CCM_CASSANDRA_VERSION=5.0.9 ./sbt/sbt connectorIt/test
```

Each parallel test group starts its nodes on its own loopback range
(`127.<group>.0.<node>`). Linux accepts the whole `127.0.0.0/8` range, but macOS only
configures `127.0.0.1`, so add the aliases first (they are removed on reboot):

```shell
for g in $(seq 0 12); do for c in 0 1; do for n in 1 2 3 4; do
  [ "$g.$c.$n" = "0.0.1" ] && continue
  sudo ifconfig lo0 alias 127.$g.$c.$n up
done; done; done
```

The SSL and authentication integration tests use the connector's default cipher suites (TLS 1.3 and
TLS 1.2 ECDHE GCM suites). The former default `TLS_RSA_*` suites are disabled by
[current JDK 17 updates in `jdk.tls.disabledAlgorithms`](https://www.oracle.com/java/technologies/javase/17all-relnotes.html), which is why
they are no longer the default.
 
The most common commands to use when developing the connector are

1. `test` - Runs the the unit tests for the project.
2. `connectorIt/test` - Runs the integration tests with Cassandra (started by CCM) and Spark
3. `package` - Builds the project and produces a runtime jar
4. `publishM2` - Publishes a snapshot of the project to your local maven repository allowing for usage with `--packages` in the spark-shell

The integration tests located in `connector-it/src/test` should
probably be the first place to look for anyone considering adding code.
There are many examples of executing a feature of the connector with
Cassandra and Spark nodes and are the core of our test coverage.

### Merge Path

b2.5 => b3.0 => b3.1 => b3.2 => b3.3 => b3.4 => trunk

New features can be considered for 2.5 as long as they do not break apis.
Once a feature is ready for b2.5, create a feature branch for b3.0 and merge
b2.5 feature branch to b3.0 feature branch. Repeat for b3.1, b3.2, b3.3, b3.4 and trunk. 

Example for imaginary CASSANALYTICS-9999.

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
git checkout -b CASSANALYTICS-9999-b2.5 apache/b2.5
# do the work, commit
git push origin CASSANALYTICS-9999-b2.5
 
# Forward merge on the next version:
git checkout -b CASSANALYTICS-9999-b3.0 apache/b3.0
git merge CASSANALYTICS-9999-b2.5
# Resolve conflict, if any
# Push the new feature branch:
git push origin CASSANALYTICS-9999-b3.0

# Forward merge on the next version:
git checkout -b CASSANALYTICS-9999-b3.1 apache/b3.1
git merge CASSANALYTICS-9999-b3.0
# Resolve conflict, if any
# Push the new feature branch:
git push origin CASSANALYTICS-9999-b3.1

# Repeat for b3.2, b3.3, b3.4
 
# Forward merge on the next version:
git checkout -b CASSANALYTICS-9999-trunk apache/trunk
git merge CASSANALYTICS-9999-b3.2
# Resolve conflict, if any
# Push the new feature branch:
git push origin CASSANALYTICS-9999-trunk
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
`TEST_PARALLEL_TASKS` environment variable. Only applies to `sbt test` & `sbt connectorIt/test` tasks.

### Set Cassandra Test Target
Our CI Build runs through the GitHub Actions Infrastructure and tests on all the builds
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
`~ connectorIt/testOnly foo`. Which would only run the suite you are interested in
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
./bin/spark-shell --repositories file:/Users/yourUser/.m2/repository --packages com.datastax.spark:spark-cassandra-connector_2.13:1.6.0-14-gcfca49e
```
Where you would change the revision `1.6.0-14-gcfca49e` to match the output
of your publish command. 

This same method should work with `publishLocal`
after the merging of [SPARK-12666](https://issues.apache.org/jira/browse/SPARK-12666)

> [!NOTE]
> Jars produced (with sbt 2 each under `target/out/jvm/scala-2.13.18/<sbt project name>/`, e.g. `spark-cassandra-connector/`):
> | Module | JAR Name Example (having suffix as `<last tag>-<commits since>-g<sha>`) |
> | --- | ---: |
> | connector | `spark-cassandra-connector_2.13-3.5.1-19-g37de9b97.jar` |
> | connector | `spark-cassandra-connector-assembly-3.5.1-19-g37de9b97.jar` |
> | driver | `spark-cassandra-connector-driver_2.13-3.5.1-19-g37de9b97.jar` |
> | test-support | `spark-cassandra-connector-test-support_2.13-3.5.1-19-g37de9b97.jar` |
> | root | `root_2.13-3.5.1-19-g37de9b97.jar` |

<details>
<summary>Expand/Collapse to view Troubleshooting steps</summary>
#### Troubleshooting `--packages` with `publishM2`

* `Provided Maven Coordinates must be in the form 'groupId:artifactId:version'`: `--packages`
  takes Maven coordinates, not a jar file name. Pass
  `com.datastax.spark:spark-cassandra-connector_2.13:<revision>` (the revision is the
  `<last tag>-<commits since>-g<sha>` suffix printed by `publishM2`), without the `.jar`.

* `download failed: org.slf4j#slf4j-api;...!slf4j-api.jar` (or another dependency), with
  `[NOT FOUND ] ... local-m2-cache: tried file:/.../.m2/repository/...jar`: Spark's default Ivy
  settings look in `~/.m2/repository` first. If that directory holds only the `.pom` of a
  dependency (no `.jar`, for example left behind by a Maven build that only read its POM),
  Spark stops there instead of falling back to Maven Central. Either delete the offending
  `<artifact>/<version>` directory from `~/.m2/repository` (Maven re-downloads it when needed), or tell
  Spark to take only the connector from `~/.m2` and everything else from Maven Central, by
  creating an Ivy settings file such as `/tmp/ivysettings.xml`:

  ```xml
  <ivysettings>
    <settings defaultResolver="central"/>
    <resolvers>
      <ibiblio name="central" m2compatible="true"/>
      <ibiblio name="local-ours" m2compatible="true" root="file:///Users/yourUser/.m2/repository"/>
    </resolvers>
    <modules>
      <module organisation="com.datastax.spark" resolver="local-ours"/>
    </modules>
  </ivysettings>
  ```

  and starting the shell with `--conf spark.jars.ivySettings=/tmp/ivysettings.xml` in place of
  `--repositories` (keep the same `--packages`).

* The same `download failed ... [NOT FOUND ] ... local-m2-cache` error keeps appearing after
  you deleted the POM-only directories from `~/.m2/repository`: Spark's Ivy cache remembers
  which resolver it used for each dependency
  (`~/.ivy2.5.2/cache/<groupId>/<artifactId>/ivydata-<version>.properties` contains
  `resolver=local-m2-cache`; the cache directory name depends on the Spark version, for
  example `~/.ivy2` for older releases) and keeps trying it. Delete the cache directories of the
  affected dependencies (for example `~/.ivy2.5.2/cache/org.slf4j/slf4j-api` and
  `~/.ivy2.5.2/cache/org.apache.commons/commons-lang3`), or start the shell with a fresh cache
  directory for a one-off check, for example `--conf spark.jars.ivy=/tmp/ivy-clean`.
</details>

### Publishing ScalaDocs

Run the generateDocs script with all the versions to generate docs for

```bash
./generateDocs.sh Version Version Version Version
```
Which will checkout those tags v$Version and run sbt doc for each of them.
The output files will eventually be moved to the gh-pages branch. After the
script has finished inspect the results then if they are good run.

```bash
git add .; git commit
```
