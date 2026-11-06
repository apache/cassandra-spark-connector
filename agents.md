# Agent Notes

This project should be built and tested with Java 17.

Set Java explicitly before running sbt commands in this workspace:

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk
export PATH="$JAVA_HOME/bin:$PATH"
```

Use the repository sbt launcher:

```bash
./sbt/sbt "<sbt task>"
```

## Testcontainers In Airlock

The Testcontainers-based tests require Docker. In the trusted-opencode Airlock container, do not mount or rely on the host Docker socket. Start and use the nested rootless Docker daemon with the Airlock helper:

```bash
start-rootless-docker <command>
```

For Testcontainers, pass `TESTCONTAINERS_HOST_OVERRIDE=127.0.0.1`; without it, tests may receive an unreachable mapped host such as `172.17.0.1` from the nested rootless Docker daemon.

Example targeted test command:

```bash
start-rootless-docker env \
  JAVA_HOME=/usr/lib/jvm/java-17-openjdk \
  PATH=/usr/lib/jvm/java-17-openjdk/bin:$PATH \
  TESTCONTAINERS_HOST_OVERRIDE=127.0.0.1 \
  ./sbt/sbt \
  "connector/Test/testOnly com.datastax.spark.connector.sql.CassandraUpdateModeStreamingSinkSpec"
```

The update-mode streaming sink test is expected to fail until update-mode support is implemented in the connector. A failure like `does not support Update mode` means the environment reached Spark and Cassandra successfully.
