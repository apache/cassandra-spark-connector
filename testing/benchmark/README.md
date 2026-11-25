# Connector benchmark

A small end-to-end benchmark of the connector in `spark-shell` (`local[4]`) against one Cassandra node. Its job is to
compare builds, Spark versions, JDKs or settings on the same machine. It does not measure absolute cluster throughput:
everything runs on one host, so only relative differences between runs mean something.

## Workloads

| Workload | What is timed |
|---|---|
| `rdd_write`, `df_write` | writing `BENCH_ROWS` rows with `saveToCassandra` and with the DataFrame writer (one run each) |
| `rdd_full_scan_count`, `df_full_scan_count` | full table scan, count only |
| `rdd_full_scan_rows` | full table scan that reads a column of every row |
| `df_full_scan_agg` | `sum(n)` over the table |
| `df_in_pushdown_5k` | `k IN (5000 keys)` pushed down to Cassandra |
| `df_direct_join`, `rdd_join_with_cassandra` | 50k keys (with the default 500k rows) joined to the table: DataFrame direct join and `joinWithCassandraTable` |
| `df_limit_100` | `limit(100)` |
| `rdd_cache_ser_build_and_read` | cache the table as `MEMORY_ONLY_SER` and read it back (affected by `spark.rdd.compress`) |
| `rdd_shuffle_reduce_by_key`, `df_shuffle_group_by` | a shuffle of Cassandra rows |

There is no token-range-restricted scan: the connector already puts a `token(k)` range on every Spark partition, so
a user `where("token(k) > ...")` adds a second bound that Cassandra rejects ("More than one restriction was found
for the start bound").

Each read workload has one warm-up run and `BENCH_RUNS` timed runs; the median, the minimum, all values and the GC
time per run of the driver JVM (which is also the executor in local mode) are printed.

## Running

1. Build the jar: `./sbt/sbt assembly` (the jar is under `target/out/jvm/scala-2.13.18/spark-cassandra-connector/`).
2. Start one Cassandra node on 127.0.0.1, for example with ccm (Cassandra 5.0 runs on Java 11):

   ```shell
   ccm create bench -v 5.0.9 --config-dir=/tmp/ccm-bench
   ccm populate -n 1 -i 127.0.0. --config-dir=/tmp/ccm-bench
   ccm start --jvm-version=11 --wait-for-binary-proto --config-dir=/tmp/ccm-bench
   ```

3. Run it, once per configuration, with a distinct tag:

   ```shell
   export SPARK_HOME=/path/to/spark-4.3.0-bin-hadoop3 OUT_DIR=/tmp/bench-results
   CONNECTOR_JAR=/path/to/spark-cassandra-connector-assembly-<ver>.jar testing/benchmark/run-benchmark.sh jdk17
   JAVA_HOME=/path/to/jdk21 CONNECTOR_JAR=... testing/benchmark/run-benchmark.sh jdk21
   CONNECTOR_JAR=... testing/benchmark/run-benchmark.sh no-rdd-compress --conf spark.rdd.compress=false
   ```

4. Compare: `testing/benchmark/summarize.sh /tmp/bench-results/*.log` prints a markdown table of the medians.
5. Remove the node: `ccm remove --config-dir=/tmp/ccm-bench`.

Run every configuration at least twice, interleaved (A, B, A, B), and treat differences within the spread of the
repeated runs as noise. Keep the machine otherwise idle and prevent sleep (`caffeinate -i` on macOS).

## What to expect

One run takes about 3 minutes with the defaults (500k rows, 5 timed runs) on a laptop. `run-benchmark.sh` prints
lines like

```
BENCH-INFO: tag=head_jdk17_2 rows=500000 runs=5 java=17.0.20.1 spark=4.3.0 rdd.compress=<default>
BENCH: rdd_write median_ms=17029 min_ms=17029 gc_ms_per_run=84 all=17029
BENCH: df_full_scan_count median_ms=1146 min_ms=1129 gc_ms_per_run=3 all=1169,1146,1129,1136,1160
```

and `summarize.sh` turns the logs into a table of medians in ms, one column per log:

```
| workload | base-jdk17-2 | head-jdk17-2 |
|---|---:|---:|
| rdd_write | 16049 | 17029 |
| df_full_scan_count | 1146 | 1146 |
```

Things that showed up in practice:

- The first configuration run against a freshly started node is slower, writes by up to 50%, while Cassandra
  warms up and compacts. Compare runs from the same round, or discard the first round.
- Writes are timed once per run, so they are the noisiest numbers; the read workloads are medians of 5 runs.
- A failed workload stops the run: the log then has no `BENCH-INFO: done` line and `run-benchmark.sh` exits with 1.
