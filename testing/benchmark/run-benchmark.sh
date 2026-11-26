#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Runs connector-benchmark.scala in spark-shell (local[4]) against a running Cassandra node.
#
# Usage: run-benchmark.sh <tag> [extra spark-shell arguments, e.g. --conf spark.rdd.compress=false]
#
# Environment:
#   SPARK_HOME        Spark 4.x distribution (Scala 2.13)                         (required)
#   CONNECTOR_JAR     connector assembly jar (./sbt/sbt assembly)                 (required)
#   JAVA_HOME         JDK that runs Spark (17, 21 or 25)                          (optional)
#   CASSANDRA_HOST    contact point                                    (default 127.0.0.1)
#   BENCH_ROWS        rows written and scanned                             (default 500000)
#   BENCH_RUNS        timed runs per read workload, after one warm-up         (default 5)
#   OUT_DIR           where <tag>.log is written             (default ./benchmark-results)
#
# Prints the BENCH lines; summarize.sh turns several logs into one table.

set -euo pipefail

if [ $# -lt 1 ]; then
  sed -n '19,33p' "$0" >&2
  exit 2
fi

tag=$1; shift
: "${SPARK_HOME:?set SPARK_HOME}" "${CONNECTOR_JAR:?set CONNECTOR_JAR}"
[ -f "$CONNECTOR_JAR" ] || { echo "CONNECTOR_JAR not found: $CONNECTOR_JAR" >&2; exit 2; }
script_dir=$(cd "$(dirname "$0")" && pwd)
out_dir=${OUT_DIR:-$PWD/benchmark-results}
mkdir -p "$out_dir"
log=$out_dir/$tag.log
if [ -n "${JAVA_HOME:-}" ]; then export PATH=$JAVA_HOME/bin:$PATH; fi

BENCH_TAG=$tag "$SPARK_HOME/bin/spark-shell" \
  --master 'local[4]' --driver-memory 3g \
  --jars "$CONNECTOR_JAR" \
  --conf spark.cassandra.connection.host="${CASSANDRA_HOST:-127.0.0.1}" \
  --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions \
  --conf spark.ui.enabled=false --conf spark.ui.showConsoleProgress=false \
  "$@" \
  -i "$script_dir/connector-benchmark.scala" < /dev/null > "$log" 2>&1 || true

grep '^BENCH' "$log" || true
grep -q '^BENCH-INFO: done' "$log" || { echo "benchmark $tag did not finish, see $log" >&2; exit 1; }
