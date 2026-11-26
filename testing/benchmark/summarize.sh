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

# Prints a markdown table of the median times (ms) from benchmark logs, one column per log.
#
# Usage: summarize.sh <log> [<log> ...]     e.g. summarize.sh benchmark-results/*.log

set -euo pipefail
[ $# -ge 1 ] || { echo "usage: $0 <log> [<log> ...]" >&2; exit 2; }

awk '
  FNR == 1 { col = FILENAME; sub(/.*\//, "", col); sub(/\.log$/, "", col); cols[++ncols] = col }
  /^BENCH: / {
    name = $2
    for (i = 3; i <= NF; i++) if ($i ~ /^median_ms=/) { v = $i; sub(/median_ms=/, "", v) }
    if (!(name in seen)) { seen[name] = 1; names[++nnames] = name }
    val[name, col] = v
  }
  END {
    line = "| workload |"; sep = "|---|"
    for (c = 1; c <= ncols; c++) { line = line " " cols[c] " |"; sep = sep "---:|" }
    print line; print sep
    for (n = 1; n <= nnames; n++) {
      line = "| " names[n] " |"
      for (c = 1; c <= ncols; c++) line = line " " ((names[n], cols[c]) in val ? val[names[n], cols[c]] : "") " |"
      print line
    }
  }' "$@"
