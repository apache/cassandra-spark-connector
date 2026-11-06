# Structured Streaming Update Mode

## Goal

Allow the Cassandra Structured Streaming sink to accept Spark `Update` output
mode so windowed aggregates can be written on every microbatch instead of
waiting for the watermark to close each window.

Repeated writes are intentional. The workload has relatively few groups, and
Cassandra can absorb repeated upserts for the same aggregate row.

## Core Use Case

The initial feature test will use clickstream data only:

1. Read a streaming clickstream source.
2. Extract `content_id`, `event`, and `event_time`.
3. Apply a watermark to `event_time`.
4. Group by `content_id` and a time window.
5. Aggregate page views and clicks.
6. Write the aggregate using `OutputMode.Update()` to Cassandra.

The test will not include article metadata, a stream-stream join, publish-time
filtering, or state-store join behavior. Those are separate concerns and will
be covered by a later end-to-end integration test.

## Minimal Cassandra Schema

```sql
CREATE TABLE IF NOT EXISTS streaming_metrics.clickstream_metrics (
    content_id text,
    bucket_start_time timestamp,
    page_views bigint,
    clicks bigint,
    updated_at_time timestamp,

    PRIMARY KEY (content_id, bucket_start_time)
)
WITH CLUSTERING ORDER BY (bucket_start_time DESC)
AND default_time_to_live = 259200;
```

The primary key matches the logical identity of a windowed aggregate. Each
later update for the same `content_id` and `bucket_start_time` must replace the
previous values in Cassandra.

## Implementation Plan

1. Confirm the Spark 3.5.8 streaming write API used by the current build.
2. Add Spark 3.5.8's `SupportsStreamingUpdateAsAppend` declaration to the
   existing V2 write builder. Cassandra's normal upserts provide the required
   update behavior.
3. Leave append-mode behavior unchanged.
4. Reuse the existing `CassandraDriverDataWriter` and `TableWriter`; no new
   Cassandra write protocol is required.
5. Keep normal scalar aggregate columns as ordinary Cassandra upserts.
6. Preserve the current Scala 2.12/2.13 and JVM 8 publication targets for this
   feature. Java 25 and Spark 4.2.1 can be handled as a later platform update.

## Acceptance Test

The Testcontainers integration test should prove all of the following:

- The query starts successfully with `OutputMode.Update()`.
- The query remains active without a streaming sink output-mode exception.
- Rows are written before the watermark finalizes the window.
- The same Cassandra primary key is written more than once.
- The aggregate value changes after later input arrives.
- Cassandra contains the latest value after the query has processed updates.
- Existing append-mode streaming tests continue to pass.

Complete mode is intentionally out of scope. Writing the complete aggregate
result would not remove Cassandra rows that disappeared from Spark state, so it
would not provide complete-snapshot semantics without a separate deletion
protocol.

The current ignored update-mode test is a useful starting point, but its
unwindowed `groupBy` only proves that update mode can start. It should be
changed to model a deterministic clickstream/window aggregate and verify
successive values for the same key.

## Write Semantics

Spark creates writers for each microbatch. The existing writer buffers and
flushes statements when each partition writer closes, so update mode naturally
produces writes at microbatch boundaries.

The connector currently has no-op streaming commit and abort methods. That is
acceptable for this feature because aggregate rows use idempotent scalar
upserts. Counter and collection mutation modes must not be treated as covered
by this guarantee because retries can repeat non-idempotent mutations.

If multiple aggregate streams later write different columns into the same
Cassandra row, the handling of omitted or null columns must be verified before
combining them. The core test should avoid that complication.

## Follow-Up Integration Test

After the core sink behavior is working, add a separate test for the complete
workflow:

- Kafka-like metadata stream
- Kafka-like clickstream stream
- State-store or stream-stream join behavior
- Publish timestamp filtering
- 10-minute, 1-hour, and 6-hour metrics
- The complete `clickstream_metrics` table schema

That test should validate application-level behavior, while the core test
remains a focused regression test for Cassandra update-mode support.
