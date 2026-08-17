package com.datastax.spark.connector.sql

import java.nio.file.Files

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.streaming.OutputMode
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatest.time.{Seconds, Span}
import org.testcontainers.cassandra.CassandraContainer
import org.testcontainers.utility.DockerImageName

class CassandraStreamingSinkTcSpec extends FlatSpec with Matchers with Eventually with BeforeAndAfterAll {

  private val keyspace = "streaming_sink_tc"
  private var cassandra: CassandraContainer = _
  private var spark: SparkSession = _
  private var connector: CassandraConnector = _

  override protected def beforeAll(): Unit = {
    cassandra = new CassandraContainer(DockerImageName.parse("cassandra:5.0.2"))
    cassandra.start()

    val host = cassandra.getHost
    val port = cassandra.getMappedPort(9042)
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("cassandra-streaming-sink-tc-spec")
      .set("spark.ui.enabled", "false")
      .set("spark.cassandra.connection.host", host)
      .set("spark.cassandra.connection.port", port.toString)
      .set("spark.cassandra.connection.localDC", "datacenter1")

    spark = SparkSession.builder().config(conf).getOrCreate()
    connector = CassandraConnector(conf)

    val session = CqlSession.builder()
      .addContactPoint(new java.net.InetSocketAddress(host, port))
      .withLocalDatacenter("datacenter1")
      .build()
    try {
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH replication = {'class':'SimpleStrategy','replication_factor':1}")
      session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.kv (key int, value int, PRIMARY KEY (key))")
      session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.empty (key int, value int, PRIMARY KEY (key))")
    } finally {
      session.close()
    }
  }

  override protected def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
    if (cassandra != null) {
      cassandra.stop()
    }
  }

  "CassandraStreamingSink with Testcontainers" should "write rows from a stream" in {
    val checkpointDir = Files.createTempDirectory("ks-tc")

    val source = spark
      .readStream
      .format("org.apache.spark.sql.datastax.test.monotonic")
      .load()
      .withColumn("value", org.apache.spark.sql.functions.col("key") + 1)
      .withColumn("key", org.apache.spark.sql.functions.col("key"))

    val query = source.writeStream
      .option("checkpointLocation", checkpointDir.toString)
      .cassandraFormat("kv", keyspace)
      .outputMode(OutputMode.Append())
      .start()

    try {
      eventually(timeout(Span(30, Seconds))) {
        query.exception should be(None)
        query.lastProgress.batchId should be > 2L
      }
    } finally {
      query.stop()
    }

    val rows = connector.withSessionDo(s => s.execute(s"SELECT Count(*) FROM $keyspace.kv").all())
    rows.get(0).getLong(0) should be > 200L
  }

  it should "write no rows from an empty stream" in {
    val checkpointDir = Files.createTempDirectory("ks-empty-tc")

    val source = spark
      .readStream
      .format("org.apache.spark.sql.datastax.test.empty")
      .load()
      .withColumn("value", org.apache.spark.sql.functions.col("key") + 1)
      .withColumn("key", org.apache.spark.sql.functions.col("key"))

    val query = source.writeStream
      .option("checkpointLocation", checkpointDir.toString)
      .cassandraFormat("empty", keyspace)
      .outputMode(OutputMode.Append())
      .start()

    try {
      eventually(timeout(Span(30, Seconds))) {
        query.exception should be(None)
        query.lastProgress.batchId should be > 2L
      }
    } finally {
      query.stop()
    }

    val rows = connector.withSessionDo(s => s.execute(s"SELECT Count(*) FROM $keyspace.empty").all())
    rows.get(0).getLong(0) should be(0L)
  }
}
