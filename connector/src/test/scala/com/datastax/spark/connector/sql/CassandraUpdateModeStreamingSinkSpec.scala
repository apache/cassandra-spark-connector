package com.datastax.spark.connector.sql

import java.nio.file.Files

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.streaming.OutputMode
import org.scalatest.concurrent.Eventually
import org.scalatest.{FlatSpec, Ignore, Matchers}
import org.scalatest.time.{Seconds, Span}
import org.testcontainers.cassandra.CassandraContainer
import org.testcontainers.utility.DockerImageName

@Ignore
class CassandraUpdateModeStreamingSinkSpec extends FlatSpec with Matchers with Eventually {

  "Cassandra streaming sink" should "write streaming aggregation updates in update mode" in {
    val cassandra = new CassandraContainer(DockerImageName.parse("cassandra:5.0.2"))
    cassandra.start()

    var spark: SparkSession = null
    try {
      val keyspace = "update_mode_test"
      val table = "kv_updates"
      val host = cassandra.getHost
      val port = cassandra.getMappedPort(9042)

      val session = CqlSession.builder()
        .addContactPoint(new java.net.InetSocketAddress(host, port))
        .withLocalDatacenter("datacenter1")
        .build()
      try {
        session.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH replication = {'class':'SimpleStrategy','replication_factor':1}")
        session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$table (key int PRIMARY KEY, value bigint)")
      } finally {
        session.close()
      }

      val conf = new SparkConf()
        .setMaster("local[2]")
        .setAppName("cassandra-update-mode-streaming-sink-test")
        .set("spark.ui.enabled", "false")
        .set("spark.cassandra.connection.host", host)
        .set("spark.cassandra.connection.port", port.toString)
        .set("spark.cassandra.connection.localDC", "datacenter1")

      spark = SparkSession.builder().config(conf).getOrCreate()

      val checkpointDir = Files.createTempDirectory("cassandra-update-mode-checkpoint")
      val source = spark
        .readStream
        .format("rate")
        .option("rowsPerSecond", "10")
        .load()
        .selectExpr("CAST(value % 2 AS INT) AS key")
        .groupBy("key")
        .count()
        .selectExpr("key", "count AS value")

      val query = source.writeStream
        .option("checkpointLocation", checkpointDir.toString)
        .cassandraFormat(table, keyspace)
        .outputMode(OutputMode.Update())
        .start()

      try {
        eventually(timeout(Span(60, Seconds))) {
          query.exception should be(None)
          val rows = CassandraConnector(conf).withSessionDo { session =>
            session.execute(s"SELECT key, value FROM $keyspace.$table").all()
          }
          rows.size() should be > 0
          (0 until rows.size()).map(index => rows.get(index).getLong("value")).max should be > 0L
        }
      } finally {
        query.stop()
      }
    } finally {
      if (spark != null) {
        spark.stop()
      }
      cassandra.stop()
    }
  }
}
