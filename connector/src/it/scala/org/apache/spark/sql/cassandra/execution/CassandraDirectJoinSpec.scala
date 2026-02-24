/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.cassandra.execution

import java.sql.Timestamp
import java.time.Instant

import com.datastax.oss.driver.api.core.DefaultProtocolVersion._
import com.datastax.spark.connector.cluster.DefaultCluster
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.{SparkCassandraITFlatSpecBase, _}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.cassandra.CassandraSourceRelation._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.execution.streaming.StreamingQueryWrapper
import org.scalatest.concurrent.Eventually


import scala.concurrent.duration._
import scala.concurrent.Future


case class DirectJoinRow (k: Int, v: Int)
case class IdRow (id: String)
case class TimestampRow (ts: java.sql.Timestamp)

class CassandraDirectJoinSpec extends SparkCassandraITFlatSpecBase with DefaultCluster with Eventually {

  override lazy val conn = CassandraConnector(defaultConf)

  override def beforeClass {
    spark.conf.set(DirectJoinSettingParam.name, "auto")
    setupCassandraCatalog

    conn.withSessionDo { session =>
      val executor = getExecutor(session)
      createKeyspace(session)
      awaitAll(
        Future {
          session.execute(s"CREATE TABLE $ks.kv (k int PRIMARY KEY, v int)")
          val ps = session.prepare(s"INSERT INTO $ks.kv (k,v) VALUES (?,?)")
          awaitAll {
            for (id <- 1 to 100) yield {
              executor.executeAsync(ps.bind(id: java.lang.Integer, id: java.lang.Integer))
            }
          }
        },
        Future {
          session.execute(s"CREATE TABLE $ks.kv2 (k int PRIMARY KEY, v1 int, v2 int, v3 int, v4 int)")
          val ps = session.prepare(s"INSERT INTO $ks.kv2 (k, v1, v2, v3, v4) VALUES (?,?,?,?,?)")
          awaitAll {
            for (id <- 1 to 100) yield {
              executor.executeAsync(ps.bind(id: java.lang.Integer, id: java.lang.Integer, id: java.lang.Integer, id: java.lang.Integer, id: java.lang.Integer))
            }
          }
        },
        Future {
          session.execute(s"CREATE TABLE $ks.multikey (ka int, kb int, kc int, c int, v int, PRIMARY KEY ((ka, kb, kc), c))")
          val ps = session.prepare(s"INSERT INTO $ks.multikey (ka, kb, kc, c, v) VALUES (?,?,?,?,?)")
          awaitAll {
            for (id <- 1 to 100; c <- 1 to 5) yield {
              val jId: java.lang.Integer = id
              val jC: java.lang.Integer = c
              executor.executeAsync(ps.bind(jId, jId, jId, jC, jC))
            }
          }
        },
        Future {
          session.execute(s"CREATE TABLE $ks.abcd (a int, b int, c int, d int, PRIMARY KEY ((a,b),c))")
          val ps = session.prepare(s"INSERT INTO $ks.abcd (a, b, c, d) VALUES (?,?,?,?)")
          awaitAll {
            for (id <- 1 to 100; c <- 1 to 5) yield {
              val jId: java.lang.Integer = id
              val jC: java.lang.Integer = c
              executor.executeAsync(ps.bind(jId, jId + 1: java.lang.Integer, jC, jC))
            }
          }
        },
        Future {
          session.execute(s"CREATE TABLE $ks.tstest (t timestamp, v int, PRIMARY KEY ((t),v))")
          val ps = session.prepare(s"INSERT INTO $ks.tstest (t, v) VALUES (?,?)")
          awaitAll {
            for (id <- 1 to 100) yield {
              val jT: Instant = Instant.ofEpochMilli(id.toLong)
              val jV: java.lang.Integer = id.toInt
              executor.executeAsync(ps.bind(jT, jV))
            }
          }
        },
        Future {
          session.execute(s"CREATE TYPE $ks.address (street text, city text, residents set<frozen<tuple<text, text>>>) ")
          session.execute(s"CREATE TABLE $ks.location (id text, address frozen <address>, PRIMARY KEY (id))")
          session.execute(
            s"""INSERT INTO $ks.location (id, address) VALUES ('test',
               |{
               |  street: 'Laurel',
               |  city: 'New Orleans',
               |  residents:{('sundance', 'dog'), ('cara', 'dog')}
               |})""".stripMargin)
          session.execute(s"CREATE TYPE $ks.user (address frozen <address>) ")
          session.execute(s"CREATE TABLE $ks.members (id text, user frozen <user>, PRIMARY KEY (id))")
          session.execute(
            s"""INSERT INTO $ks.members (id, user) VALUES ('test1',
               |{
               |  address: {
               |    street: 'Laurel',
               |    city: 'New Orleans',
               |    residents:{('sundance', 'dog'), ('cara', 'dog')}
               |  }
               |})""".stripMargin)
        },
        Future {
          info("Making table with all PV4 Datatypes")
          skipIfProtocolVersionLT(V4) {
            session.execute(
              s"""
                 | CREATE TABLE $ks.test_data_type (
                 | a ASCII,
                 | b INT,
                 | c FLOAT,
                 | d DOUBLE,
                 | e BIGINT,
                 | f BOOLEAN,
                 | g DECIMAL,
                 | h INET,
                 | i TEXT,
                 | j TIMESTAMP,
                 | k UUID,
                 | l VARINT,
                 | m SMALLINT,
                 | n TINYINT,
                 | PRIMARY KEY ((a), b, c)
                 |)""".stripMargin)
            session.execute(
              s"""
                 | INSERT INTO $ks.test_data_type (a, b, c, d, e, f, g, h, i, j, k, l, m, n)
                 | VALUES (
                 | 'ascii',
                 | 10,
                 | 12.34,
                 | 12.3456789,
                 | 123344556,
                 | true,
                 | 12.36,
                 | '74.125.239.135',
                 | 'text',
                 | '2011-02-03 04:05+0000',
                 | 123e4567-e89b-12d3-a456-426655440000,
                 | 123456,
                 | 22,
                 | 4
                 |)""".
                stripMargin)
          }
        },
        Future {
          session.execute(
            s"""CREATE TABLE IF NOT EXISTS $ks.module_by_coverage (
          date text,
          hour text,
          xmlid text,
          moduleid text,
          coverageid text,
          description text,
          PRIMARY KEY ((date, hour), xmlid, moduleid)) WITH CLUSTERING ORDER BY (xmlid ASC, moduleid ASC);""")
          session.execute(s"INSERT INTO $ks.module_by_coverage " +
            s"(date,hour,  xmlid,moduleid,coverageid, description) VALUES " +
            s"('2018-07-03','0301','Q_1','mod1', 'cov1', 'desc1')")
        }
      )
      //Just in case
      executor.waitForCurrentlyExecutingTasks()
    }
  }



  import spark.implicits._

  "Cassandra Direct Joins Strategy" should "be extracted from logical plans" in {

    val left = spark.createDataset(Seq(DirectJoinRow(1,1)))
    val right = spark.read.cassandraFormat("kv", ks).load()

    val join = left.join(right, left("k") === right("k"))

    withClue(planDetails(join)) {
      getDirectJoin(join) shouldBe defined
    }

  }

  it should "be extracted from a plan with a rename" in {
    val left = spark.createDataset(Seq(DirectJoinRow(1,1)))
    val right = spark.read.cassandraFormat("kv", ks).load().withColumnRenamed("k", "x")

    val join = left.join(right, left("k") === right("x"))

    withClue(planDetails(join)) {
      getDirectJoin(join) shouldBe defined
    }
  }

  it should "be extracted from a plan with many renames" in {
    val left = spark.createDataset(Seq(DirectJoinRow(1,1)))
    val right = spark.read.cassandraFormat("kv", ks).load()
      .withColumnRenamed("k", "x")
      .withColumnRenamed("x","b")
      .filter('b < 5)
      .withColumnRenamed("b","a")

    val join = left.join(right, left("k") === right("a"))

    withClue(planDetails(join)) {
      getDirectJoin(join) shouldBe defined
    }
  }

  it should "be extracted from a plan with many renames and multiple partition keys" in {
    val left = spark.createDataset(Seq(DirectJoinRow(1,1)))
    val right = spark.read.cassandraFormat("multikey", ks).load()
      .withColumnRenamed("ka", "a")
      .withColumnRenamed("kb","b")
      .filter('b < 5)
      .withColumnRenamed("kc","d")

    val join = left.join(right,
      left("k") === right("a")
        && left("k") === right("b")
        && left("k") === right("d")
    )

    withClue(planDetails(join)) {
      getDirectJoin(join) shouldBe defined
    }
  }

  it should "be extracted from a left outer Join" in {
    val left = spark.createDataset(Seq(DirectJoinRow(1,1)))
    val right = spark.read.cassandraFormat("kv", ks).load()

    val join = left.join(right, left("k") === right("k"), "leftouter")

    withClue(planDetails(join)) {
      getDirectJoin(join) shouldBe defined
    }
  }


  it should "not be extracted from a plan with a aggregation" in {
    val left = spark.createDataset(Seq(DirectJoinRow(1,1)))
    val right = spark.read.cassandraFormat("kv", ks)
      .load()
      .withColumnRenamed("k", "x")
      .groupBy('x)
      .max("v")

    val join = left.join(right, left("k") === right("x"))

    withClue(planDetails(join)) {
      getDirectJoin(join) shouldBe empty
    }
  }

  it should "not be extracted from a plan where not all partition keys are joined" in {
    val left = spark.createDataset(Seq(DirectJoinRow(1,1)))
    val right = spark.read.cassandraFormat("multikey", ks).load()

    val join = left.join(right, left("k") === right("ka"))

    withClue(planDetails(join)) {
      getDirectJoin(join) shouldBe empty
    }
  }


  it should "not mess up non-Cassandra Joins" in {
    val left = spark.createDataset(Seq(DirectJoinRow(1,1)))
    val right = spark.createDataset(Seq(DirectJoinRow(1,1)))

    val join = left.join(right, left("k") === right("k"))

    withClue(planDetails(join)) {
      getDirectJoin(join) shouldBe empty
    }
  }

  it should "not do a join if the ratio fails" in withConfig(DirectJoinSizeRatioParam.name, "-1"){
    val left = spark.createDataset(Seq(DirectJoinRow(1,1)))
    val right = spark.read.cassandraFormat("kv", ks).load()

    val join = left.join(right, left("k") === right("k"))

    withClue(planDetails(join)) {
      getDirectJoin(join) shouldBe empty
    }
  }

  it should "do a join if the ratio fails but is hinted" in withConfig(DirectJoinSizeRatioParam.name, "-1"){
    val left = spark.createDataset(Seq(DirectJoinRow(1,1)))
    val right = spark.read.cassandraFormat("kv", ks).load().directJoin()

    val join = left.join(right, left("k") === right("k"))

    withClue(planDetails(join)) {
      getDirectJoin(join) shouldBe defined
    }
  }

  "Structured Streaming" should "allow a direct join" in {
    val rateStream = spark
      .readStream
      .format("rate")
      .option("rowsPerSecond", "1000")
      .load()
      .withColumn("value", 'value.cast("Int"))

    val cassandraTable = spark
      .read
      .cassandraFormat("kv", ks)
      .load
      .directJoin(AlwaysOn)

    val join = rateStream
      .join(cassandraTable, rateStream("value") === cassandraTable("k"))

    val stream = join
      .writeStream
      .format("console")
      .start()
      .asInstanceOf[StreamingQueryWrapper]


    //Need to wait for a real batch to occur
    try {
      eventually(timeout(scaled(10.seconds))) {
        getDirectJoin(stream) shouldBe defined
      }
    } finally {
      stream.stop
    }
  }

  "Cassandra Data Source Execution" should " do a simple left join" in {
    val left = spark.createDataset(Seq(DirectJoinRow(1,1)))
    val right = spark.read.cassandraFormat("kv", ks).load()

    val join = left.join(right, left("k") === right("k"))

    withClue(planDetails(join))
    {
      getDirectJoin(join) shouldBe defined
      join.collect() should be (Array(Row(1,1,1,1)))
    }
  }

  it should " do a join with a cassandra branch filter" in {
    val left = spark.createDataset((1 to 10).map(x => DirectJoinRow(x,x)))
    val right = spark.read.cassandraFormat("kv", ks).load().filter('v < 5)

    val join = left.join(right, left("k") === right("k"))

    val expected = (1 to 4).map(x => Row(x,x,x,x))

    withClue(planDetails(join))
    {
      getDirectJoin(join) shouldBe defined
      join.collect() should be (expected)
    }
  }

  it should "evaluate non-cassandra join clauses and conditions" in {
    val left = spark.createDataset((1 to 10).map(x => DirectJoinRow(x, x)))
    val right = spark.read.cassandraFormat("abcd", ks).load()

    val join = left.join(right,
      left("k") === right("a")
        && (left("k") + 1) === right("b")
        && left("k") === right("c"))

    val expected = for (i <- 1 to 5) yield {
      Row(i, i, i, i + 1, i, i)
    }

    withClue(planDetails(join)) {
      getDirectJoin(join) shouldBe defined
      join.collect() shouldBe (expected)
    }
  }

  it should "do a join with cassandra pushdowns" in {
    val left = spark.createDataset((1 to 10).map(x => DirectJoinRow(x,x)))
    val right = spark.read.cassandraFormat("multikey", ks).load().filter('c  < 3)

    val join = left.join(right,
      left("k") === right("ka")
        && left("k") === right("kb")
        && left("k") === right("kc"))

    val expected = for (i <- 1 to 10; c <- 1 to 2) yield {
      Row(i,i,i,i,i,c,c)
    }

    withClue(planDetails(join)) {
      getDirectJoin(join) shouldBe defined
      join.collect() shouldBe (expected)
    }
  }

  it should "join with a table with all types" in skipIfProtocolVersionLT(V4){
    val left = spark.createDataset(Seq("ascii"))
    val right = spark.read.cassandraFormat("test_data_type", ks).load()
    val join = left.join(right, left("value") === right("a"))
    val expected =
      Row(
        "ascii",
        "ascii",
        10,
        12.34f,
        12.3456789d,
        123344556,
        true,
        BigDecimal("12.360000000000000000"),
        "74.125.239.135",
        "text",
        new Timestamp(1296705900000L),
        "123e4567-e89b-12d3-a456-426655440000",
        BigDecimal(123456),
        22,
        4)

    withClue(planDetails(join)) {
      getDirectJoin(join) shouldBe defined
    }
    val elements = join.collect()(0)
    for ( (x,y) <- elements.toSeq.zip(expected.toSeq)){
      withClue(s"$x: ${x.getClass} != $y: ${y.getClass}") {
        x match {
          case bdx: java.math.BigDecimal =>
            val bdy = y.asInstanceOf[scala.math.BigDecimal]
            bdx.equals(bdy.bigDecimal) should be (true)

          case x => x should equal (y)
        }
      }
    }
  }

  it should "join a plan with many renames and multiple partition keys" in {
    val left = spark.createDataset(Seq(DirectJoinRow(1,1)))
    val right = spark.read.cassandraFormat("multikey", ks).load()
      .withColumnRenamed("ka", "a")
      .withColumnRenamed("kb","b")
      .filter('b < 5)
      .withColumnRenamed("kc","d")

    val join = left.join(right,
      left("k") === right("a")
        && left("k") === right("b")
        && left("k") === right("d")
    )

    val expected = for (i <- 1 to 1; c <- 1 to 5) yield {
      Row(i,i,i,i,i,c,c)
    }

    withClue(planDetails(join)) {
      getDirectJoin(join) shouldBe defined
    }

    val plan = getDirectJoin(join).get
    withClue(s"$plan \n ${plan.output}") {
      join.collect should be (expected)
    }
  }

  it should "handle left outer joins" in {
    val left = spark.createDataset(Seq(DirectJoinRow(-1,-1),DirectJoinRow(1,1), DirectJoinRow(-5,-5)))
    val right = spark.read.cassandraFormat("kv", ks).load()

    val join = left.join(right, left("k") === right("k"), "leftouter")

    val expected = Seq (
      Row(-1,-1, null, null),
      Row(1,1,1,1),
      Row(-5, -5, null, null)
    )

    withClue(planDetails(join)) {
      getDirectJoin(join) shouldBe defined
    }

    val plan = getDirectJoin(join).get
    withClue(s"$plan \n ${plan.output}") {
      join.collect should be (expected)
    }
  }

  it should "handle right outer joins" in {
    val right = spark.createDataset(Seq(DirectJoinRow(-1,-1),DirectJoinRow(1,1), DirectJoinRow(-5,-5)))
    val left = spark.read.cassandraFormat("kv", ks).load()

    val join = left.join(right, left("k") === right("k"), "right")

    val expected = Seq (
      Row(null, null, -1, -1),
      Row(1,1,1,1),
      Row(null, null, -5, -5)
    )

    withClue(planDetails(join)) {
      getDirectJoin(join) shouldBe defined
    }

    val plan = getDirectJoin(join).get
    withClue(s"$plan \n ${plan.output}") {
      join.collect should be (expected)
    }
  }


  it should "handle inner join with cassandra on the left side" in {
    val right = spark.createDataset(Seq(DirectJoinRow(-1,-1),DirectJoinRow(1,1), DirectJoinRow(-5,-5)))
    val left = spark.read.cassandraFormat("kv", ks).load()

    val join = left.join(right, left("k") === right("k"))

    val expected = Seq (
      Row(1,1,1,1)
    )

    withClue(planDetails(join)) {
      getDirectJoin(join) shouldBe defined
    }

    val plan = getDirectJoin(join).get
    withClue(s"$plan \n ${plan.output}") {
      join.collect should be (expected)
    }
  }

  /*
  The Join Gauntlet
   */
  "In the Join Tests" should " work with a nested join" in compareDirectOnDirectOff{ spark =>

    val right = spark.createDataset(Seq(DirectJoinRow(-1,-1),DirectJoinRow(1,1), DirectJoinRow(-5,-5)))
    val cassandra = spark.read.cassandraFormat("kv", ks).load

    val firstJoin = cassandra.join(right, cassandra("k") === right("k"))

    val left = spark.read.cassandraFormat("kv", ks).load

    left.join(right, left("k") === right("k"))
  }

  it should " work with in a complicated join tree" in compareDirectOnDirectOff{ spark =>

    val right =
      spark
        .createDataset(Seq(DirectJoinRow(-1,-1),DirectJoinRow(1,1), DirectJoinRow(-5,-5)))
        .select('v as "pops", 'k as "k")

    val cassandra = spark.read.cassandraFormat("kv", ks).load

    val firstJoin = cassandra.join(right, cassandra("k") === right("k"))

    firstJoin
      .filter(cassandra("v").isNotNull)
      .groupBy(right("pops")).avg("v")
  }

  it should " work with a join tree with literals and other expressions" in compareDirectOnDirectOff{ spark =>

    val right =
      spark
        .createDataset(Seq(DirectJoinRow(-1,-1),DirectJoinRow(1,1), DirectJoinRow(-5,-5)))
        .select('v as "pops", 'k as "k", lit(5) as "five")

    val cassandra = spark.read.cassandraFormat("kv", ks).load
      .withColumn("3k", 'k * 3)

    val firstJoin = cassandra.join(right, cassandra("k") === right("k"))

    firstJoin
      .filter(cassandra("v").isNotNull)
      .groupBy(right("pops")).avg("v")
  }

  it should " work with a pushdown in a tree" in compareDirectOnDirectOff{ spark =>
    val left = spark.createDataset((1 to 100).map(x => DirectJoinRow(x,x)))
    val right = spark.read.cassandraFormat("multikey", ks).load()
      .filter('c < 5)

    left.join(right,
      left("k") + 1 === right("ka")
        && left("k") + 1 === right("kb")
        && left("k") + 1  === right("kc"),
      "left_outer")
      .filter('c % 2 === 1)
      .groupBy('ka, 'kb)
      .max("c")
  }


  it should " work in a recursive join" in compareDirectOnDirectOff{ spark =>
    val left = spark.read.cassandraFormat("multikey", ks).load().directJoin(AlwaysOff)
    val right = spark.read.cassandraFormat("multikey", ks).load()

    left.join(right,
      left("ka") === right("ka")
        && left("kb") === right("kb")
        && left("kc") === right("kc"))
  }

  it should " work when we use join keys after the join" in compareDirectOnDirectOff{ spark =>
    val left = spark.read.cassandraFormat("multikey", ks).load().directJoin(AlwaysOff)
    val right = spark.read.cassandraFormat("multikey", ks).load()

    left.join(right,
      left("ka") === right("ka")
        && left("kb") === right("kb")
        && left("kc") === right("kc"))
      .withColumn("funnewcol", left("ka") + left("kb") + left("kc"))
  }

  it should "work with UDT's and Tuples in the Table definition" in compareDirectOnDirectOff{ spark =>
    val left = spark.createDataset(Seq(IdRow("test")))
    val right = spark.read.cassandraFormat("location", ks).load()
    left.join(right, left("id") === right("id"))
  }

  it should "work with field extractor after join" in compareDirectOnDirectOff{ spark =>
    val left = spark.createDataset(Seq(IdRow("test")))
    val right = spark.read.cassandraFormat("location", ks).load()
    left.join(right, left("id") === right("id"))
      .select($"address.*")
      .select($"street", $"city")
  }

  it should "work with deeply nested field extractor after join" in compareDirectOnDirectOff{ spark =>
    val left = spark.createDataset(Seq(IdRow("test1")))
    val right = spark.read.cassandraFormat("members", ks).load()
    left.join(right, left("id") === right("id"))
      .select($"user.address.*")
      .select($"street", $"city")
  }

  it should "work with complex field extractor after join" in compareDirectOnDirectOff { spark =>
    val left = spark.createDataset(Seq(IdRow("test")))
    val right = spark.read.cassandraFormat("location", ks).load()
      .select($"id", struct("address.street", "address.city") as "struct")
    left.join(right, left("id") === right("id"))
      .select($"struct.*")
      .select($"street", $"city")
  }

  it should "work on a timestamp PK join" in compareDirectOnDirectOff { spark =>
    val left = spark.createDataset(
      (1 to 100).map(value => TimestampRow(new Timestamp(value.toLong)))
    )
    val right = spark.read.cassandraFormat("tstest", ks).load()
    left.join(right, left("ts") === right("t"))
  }

  it should "handle a joins whose projected output columns differ after remodeling" in {
    val moduleByCoverage = spark.read.cassandraFormat(keyspace = ks, table = "module_by_coverage").load

    def getKeys() : List[(String,String)] = {
      for {
        d <- List("2018-07-03")
        h <- (0 to 23)
        m <- (0 to 3)
      } yield (d, f"${h}%02d${m}%02d")
    }

    val joinKeyDF = getKeys.toDF("date","hour")

    val joined = joinKeyDF.join(moduleByCoverage, Seq("date","hour"))

    val quotes = joined.where($"xmlid" rlike "^.*?Q_.*$")

    quotes.count should be (1)
  }

  it should "handle joins between two datasource v2 tables in sparksql" in compareDirectOnDirectOff { spark =>
    spark.sql(s"SELECT l.k, l.v, r.v1, r.v2 from $ks.kv as l LEFT JOIN $ks.kv2 as r on l.k == r.k")
  }

  /** SPARKC-613 */
  it should "use direct join for sql join" in compareDirectOnDirectOff { spark =>
    import spark.implicits._

    val toJoin = spark.range(1, 5).map(_.intValue).withColumnRenamed("value", "id")

    toJoin.createOrReplaceTempView("tojoin")
    spark.read.cassandraFormat("kv", ks).load().createOrReplaceTempView(s"cassdata")

    spark.sql("select * from tojoin tj inner join cassdata cd on tj.id = cd.k")
  }

  /** SPARKC-667 */
  it should "work with keyspace name starting with a digit" in {
    val keyspace = s"0123_$ks"

    conn.withSessionDo { session =>
      val executor = getExecutor(session)
      awaitAll(
        Future {
          createKeyspace(session, s""""$keyspace"""")
          session.execute(s"""CREATE TABLE "$keyspace".kv (k int PRIMARY KEY, v int)""")
          val ps = session.prepare(s"""INSERT INTO "$keyspace".kv (k,v) VALUES (?,?)""")
          awaitAll {
            for (id <- 1 to 100) yield {
              executor.executeAsync(ps.bind(id: java.lang.Integer, id: java.lang.Integer))
            }
          }
        }
      )
    }

    try {
      val left = spark.createDataset(Seq(DirectJoinRow(1, 1)))
      val right = spark.read.cassandraFormat(keyspace = keyspace, table = "kv").load()
      left.join(right, left("k") === right("k")).collect()
    }
    finally {
      conn.withSessionDo { session =>
        awaitAll(
          Future {
            session.execute(s"""DROP KEYSPACE "$keyspace"""")
          }
        )
      }
    }
  }

  private def compareDirectOnDirectOff(test: ((SparkSession) => DataFrame)) = {
    val classicSpark = spark.asInstanceOf[org.apache.spark.sql.classic.SparkSession]
    val sparkJoinOn = classicSpark.cloneSession()
    sparkJoinOn.conf.set(DirectJoinSettingParam.name, "on")
    val sparkJoinOff = classicSpark.cloneSession()
    sparkJoinOff.conf.set(DirectJoinSettingParam.name, "off")

    withClue(s"ON\n${planDetails(test(sparkJoinOn))} \nvs\n Off\n${planDetails(test(sparkJoinOff))}") {
      SparkSession.setActiveSession(sparkJoinOn)
      getDirectJoin(test(sparkJoinOn)) shouldBe defined
      val results = test(sparkJoinOn).collect

      SparkSession.setActiveSession(sparkJoinOff)
      getDirectJoin(test(sparkJoinOff)) shouldBe empty
      val expected = test(sparkJoinOff).collect
      expected should not be empty
      results should contain theSameElementsAs expected
    }
  }

  def getDirectJoin(df: Dataset[_]): Option[CassandraDirectJoinExec] = {
    val plan = df.queryExecution.sparkPlan
    plan.collectFirst{ case x: CassandraDirectJoinExec => x }
  }

  def getDirectJoin(stream: StreamingQueryWrapper) = {
    val plan = stream.streamingQuery.lastExecution.executedPlan
    plan.collectFirst { case x: CassandraDirectJoinExec => x }
  }

  def planDetails(df: Dataset[_]): String = {
    s"""Optimized Plan: ${df.queryExecution.optimizedPlan}
       |Spark Plan: ${df.queryExecution.sparkPlan}
     """.stripMargin
  }

}
