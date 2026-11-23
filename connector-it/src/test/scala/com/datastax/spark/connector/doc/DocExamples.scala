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

package com.datastax.spark.connector.doc

import java.util.Random

import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.types.CassandraOption
import com.datastax.spark.connector._
import com.datastax.spark.connector.writer.WriteConf
import com.datastax.spark.connector.cluster.DefaultCluster

import com.datastax.oss.driver.api.core.DefaultProtocolVersion._

import scala.concurrent.Future

case class RandomListSelector[T](list: Seq[T]) {
  val r = { new Random() }
  def next(): T = list(r.nextInt(list.length))
}

class DocExamples extends SparkCassandraITFlatSpecBase with DefaultCluster {

  val numrows: Long = 1000

  override lazy val conn = CassandraConnector(defaultConf)
  override val ks = "doc_example"

  conn.withSessionDo { session =>
    createKeyspace(session)

    awaitAll(
      Future {
        session.execute(
          s"""CREATE TABLE doc_example.tab1 (key INT, col_1 INT, col_2 INT,
             |PRIMARY KEY (key))""".stripMargin)
        session.execute(
          s"""INSERT INTO doc_example.tab1 (key, col_1, col_2) VALUES (1, null, 1)
             |
           """.stripMargin)
      },
      Future {
        session.execute(
          s"""CREATE TABLE doc_example.tab2 (key INT, col_1 INT, col_2 INT,
             |PRIMARY KEY (key))""".stripMargin)
        session.execute(
          s"""INSERT INTO doc_example.tab2 (key, col_1, col_2) VALUES (1, 5, null)
             |
           """.stripMargin)
      },
      Future {session.execute(
        s"""CREATE TABLE $ks.purchases (
           |userid int,
           |purchaseid int,
           |objectid text,
           |amount int,
           |PRIMARY KEY (userid, purchaseid, objectid))""".stripMargin)
      },
      Future {session.execute(
        s"""CREATE TABLE doc_example.users (
          |userid int PRIMARY KEY,
          |name text,
          |zipcode int)""".stripMargin)
       })
  }

  override def beforeClass {
    val numrows = 1000// We can't be linked to the scope of flatspec which isn't serializable
    val ks = "doc_example"
    val r = new Random(100)
    val zipcodes = (1 to 100).map(_ => r.nextInt(99999)).distinct
    val objects = Seq("pepper", "lemon", "pear", "squid", "beet", "iron", "grass", "axe", "grape")



    val randomObject = RandomListSelector(objects)
    val randomZipCode = RandomListSelector(zipcodes)

    sc.parallelize(1 to numrows).map(x =>
      (x, s"User $x", randomZipCode.next)
    ).saveToCassandra(ks, "users")

    sc.parallelize(1 to numrows).flatMap(x =>
      (1 to 10).map( p => (x, p, randomObject.next, p))
    ).saveToCassandra(ks, "purchases")
  }

  "Docs" should "demonstrate copying a table without deletes" in skipIfProtocolVersionLT(V4){
    sc.cassandraTable[(Int, CassandraOption[Int], CassandraOption[Int])](ks, "tab1")
      .saveToCassandra(ks, "tab2")
    sc.cassandraTable[(Int, Int, Int)](ks, "tab2").collect should contain((1, 5, 1))
  }

  it should "demonstrate only deleting some records" in skipIfProtocolVersionLT(V4){
    sc.parallelize(1 to 6).map(x => (x, x, x)).saveToCassandra(ks, "tab1")
    sc.parallelize(1 to 6).map {
      case x if (x >= 5) => (x, CassandraOption.Null, CassandraOption.Unset)
      case x if (x <= 2) => (x, CassandraOption.Unset, CassandraOption.Null)
      case x => (x, CassandraOption(-1), CassandraOption(-1))
    }.saveToCassandra(ks, "tab1")


    val results = sc.cassandraTable[(Int, Option[Int], Option[Int])](ks, "tab1").collect
    results should contain theSameElementsAs Seq(
      (1, Some(1), None),
      (2, Some(2), None),
      (3, Some(-1), Some(-1)),
      (4, Some(-1), Some(-1)),
      (5, None, Some(5)),
      (6, None, Some(6)))
  }

  it should "demonstrate converting Options to CassandraOptions" in skipIfProtocolVersionLT(V4){
    import com.datastax.spark.connector.types.CassandraOption
    //Setup original data (1, 1, 1) --> (6, 6, 6)
    sc.parallelize(1 to 6).map(x => (x, x, x)).saveToCassandra(ks, "tab1")

    //Setup options Rdd (1, -1, None) (2, -1, None) (3, -1, None)
    val optRdd = sc.parallelize(1 to 6)
      .map(x => (x, None, None))
      .map { case (x: Int, y: Option[Int], z: Option[Int]) =>
        (x, CassandraOption.deleteIfNone(y), CassandraOption.unsetIfNone(z))
      }.saveToCassandra(ks, "tab1")

    val results = sc.cassandraTable[(Int, Option[Int], Option[Int])](ks, "tab1").collect

    results should contain theSameElementsAs Seq(
      (1, None, Some(1)),
      (2, None, Some(2)),
      (3, None, Some(3)),
      (4, None, Some(4)),
      (5, None, Some(5)),
      (6, None, Some(6))
    )

  }

  it should "show using a write conf to ignore nulls" in skipIfProtocolVersionLT(V4){
    //Setup original data (1, 1, 1) --> (6, 6, 6)
    sc.parallelize(1 to 6).map(x => (x, x, x)).saveToCassandra(ks, "tab1")

    val ignoreNullsWriteConf = WriteConf.fromSparkConf(sc.getConf).copy(ignoreNulls = true)
    //These writes will do not delete because we are ignoring nulls
    val optRdd = sc.parallelize(1 to 6)
      .map(x => (x, None, None))
      .saveToCassandra(ks, "tab1", writeConf = ignoreNullsWriteConf)

    val results = sc.cassandraTable[(Int, Int, Int)](ks, "tab1").collect

    results should contain theSameElementsAs Seq(
      (1, 1, 1),
      (2, 2, 2),
      (3, 3, 3),
      (4, 4, 4),
      (5, 5, 5),
      (6, 6, 6)
    )

  }

  "Doc Page 16" should "Keying a Table with the Partition Key Produces an RDD ..." in {
    val ks = "doc_example"
    val rdd = {
      sc.cassandraTable[(String, Int)](ks, "users")
        .select("name" as "_1", "zipcode" as "_2", "userid")
        .keyBy[Tuple1[Int]]("userid")
    }

    rdd.partitioner shouldBe defined
  }

  it should "Reading a table into a (K,V) type or ..." in {
    val ks = "doc_example"
    val rdd1 = sc.cassandraTable[(Int, Int)](ks, "users")
    rdd1.partitioner shouldBe empty


    val rdd2 = {
      sc.cassandraTable[(Int, String)](ks, "users")
        .select("name" as "_2", "zipcode" as "_1", "userid")
        .keyBy[Tuple1[Int]]("zipcode")
    }
    rdd2.partitioner shouldBe empty
  }

  it should "Grouping/Reducing on out of order Clustering keys" in {
    val ks = "doc_example"
    val rdd = {
      sc.cassandraTable[(Int, Int)](ks, "purchases")
        .select("purchaseid" as "_1", "amount" as "_2", "userid", "objectid")
        .keyBy[(Int, String)]("userid", "objectid")
    }

    rdd.groupByKey.toDebugString should not include ("+-")
    rdd.groupByKey.count should be >= numrows
  }

  it should "doing the same thing without a partitioner requires a shuffle" in {
    val ks = "doc_example"

    //Empty map will remove the partitioner
    val rdd = {
      sc.cassandraTable[(Int, Int)](ks, "purchases")
        .select("purchaseid" as "_1", "amount" as "_2", "userid", "objectid")
        .keyBy[(Int, String)]("userid", "objectid")
    }.map(x => x)

    rdd.partitioner shouldBe empty

    rdd.groupByKey.toDebugString should include("+-")
    rdd.groupByKey.count should be >= numrows
  }

  it should "Joining to Cassandra RDDs from non Cassandra RDDs" in {
    import com.datastax.spark.connector._

    val ks = "doc_example"
    val rdd = {
      sc.cassandraTable[(String, Int)](ks, "users")
        .select("name" as "_1", "zipcode" as "_2", "userid")
        .keyBy[Tuple1[Int]]("userid")
    }

    val joinrdd = sc.parallelize(1 to 10000).map(x => (Tuple1(x), x)).join(rdd)
    joinrdd.toDebugString should include("+-")
    joinrdd.count should be(numrows)

    //Use an empty map to drop the partitioner
    val rddnopart = {
      sc.cassandraTable[(String, Int)](ks, "users")
        .select("name" as "_1", "zipcode" as "_2", "userid")
        .keyBy[Tuple1[Int]]("userid").map(x => x)
    }

    val joinnopart = sc.parallelize(1 to 10000).map(x => (Tuple1(x), x)).join(rddnopart)
    joinnopart.toDebugString should include("+-")

    joinnopart.count should be(numrows)
  }

  it should "Joining to Cassandra RDDs on Common Partition Keys" in {
    val ks = "doc_example"
    val rdd1 = {
      sc.cassandraTable[(Int, Int, String)](ks, "purchases")
        .select("purchaseid" as "_1", "amount" as "_2", "objectid" as "_3", "userid")
        .keyBy[Tuple1[Int]]("userid")
    }

    val rdd2 = {
      sc.cassandraTable[(String, Int)](ks, "users")
        .select("name" as "_1", "zipcode" as "_2", "userid")
        .keyBy[Tuple1[Int]]("userid")
    }.applyPartitionerFrom(rdd1) // Assigns the partitioner from the first rdd to this one

    val joinRDD = rdd1.join(rdd2)
    joinRDD.toDebugString should not include ("+-")

    joinRDD.count should be(numrows * 10)

    val rdd1nopart = {
      sc.cassandraTable[(Int, Int, String)](ks, "purchases")
        .select("purchaseid" as "_1", "amount" as "_2", "objectid" as "_3", "userid")
        .keyBy[Tuple1[Int]]("userid")
    }.map(x => x)

    val rdd2nopart = {
      sc.cassandraTable[(String, Int)](ks, "users")
        .select("name" as "_1", "zipcode" as "_2", "userid")
        .keyBy[Tuple1[Int]]("userid")
    }.map(x => x)

    val joinnopart = rdd1nopart.join(rdd2nopart)
    joinnopart.toDebugString should include("+-")

    joinnopart.count should be(numrows * 10)
  }

}
