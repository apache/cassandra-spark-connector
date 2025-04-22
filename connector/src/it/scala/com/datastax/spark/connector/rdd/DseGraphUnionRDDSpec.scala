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

package com.datastax.spark.connector.rdd

import java.lang.{Integer => JInteger, String => JString}

import scala.jdk.CollectionConverters._
import scala.concurrent.Future
import scala.language.existentials
import scala.util.Random
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.spark.connector._
import com.datastax.spark.connector.cluster.DefaultCluster
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.{DseTestUtil, SparkConf}

class DseGraphUnionRDDSpec extends SparkCassandraITFlatSpecBase with DefaultCluster {

  override lazy val conn = CassandraConnector(sparkConf)

  val rand = new Random(10)
  val labels = ('a' to 'e').map(_.toString)
  val tables = labels.map(label => s"${label}_p")

  override val ks = "graphunion"

  override def beforeClass {
    conn.withSessionDo { case session =>
      session.execute(
        s"""CREATE KEYSPACE IF NOT EXISTS $ks
           |WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }"""
          .stripMargin)
      awaitAll(
        tables.zip(labels).map { case (tableName, label) => Future(makeSimplePropTable(session, tableName, label)) }: _*
      )
    }
  }


  def makeSimplePropTable(session: CqlSession, name: String, label: String): Unit = {
    session.execute(
      s"""CREATE TABLE IF NOT EXISTS $ks.${name} ($label INT, label TEXT, foo TEXT, PRIMARY KEY
         |($label))""".stripMargin)
    for (i <- 1 to 100) {
      session.execute(s"INSERT INTO $ks.${name} ($label, label, foo) values ($i, '$label', 'foo$i')")
    }
  }

  "DseGraphUnionRDD" should " create a partitioner for a union of C* tables" in {
    val tableRDDs = tables.map(sc.cassandraTable(ks, _))
    val unionRDD = new DseGraphUnionedRDD(sc, tableRDDs, ks, labels)
    unionRDD.partitioner shouldBe defined
    val part = unionRDD.partitioner.get match {
      case x: DseGraphPartitioner[_, _] => x
      case _ => fail(s"${unionRDD.partitioner.get} is not a DseGraphPartitioner")
    }
  }

  it should "make a partitioner which correctly maps labels to UnionRDD partitions" in {
    val tableRDDs = tables.map(
      sc.cassandraTable(ks, _).withReadConf(ReadConf(splitCount = Some(rand.nextInt(8) + 1))))

    val tableToRDD = labels.zip(tableRDDs).toMap
    val unionRDD = new DseGraphUnionedRDD(sc, tableRDDs, ks, labels)
    unionRDD.partitioner shouldBe defined
    val part = unionRDD.partitioner.get match {
      case x: DseGraphPartitioner[_, _] => x
      case _ => fail(s"${unionRDD.partitioner.get} is not a DseGraphPartitioner")
    }
    val unionedPartitions = unionRDD.partitions
    for (label <- labels) {
      val targetRDD = tableToRDD(label)
      val offset = part.labelToRddOffset(label)
      val mappedPartitions = unionedPartitions
        .slice(offset, offset + targetRDD.partitions.length)
        .map(DseTestUtil.getParentPartition(_))
      mappedPartitions should contain theSameElementsInOrderAs targetRDD.partitions
    }
  }

  it should "correctly partition Map keys based on label with different Partiton Keys" in {
    val tableRDDs = tables.map(
      sc.cassandraTable(ks, _).withReadConf(ReadConf(splitCount = Some(rand.nextInt(8) + 1))))
    val tableToRDD = labels.zip(tableRDDs).toMap
    val unionRDD = new DseGraphUnionedRDD(sc, tableRDDs, ks, labels)
    unionRDD.partitioner shouldBe defined
    val part = unionRDD.partitioner.get match {
      case x: DseGraphPartitioner[_, _] => x
      case _ => fail(s"${unionRDD.partitioner.get} is not a DseGraphPartitioner")
    }

    /** Read the entire union RDD with each element zipped with the Partiiton Index it came from **/
    val keys = unionRDD
      .mapPartitionsWithIndex { case (int, it) => it.map((int, _)) }
      .collect
      .map { case (index, row) => {
        val label = row.get[JString]("label")
        Map[JString, AnyRef](
          part.LabelAccessor -> label,
          label -> row.get[JInteger](label),
          "foo" -> row.get[JString]("foo"),
          "partition" -> (index: JInteger)
        ).asJava
      }
      }

    for (key <- keys) {
      val determinedPartition = part.getPartition(key)
      determinedPartition should be(key.get("partition"))
    }
  }
}
