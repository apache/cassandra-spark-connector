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

package com.datastax.spark.connector.writer

import com.datastax.oss.driver.api.core.cql.{BatchStatement, BoundStatement, DefaultBatchType}
import com.datastax.oss.driver.api.core.{CqlSession, DefaultConsistencyLevel}
import com.datastax.spark.connector.cluster.DefaultCluster
import com.datastax.spark.connector.cql.{CassandraConnector, Schema}
import com.datastax.spark.connector.util.schemaFromCassandra
import com.datastax.spark.connector.{BatchSize, BytesInBatch, RowsInBatch, SparkCassandraITFlatSpecBase}

import scala.jdk.CollectionConverters._
import scala.util.Random

class GroupingBatchBuilderSpec extends SparkCassandraITFlatSpecBase with DefaultCluster {
  override lazy val conn = CassandraConnector(defaultConf)

  conn.withSessionDo { session =>
    createKeyspace(session)
    session.execute(s"""CREATE TABLE $ks.tab (id INT PRIMARY KEY, value TEXT)""")
  }

  val schema = schemaFromCassandra(conn, Some(ks), Some("tab"))
  val rowWriter = RowWriterFactory.defaultRowWriterFactory[(Int, String)].rowWriter(schema.tables.head, IndexedSeq("id", "value"))

  def makeBatchBuilder(session: CqlSession): (RichBoundStatementWrapper => Any, BatchSize, Int, Iterator[(Int, String)]) => GroupingBatchBuilder[(Int, String)] = {
    val protocolVersion = session.getContext.getProtocolVersion
    val stmt = session.prepare( s"""INSERT INTO $ks.tab (id, value) VALUES (:id, :value)""")
    val boundStmtBuilder = new BoundStatementBuilder(
      rowWriter,
      stmt,
      protocolVersion = protocolVersion)
    val batchStmtBuilder = new BatchStatementBuilder(DefaultBatchType.UNLOGGED, DefaultConsistencyLevel.LOCAL_ONE)
    new GroupingBatchBuilder[(Int, String)](
      boundStmtBuilder,
      batchStmtBuilder,
      _: RichBoundStatementWrapper => Any,
      _: BatchSize,
      _: Int,
      _: Iterator[(Int, String)])
  }

  def staticBatchKeyGen(bs: RichBoundStatementWrapper): Int = 0

  def dynamicBatchKeyGen(bs: RichBoundStatementWrapper): Int = bs.stmt.getInt(0) % 2

  def dynamicBatchKeyGen5(bs: RichBoundStatementWrapper): Int = bs.stmt.getInt(0) % 5

  "GroupingBatchBuilder in fixed batch key mode" should "make bound statements when batch size is specified as RowsInBatch(1)" in {
    conn.withSessionDo { session =>
      val bm = makeBatchBuilder(session)
      val data = Seq((1, "one"), (2, "two"), (3, "three"))
      val statements = bm(staticBatchKeyGen, RowsInBatch(1), 1, data.toIterator).toList
      statements.foreach(_ shouldBe a[RichBoundStatementWrapper])
      statements should have size 3
      statements.map(s => s.asInstanceOf[RichBoundStatementWrapper].stmt).map(s => (s.getInt(0), s.getString(1))) should contain theSameElementsAs data
    }
  }

  it should "make bound statements when batch size is specified as BytesInBatch(0)" in {
    conn.withSessionDo { session =>
      val bm = makeBatchBuilder(session)
      val data = Seq((1, "one"), (2, "two"), (3, "three"))
      val statements = bm(staticBatchKeyGen, BytesInBatch(0), 1, data.toIterator).toList
      statements.foreach(_ shouldBe a[RichBoundStatementWrapper])
      statements should have size 3
      statements.map(s => s.asInstanceOf[RichBoundStatementWrapper].stmt).map(s => (s.getInt(0), s.getString(1))) should contain theSameElementsAs data
    }
  }

  it should "make a batch and a bound statements according to the number of statements in a group" in {
    conn.withSessionDo { session =>
      val bm = makeBatchBuilder(session)
      val data = Seq((1, "one"), (2, "two"), (3, "three"))
      val statements = bm(staticBatchKeyGen, RowsInBatch(2), 1, data.toIterator).toList
      statements should have size 2
      statements(0) shouldBe a[RichBatchStatementWrapper]
      statements(1) shouldBe a[RichBoundStatementWrapper]
      statements.flatMap {
        case s: RichBoundStatementWrapper => List(s.stmt)
        case s: RichBatchStatementWrapper => s.stmt.asScala.collect{ case b: BoundStatement => b }
      }.map(s => (s.getInt(0), s.getString(1))) should contain theSameElementsAs data
    }
  }

  it should "make equal batches when batch size is specified in rows" in {
    conn.withSessionDo { session =>
      val bm = makeBatchBuilder(session)
      val data = Seq((1, "one"), (2, "two"), (3, "three"), (4, "four"))
      val statements = bm(staticBatchKeyGen, RowsInBatch(2), 1, data.toIterator).toList
      statements should have size 2
      statements foreach {
        _ shouldBe a[RichBatchStatementWrapper]
      }
      statements.map(_.stmt).flatMap {
        case s: BatchStatement =>
          s.size() should be(2)
          s.asScala.collect{ case b: BoundStatement => b }
      }.map(s => (s.getInt(0), s.getString(1))) should contain theSameElementsAs data
    }
  }

  it should "make batches of size not greater than the size specified in bytes" in {
    conn.withSessionDo { session =>
      val bm = makeBatchBuilder(session)
      val data = Seq(
        (1, "a"), // 5 bytes
        (2, "aa"), // 6 bytes
        (3, "aaa"), // 7 bytes
        (4, "aaaa"), // 8 bytes
        (5, "aaaaa"), // 9 bytes
        (6, "aaaaaa"), // 10 bytes
        (7, "aaaaaaaaaaaa") // 16 bytes
      )
      val statements = bm(staticBatchKeyGen, BytesInBatch(15), 1, data.toIterator).toList
      statements should have size 5
      statements.take(2) foreach {
        _ shouldBe a[RichBatchStatementWrapper]
      }
      statements.drop(2).take(3) foreach {
        _ shouldBe a[RichBoundStatementWrapper]
      }

      val stmtss = statements.map(_.stmt).map {
        case s: BoundStatement => List(s)
        case s: BatchStatement => s.asScala.collect{ case b: BoundStatement => b }
      }

      stmtss.foreach(stmts => stmts.size should be > 0)
      stmtss.foreach(stmts => if (stmts.size > 1) stmts.map(BoundStatementBuilder.calculateDataSize).sum should be <= 15)
      stmtss.flatten.map(s => (s.getInt(0), s.getString(1))) should contain theSameElementsInOrderAs data
    }
  }

  it should "produce empty stream when no data is available and batch size is specified in rows" in {
    conn.withSessionDo { session =>
      val bm = makeBatchBuilder(session)
      val data = Seq()
      val statements = bm(staticBatchKeyGen, RowsInBatch(10), 1, data.toIterator).toList
      statements should have size 0
    }
  }

  it should "produce empty stream when no data is available and batch size is specified in bytes" in {
    conn.withSessionDo { session =>
      val bm = makeBatchBuilder(session)
      val data = Seq()
      val statements = bm(staticBatchKeyGen, BytesInBatch(10), 1, data.toIterator).toList
      statements should have size 0
    }
  }

  "GroupingBatchBuilder in dynamic batch key mode" should "make bound statements when batch size is specified as RowsInBatch(1)" in {
    conn.withSessionDo { session =>
      val bm = makeBatchBuilder(session)
      val data = Seq((1, "one"), (2, "two"), (3, "three"))
      val statements = bm(dynamicBatchKeyGen, RowsInBatch(1), 1, data.toIterator).toList
      statements.foreach(_ shouldBe a[RichBoundStatementWrapper])
      statements should have size 3
      statements.map(s => s.asInstanceOf[RichBoundStatementWrapper].stmt).map(s => (s.getInt(0), s.getString(1))) should contain theSameElementsAs data
    }
  }

  it should "make bound statements when batch size is specified as BytesInBatch(0)" in {
    conn.withSessionDo { session =>
      val bm = makeBatchBuilder(session)
      val data = Seq((1, "one"), (2, "two"), (3, "three"))
      val statements = bm(dynamicBatchKeyGen, BytesInBatch(0), 1, data.toIterator).toList
      statements.foreach(_ shouldBe a[RichBoundStatementWrapper])
      statements should have size 3
      statements.map(s => s.asInstanceOf[RichBoundStatementWrapper].stmt).map(s => (s.getInt(0), s.getString(1))) should contain theSameElementsAs data
    }
  }

  it should "make a batch and a bound statements according to the number of statements in a group and a batch key" in {
    conn.withSessionDo { session =>
      val bm = makeBatchBuilder(session)
      val data = Seq((1, "one"), (2, "two"), (3, "three"))
      val statements = bm(dynamicBatchKeyGen, RowsInBatch(2), 2, data.toIterator).toList
      statements should have size 2
      statements(0) shouldBe a[RichBatchStatementWrapper]
      statements(1) shouldBe a[RichBoundStatementWrapper]
      statements.flatMap {
        case s: RichBoundStatementWrapper => List(s.stmt)
        case s: RichBatchStatementWrapper => s.stmt.asScala.collect{ case b: BoundStatement => b }
      }.map(s => (s.getInt(0), s.getString(1))) should contain theSameElementsAs data
    }
  }

  it should "make bound statements if batches cannot be made due to imposed limits" in {
    conn.withSessionDo { session =>
      val bm = makeBatchBuilder(session)
      val data = Seq((1, "one"), (2, "two"), (3, "three"))
      val statements = bm(dynamicBatchKeyGen, RowsInBatch(2), 1, data.toIterator).toList
      statements should have size 3
      statements.foreach(_ shouldBe a[RichBoundStatementWrapper])
      statements.map(s => s.asInstanceOf[RichBoundStatementWrapper].stmt).map(s => (s.getInt(0), s.getString(1))) should contain theSameElementsAs data
    }
  }

  it should "make equal batches when batch size is specified in rows and batch buffer is enough" in {
    conn.withSessionDo { session =>
      val bm = makeBatchBuilder(session)
      val data = Seq((1, "one"), (2, "two"), (3, "three"), (4, "four"))
      val statements = bm(dynamicBatchKeyGen, RowsInBatch(2), 2, data.toIterator).toList
      statements should have size 2
      statements foreach {
        _ shouldBe a[RichBatchStatementWrapper]
      }
      statements.flatMap {
        case s: RichBatchStatementWrapper =>
          s.stmt.size() should be(2)
          s.stmt.asScala.collect{ case b: BoundStatement => b }
      }.map(s => (s.getInt(0), s.getString(1))) should contain theSameElementsAs data
    }
  }

  it should "make batches of size not greater than the size specified in bytes" in {
    conn.withSessionDo { session =>
      val bm = makeBatchBuilder(session)
      val data = Seq(
        (1, "a"), // 5 bytes
        (2, "aa"), // 6 bytes
        (3, "aaa"), // 7 bytes
        (4, "aaaa"), // 8 bytes
        (5, "aaaaa"), // 9 bytes
        (6, "aaaaaa"), // 10 bytes
        (7, "aaaaaaaaaaaa") // 16 bytes
      )
      // we expect this:
      // 1: (1, 3), (5), (7)
      // 0: (2, 4), (6)

      val statements = bm(dynamicBatchKeyGen, BytesInBatch(15), 2, data.toIterator).toList
      statements should have size 5

      val boundStatements = statements collect {
        case s: RichBoundStatementWrapper => s.stmt.getInt(0)
      }
      boundStatements should have size 3
      boundStatements should contain theSameElementsAs Seq(5, 6, 7)

      val batchStatements = statements collect {
        case s: RichBatchStatementWrapper if s.stmt.size() == 2 => s.stmt.asScala.collect{ case b: BoundStatement => b.getInt(0) }
      }
      batchStatements should have size 2
      batchStatements should contain theSameElementsAs Seq(List(1, 3), List(2, 4))

      val stmtss = statements.map {
        case s: RichBoundStatementWrapper => List(s.stmt)
        case s: RichBatchStatementWrapper => s.stmt.asScala.collect{ case b: BoundStatement => b }
      }
      stmtss.foreach(stmts => stmts.size should be > 0)
      stmtss.foreach(stmts => if (stmts.size > 1) stmts.map(BoundStatementBuilder.calculateDataSize).sum should be <= 15)
      stmtss.flatten.map(s => (s.getInt(0), s.getString(1))) should contain theSameElementsAs data
    }
  }

  it should "produce empty stream when no data is available and batch size is specified in rows" in {
    conn.withSessionDo { session =>
      val bm = makeBatchBuilder(session)
      val data = Seq()
      val statements = bm(dynamicBatchKeyGen, RowsInBatch(10), 1, data.toIterator).toList
      statements should have size 0
    }
  }

  it should "produce empty stream when no data is available and batch size is specified in bytes" in {
    conn.withSessionDo { session =>
      val bm = makeBatchBuilder(session)
      val data = Seq()
      val statements = bm(dynamicBatchKeyGen, BytesInBatch(10), 1, data.toIterator).toList
      statements should have size 0
    }
  }

  it should "work with random data" in {
    conn.withSessionDo { session =>
      val bm = makeBatchBuilder(session)
      val size = 10000
      val data = (1 to size).map(x => (Random.nextInt().abs, Random.nextString(Random.nextInt(20))))
      val statements = bm(dynamicBatchKeyGen5, RowsInBatch(10), 4, data.toIterator).toList
      val batches = statements.collect { case bs: BatchStatement => bs.size() }
      statements.flatMap {
        case s: RichBoundStatementWrapper => List(s.stmt)
        case s: RichBatchStatementWrapper =>
          s.stmt.size() should be <= 10
          s.stmt.asScala.collect { case b: BoundStatement => b }
      }.map(s => (s.getInt(0), s.getString(1))).sortBy(_.toString()) should contain theSameElementsInOrderAs data.sortBy(_.toString())
    }

  }

}
