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

package com.datastax.spark.connector.rdd.typeTests

import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.driver.api.core.{CqlSession, Version}
import com.datastax.spark.connector._
import com.datastax.spark.connector.cluster.DefaultCluster
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.mapper.ColumnMapper
import com.datastax.spark.connector.rdd.ValidRDDType
import com.datastax.spark.connector.rdd.reader.RowReaderFactory
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.cassandra.{DataFrameReaderWrapper, DataFrameWriterWrapper}

import scala.collection.convert.ImplicitConversionsToScala._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._


abstract class VectorTypeTest[
  ScalaType: ClassTag : TypeTag,
  DriverType <: Number : ClassTag,
  CaseClassType <: Product : ClassTag : TypeTag : ColumnMapper: RowReaderFactory : ValidRDDType](typeName: String) extends SparkCassandraITFlatSpecBase with DefaultCluster
{
  override lazy val conn = CassandraConnector(sparkConf)

  val VectorTable = "vectors"

  def createVectorTable(session: CqlSession, table: String): Unit = {
    session.execute(
      s"""CREATE TABLE IF NOT EXISTS $ks.$table (
         |  id INT PRIMARY KEY,
         |  v VECTOR<$typeName, 3>
         |)""".stripMargin)
  }

  def minCassandraVersion: Option[Version] = Some(Version.parse("5.0-beta1"))

  def minDSEVersion: Option[Version] = None

  def vectorFromInts(ints: Int*): Seq[ScalaType]

  def vectorItem(id: Int, v: Seq[ScalaType]): CaseClassType

  override def beforeClass() {
    conn.withSessionDo { session =>
      session.execute(
        s"""CREATE KEYSPACE IF NOT EXISTS $ks
           |WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }"""
          .stripMargin)
    }
  }

  private def assertVectors(rows: List[Row], expectedVectors: Seq[Seq[ScalaType]]): Unit = {
    val returnedVectors = for (i <- expectedVectors.indices) yield {
      rows.find(_.getInt("id") == i + 1).get.getVector("v", implicitly[ClassTag[DriverType]].runtimeClass.asInstanceOf[Class[Number]]).iterator().toSeq
    }

    returnedVectors should contain theSameElementsInOrderAs expectedVectors
  }

  "SCC" should s"write case class instances with $typeName vector using DataFrame API" in from(minCassandraVersion, minDSEVersion) {
    val table = s"${typeName.toLowerCase}_write_caseclass_to_df"
    conn.withSessionDo { session =>
      createVectorTable(session, table)

      spark.createDataFrame(Seq(vectorItem(1, vectorFromInts(1, 2, 3)), vectorItem(2, vectorFromInts(4, 5, 6))))
        .write
        .cassandraFormat(table, ks)
        .mode(SaveMode.Append)
        .save()
      assertVectors(session.execute(s"SELECT * FROM $ks.$table").all().iterator().toList,
        Seq(vectorFromInts(1, 2, 3), vectorFromInts(4, 5, 6)))

      spark.createDataFrame(Seq(vectorItem(2, vectorFromInts(6, 5, 4)), vectorItem(3, vectorFromInts(7, 8, 9))))
        .write
        .cassandraFormat(table, ks)
        .mode(SaveMode.Append)
        .save()
      assertVectors(session.execute(s"SELECT * FROM $ks.$table").all().iterator().toList,
        Seq(vectorFromInts(1, 2, 3), vectorFromInts(6, 5, 4), vectorFromInts(7, 8, 9)))

      spark.createDataFrame(Seq(vectorItem(1, vectorFromInts(9, 8, 7)), vectorItem(2, vectorFromInts(10, 11, 12))))
        .write
        .cassandraFormat(table, ks)
        .mode(SaveMode.Overwrite)
        .option("confirm.truncate", value = true)
        .save()
      assertVectors(session.execute(s"SELECT * FROM $ks.$table").all().iterator().toList,
        Seq(vectorFromInts(9, 8, 7), vectorFromInts(10, 11, 12)))
    }
  }

  it should s"write tuples with $typeName vectors using DataFrame API" in from(minCassandraVersion, minDSEVersion) {
    val table = s"${typeName.toLowerCase}_write_tuple_to_df"
    conn.withSessionDo { session =>
      createVectorTable(session, table)

      spark.createDataFrame(Seq((1, vectorFromInts(1, 2, 3)), (2, vectorFromInts(4, 5, 6))))
        .toDF("id", "v")
        .write
        .cassandraFormat(table, ks)
        .mode(SaveMode.Append)
        .save()
      assertVectors(session.execute(s"SELECT * FROM $ks.$table").all().iterator().toList,
        Seq(vectorFromInts(1, 2, 3), vectorFromInts(4, 5, 6)))
    }
  }

  it should s"write case class instances with $typeName vectors using RDD API" in from(minCassandraVersion, minDSEVersion) {
    val table = s"${typeName.toLowerCase}_write_caseclass_to_rdd"
    conn.withSessionDo { session =>
      createVectorTable(session, table)

      spark.sparkContext.parallelize(Seq(vectorItem(1, vectorFromInts(1, 2, 3)), vectorItem(2, vectorFromInts(4, 5, 6))))
        .saveToCassandra(ks, table)
      assertVectors(session.execute(s"SELECT * FROM $ks.$table").all().iterator().toList,
        Seq(vectorFromInts(1, 2, 3), vectorFromInts(4, 5, 6)))
    }
  }

  it should s"write tuples with $typeName vectors using RDD API" in from(minCassandraVersion, minDSEVersion) {
    val table = s"${typeName.toLowerCase}_write_tuple_to_rdd"
    conn.withSessionDo { session =>
      createVectorTable(session, table)

      spark.sparkContext.parallelize(Seq((1, vectorFromInts(1, 2, 3)), (2, vectorFromInts(4, 5, 6))))
        .saveToCassandra(ks, table)
      assertVectors(session.execute(s"SELECT * FROM $ks.$table").all().iterator().toList,
        Seq(vectorFromInts(1, 2, 3), vectorFromInts(4, 5, 6)))
    }
  }

  it should s"read case class instances with $typeName vectors using DataFrame API" in from(minCassandraVersion, minDSEVersion) {
    val table = s"${typeName.toLowerCase}_read_caseclass_from_df"
    conn.withSessionDo { session =>
      createVectorTable(session, table)
    }
    spark.sparkContext.parallelize(Seq((1, vectorFromInts(1, 2, 3)), (2, vectorFromInts(4, 5, 6))))
      .saveToCassandra(ks, table)

    import spark.implicits._
    spark.read.cassandraFormat(table, ks).load().as[CaseClassType].collect() should contain theSameElementsAs
      Seq(vectorItem(1, vectorFromInts(1, 2, 3)), vectorItem(2, vectorFromInts(4, 5, 6)))
  }

  it should s"read tuples with $typeName vectors using DataFrame API" in from(minCassandraVersion, minDSEVersion) {
    val table = s"${typeName.toLowerCase}_read_tuple_from_df"
    conn.withSessionDo { session =>
      createVectorTable(session, table)
    }
    spark.sparkContext.parallelize(Seq((1, vectorFromInts(1, 2, 3)), (2, vectorFromInts(4, 5, 6))))
      .saveToCassandra(ks, table)

    import spark.implicits._
    spark.read.cassandraFormat(table, ks).load().as[(Int, Seq[ScalaType])].collect() should contain theSameElementsAs
      Seq((1, vectorFromInts(1, 2, 3)), (2, vectorFromInts(4, 5, 6)))
  }

  it should s"read case class instances with $typeName vectors using RDD API" in from(minCassandraVersion, minDSEVersion) {
    val table = s"${typeName.toLowerCase}_read_caseclass_from_rdd"
    conn.withSessionDo { session =>
      createVectorTable(session, table)
    }
    spark.sparkContext.parallelize(Seq((1, vectorFromInts(1, 2, 3)), (2, vectorFromInts(4, 5, 6))))
      .saveToCassandra(ks, table)

    spark.sparkContext.cassandraTable[CaseClassType](ks, table).collect() should contain theSameElementsAs
      Seq(vectorItem(1, vectorFromInts(1, 2, 3)), vectorItem(2, vectorFromInts(4, 5, 6)))
  }

  it should s"read tuples with $typeName vectors using RDD API" in from(minCassandraVersion, minDSEVersion) {
    val table = s"${typeName.toLowerCase}_read_tuple_from_rdd"
    conn.withSessionDo { session =>
      createVectorTable(session, table)
    }
    spark.sparkContext.parallelize(Seq((1, vectorFromInts(1, 2, 3)), (2, vectorFromInts(4, 5, 6))))
      .saveToCassandra(ks, table)

    spark.sparkContext.cassandraTable[(Int, Seq[ScalaType])](ks, table).collect() should contain theSameElementsAs
      Seq((1, vectorFromInts(1, 2, 3)), (2, vectorFromInts(4, 5, 6)))
  }

  it should s"read rows with $typeName vectors using SQL API" in from(minCassandraVersion, minDSEVersion) {
    val table = s"${typeName.toLowerCase}_read_rows_from_sql"
    conn.withSessionDo { session =>
      createVectorTable(session, table)
    }
    spark.sparkContext.parallelize(Seq((1, vectorFromInts(1, 2, 3)), (2, vectorFromInts(4, 5, 6))))
      .saveToCassandra(ks, table)

    import spark.implicits._
    spark.conf.set("spark.sql.catalog.casscatalog", "com.datastax.spark.connector.datasource.CassandraCatalog")
    spark.sql(s"SELECT * FROM casscatalog.$ks.$table").as[(Int, Seq[ScalaType])].collect() should contain theSameElementsAs
      Seq((1, vectorFromInts(1, 2, 3)), (2, vectorFromInts(4, 5, 6)))
  }

}

class IntVectorTypeTest extends VectorTypeTest[Int, Integer, IntVectorItem]("INT") {
  override def vectorFromInts(ints: Int*): Seq[Int] = ints

  override def vectorItem(id: Int, v: Seq[Int]): IntVectorItem = IntVectorItem(id, v)
}

case class IntVectorItem(id: Int, v: Seq[Int])

class LongVectorTypeTest extends VectorTypeTest[Long, java.lang.Long, LongVectorItem]("BIGINT") {
  override def vectorFromInts(ints: Int*): Seq[Long] = ints.map(_.toLong)

  override def vectorItem(id: Int, v: Seq[Long]): LongVectorItem = LongVectorItem(id, v)
}

case class LongVectorItem(id: Int, v: Seq[Long])

class FloatVectorTypeTest extends VectorTypeTest[Float, java.lang.Float, FloatVectorItem]("FLOAT") {
  override def vectorFromInts(ints: Int*): Seq[Float] = ints.map(_.toFloat + 0.1f)

  override def vectorItem(id: Int, v: Seq[Float]): FloatVectorItem = FloatVectorItem(id, v)
}

case class FloatVectorItem(id: Int, v: Seq[Float])

class DoubleVectorTypeTest extends VectorTypeTest[Double, java.lang.Double, DoubleVectorItem]("DOUBLE") {
  override def vectorFromInts(ints: Int*): Seq[Double] = ints.map(_.toDouble + 0.1d)

  override def vectorItem(id: Int, v: Seq[Double]): DoubleVectorItem = DoubleVectorItem(id, v)
}

case class DoubleVectorItem(id: Int, v: Seq[Double])

