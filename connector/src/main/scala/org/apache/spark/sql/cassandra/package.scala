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

package org.apache.spark.sql

import scala.language.implicitConversions
import com.datastax.spark.connector.util.{ConfigParameter, DeprecatedConfigParameter}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.streaming.DataStreamWriter

package object cassandra {

  /** A data frame format used to access Cassandra through Connector */
  val CassandraFormat = "org.apache.spark.sql.cassandra"

  /** Returns a map of options which configure the path to Cassandra table as well as whether pushdown is enabled
    * or not */
  def cassandraOptions(
      table: String,
      keyspace: String,
      cluster: String = CassandraSourceRelation.defaultClusterName,
      pushdownEnable: Boolean = true): Map[String, String] =
    Map(
      DefaultSource.CassandraDataSourceClusterNameProperty -> cluster,
      DefaultSource.CassandraDataSourceKeyspaceNameProperty -> keyspace,
      DefaultSource.CassandraDataSourceTableNameProperty -> table,
      DefaultSource.CassandraDataSourcePushdownEnableProperty -> pushdownEnable.toString)

  implicit class DataFrameReaderWrapper(private val dfReader: DataFrameReader) extends AnyVal {
    /** Sets the format used to access Cassandra through Connector */
    def cassandraFormat: DataFrameReader = {
      dfReader.format(CassandraFormat)
    }

    /** Sets the format used to access Cassandra through Connector and configure a path to Cassandra table. */
    def cassandraFormat(
        table: String,
        keyspace: String,
        cluster: String = CassandraSourceRelation.defaultClusterName,
        pushdownEnable: Boolean = true): DataFrameReader = {

      cassandraFormat.options(cassandraOptions(table, keyspace, cluster, pushdownEnable))
    }
  }

  implicit class DataFrameWriterWrapper[T](private val dfWriter: DataFrameWriter[T]) extends AnyVal {
    /** Sets the format used to access Cassandra through Connector */
    def cassandraFormat: DataFrameWriter[T] = {
      dfWriter.format(CassandraFormat)
    }

    /** Sets the format used to access Cassandra through Connector and configure a path to Cassandra table. */
    def cassandraFormat(
        table: String,
        keyspace: String,
        cluster: String = CassandraSourceRelation.defaultClusterName,
        pushdownEnable: Boolean = true): DataFrameWriter[T] = {

      cassandraFormat.options(cassandraOptions(table, keyspace, cluster, pushdownEnable))
    }

    private def getSource(): String ={
      val dfSourceField = classOf[DataFrameWriter[_]].getDeclaredField("source")
      dfSourceField.setAccessible(true)
      dfSourceField.get(dfWriter).asInstanceOf[String]
    }

    def withTTL(constant: Int): DataFrameWriter[T] = {
      withTTL(constant.toString)
    }

    def withTTL(column: String): DataFrameWriter[T] = {
      val source: String = getSource()
      if (source != CassandraFormat) throw new IllegalArgumentException(
          s"Write destination must be $CassandraFormat for setting TTL. Destination was $source")
      dfWriter.option(CassandraSourceRelation.TTLParam.name, column)
    }

    def withWriteTime(constant: Long): DataFrameWriter[T] = {
      withWriteTime(constant.toString)
    }

    def withWriteTime(column: String): DataFrameWriter[T] = {
      val source: String = getSource()
      if (source != CassandraFormat) throw new IllegalArgumentException(
        s"Write destination must be $CassandraFormat for setting WriteTime. Destination was $source")
      dfWriter.option(CassandraSourceRelation.WriteTimeParam.name, column)
    }

  }

  implicit class DataStreamWriterWrapper[T](private val dsWriter: DataStreamWriter[T]) extends AnyVal {
    /** Sets the format used to access Cassandra through Connector */
    def cassandraFormat: DataStreamWriter[T] = {
      dsWriter.format(CassandraFormat)
    }

    /** Sets the format used to access Cassandra through Connector and configure a path to Cassandra table. */
    def cassandraFormat(
      table: String,
      keyspace: String,
      cluster: String = CassandraSourceRelation.defaultClusterName,
      pushdownEnable: Boolean = true): DataStreamWriter[T] = {

      cassandraFormat.options(cassandraOptions(table, keyspace, cluster, pushdownEnable))
    }

    private def getSource(): String ={
      val dfSourceField = classOf[DataStreamWriter[_]].getDeclaredField("source")
      dfSourceField.setAccessible(true)
      dfSourceField.get(dsWriter).asInstanceOf[String]
    }

    def withTTL(constant: Int): DataStreamWriter[T] = {
      withTTL(constant.toString)
    }

    def withTTL(column: String): DataStreamWriter[T] = {
      val source = getSource()
      if (source != CassandraFormat) throw new IllegalArgumentException(
        s"Write destination must be $CassandraFormat for setting TTL. Destination was $source")
      dsWriter.option(CassandraSourceRelation.TTLParam.name, column)
    }

    def withWriteTime(constant: Long): DataStreamWriter[T] = {
      withWriteTime(constant.toString)
    }

    def withWriteTime(column: String): DataStreamWriter[T] = {
      val source = getSource()
      if (source != CassandraFormat) throw new IllegalArgumentException(
        s"Write destination must be $CassandraFormat for setting WriteTime. Destination was $source")
      dsWriter.option(CassandraSourceRelation.WriteTimeParam.name, column)
    }
  }

  @deprecated("Use SparkSession instead of SQLContext", "2.0.0")
  implicit class CassandraSQLContextFunctions(private val sqlContext: SQLContext) extends AnyVal {

    import org.apache.spark.sql.cassandra.CassandraSQLContextParams._

    /** Set current used cluster name */
    @deprecated("Use SparkSession instead of SQLContext", "2.0.0")
    def setCluster(cluster: String): SQLContext = {
      sqlContext.setConf(SqlClusterParam.name, cluster)
      sqlContext
    }

    /** Get current used cluster name */
    @deprecated("Use SparkSession instead of SQLContext", "2.0.0")
    def getCluster: String = sqlContext.getConf(SqlClusterParam.name, SqlClusterParam.default)

    /** Set the Spark Cassandra Connector configuration parameters */
    @deprecated("Use SparkSession instead of SQLContext", "2.0.0")
    def setCassandraConf(options: Map[String, String]): SQLContext = {
      //noinspection ScalaDeprecation
      setCassandraConf(SqlClusterParam.default, options)
      sqlContext
    }

    /** Set the Spark Cassandra Connector configuration parameters which will be used when accessing
      * a given cluster */
    @deprecated("Use SparkSession instead of SQLContext", "2.0.0")
    def setCassandraConf(
        cluster: String,
        options: Map[String, String]): SQLContext = {
      checkOptions(options)
      for ((k, v) <- options) sqlContext.setConf(s"$cluster/$k", v)
      sqlContext
    }

    /** Set the Spark Cassandra Connector configuration parameters which will be used when accessing
      * a given keyspace in a given cluster */
    @deprecated("Use SparkSession instead of SQLContext", "2.0.0")
    def setCassandraConf(
        cluster: String,
        keyspace: String,
        options: Map[String, String]): SQLContext = {
      checkOptions(options)
      for ((k, v) <- options) sqlContext.setConf(s"$cluster:$keyspace/$k", v)
      sqlContext
    }
  }

  private def columnToExpr(col: Column): Expression =
    org.apache.spark.sql.classic.ColumnConversions.expression(col)

  private def exprToColumn(expr: Expression): Column =
    org.apache.spark.sql.classic.ClassicConversions.ColumnConstructorExt(Column).apply(expr)

  def ttl(column: Column): Column = {
      exprToColumn(CassandraTTL(columnToExpr(column)))
  }

  def ttl(column: String): Column = {
      ttl(Column(column))
  }

  def writeTime(column: Column): Column = {
      exprToColumn(CassandraWriteTime(columnToExpr(column)))
  }

  def writeTime(column: String): Column = {
      writeTime(Column(column))
  }

  implicit class CassandraSparkSessionFunctions(private val sparkSession: SparkSession) extends AnyVal {

    import org.apache.spark.sql.cassandra.CassandraSQLContextParams._

    /** Set current used cluster name */
    def setCluster(cluster: String): SparkSession = {
      sparkSession.conf.set(SqlClusterParam.name, cluster)
      sparkSession
    }

    /** Get current used cluster name */
    def getCluster: String = sparkSession.conf.get(SqlClusterParam.name, SqlClusterParam.default)

    /** Set the Spark Cassandra Connector configuration parameters */
    def setCassandraConf(options: Map[String, String]): SparkSession = {
      setCassandraConf(SqlClusterParam.default, options)
      sparkSession
    }

    /** Set the Spark Cassandra Connector configuration parameters which will be used when accessing
      * a given cluster */
    def setCassandraConf(
        cluster: String,
        options: Map[String, String]): SparkSession = {
      checkOptions(options)
      for ((k, v) <- options) sparkSession.conf.set(s"$cluster/$k", v)
      sparkSession
    }

    /** Set the Spark Cassandra Connector configuration parameters which will be used when accessing
      * a given keyspace in a given cluster */
    def setCassandraConf(
        cluster: String,
        keyspace: String,
        options: Map[String, String]): SparkSession = {
      checkOptions(options)
      for ((k, v) <- options) sparkSession.conf.set(s"$cluster:$keyspace/$k", v)
      sparkSession
    }
  }

  object CassandraSQLContextParams {
    // Should use general used database than Cassandra specific keyspace?
    // Other source tables don't have keyspace concept. We should make
    // an effort to set CassandraSQLContext a more database like to join
    // tables from other sources. Keyspace is equivalent to database in SQL world
    val ReferenceSection = "Cassandra SQL Context Options"

    val SqlClusterParam = ConfigParameter[String](
      name = "spark.cassandra.sql.cluster",
      section = ReferenceSection,
      default = "default",
      description = "Sets the default Cluster to inherit configuration from")

    private[cassandra] def checkOptions(options: Map[String, String]): Unit = {
      val AllValidOptions = DeprecatedConfigParameter.names ++ ConfigParameter.names
      options.keySet.foreach { name =>
        require(AllValidOptions.contains(name),
          s"Unrelated parameter. You can only set the following parameters: ${AllValidOptions.mkString(", ")}")
      }
    }
  }

}
