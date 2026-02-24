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

package com.datastax.spark.connector

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.`type`.codec.TypeCodec
import com.datastax.oss.driver.api.core.`type`.codec.registry.CodecRegistry
import com.datastax.oss.driver.api.core.cql.{AsyncResultSet, ColumnDefinitions, PreparedStatement, ResultSet, Row}
import com.datastax.spark.connector.util.DriverUtil.toName

/** Represents a single row fetched from Cassandra.
  * Offers getters to read individual fields by column name or column index.
  * The getters try to convert value to desired type, whenever possible.
  * Most of the column types can be converted to a `String`.
  * For nullable columns, you should use the `getXXXOption` getters which convert
  * `null`s to `None` values, otherwise a `NullPointerException` would be thrown.
  *
  * All getters throw an exception if column name/index is not found.
  * Column indexes start at 0.
  *
  * If the value cannot be converted to desired type,
  * [[com.datastax.spark.connector.types.TypeConversionException]] is thrown.
  *
  * Recommended getters for Cassandra types:
  *
  * - `ascii`:     `getString`, `getStringOption`
  * - `bigint`:    `getLong`, `getLongOption`
  * - `blob`:      `getBytes`, `getBytesOption`
  * - `boolean`:   `getBool`, `getBoolOption`
  * - `counter`:   `getLong`, `getLongOption`
  * - `decimal`:   `getDecimal`, `getDecimalOption`
  * - `double`:    `getDouble`, `getDoubleOption`
  * - `float`:     `getFloat`, `getFloatOption`
  * - `inet`:      `getInet`, `getInetOption`
  * - `int`:       `getInt`, `getIntOption`
  * - `text`:      `getString`, `getStringOption`
  * - `timestamp`: `getDate`, `getDateOption`
  * - `timeuuid`:  `getUUID`, `getUUIDOption`
  * - `uuid`:      `getUUID`, `getUUIDOption`
  * - `varchar`:   `getString`, `getStringOption`
  * - `varint`:    `getVarInt`, `getVarIntOption`
  * - `list`:      `getList[T]`
  * - `set`:       `getSet[T]`
  * - `map`:       `getMap[K, V]`
  *
  * Collection getters `getList`, `getSet` and `getMap` require to explicitly pass an appropriate item type:
  * {{{
  * row.getList[String]("a_list")
  * row.getList[Int]("a_list")
  * row.getMap[Int, String]("a_map")
  * }}}
  *
  * Generic `get` allows to automatically convert collections to other collection types.
  * Supported containers:
  * - `scala.collection.immutable.List`
  * - `scala.collection.immutable.Set`
  * - `scala.collection.immutable.TreeSet`
  * - `scala.collection.immutable.Vector`
  * - `scala.collection.immutable.Map`
  * - `scala.collection.immutable.TreeMap`
  * - `scala.collection.Iterable`
  * - `scala.collection.IndexedSeq`
  * - `java.util.ArrayList`
  * - `java.util.HashSet`
  * - `java.util.HashMap`
  *
  * Example:
  * {{{
  * row.get[List[Int]]("a_list")
  * row.get[Vector[Int]]("a_list")
  * row.get[java.util.ArrayList[Int]]("a_list")
  * row.get[TreeMap[Int, String]]("a_map")
  * }}}
  *
  *
  * Timestamps can be converted to other Date types by using generic `get`. Supported date types:
  * - java.util.Date
  * - java.sql.Date
  */
final class CassandraRow(val metaData: CassandraRowMetadata, val columnValues: IndexedSeq[AnyRef])
  extends ScalaGettableData with Serializable {

  /**
    * The constructor is for testing and backward compatibility only.
    * Use default constructor with shared metadata for memory saving and performance.
    *
    * @param columnNames
    * @param columnValues
    */
  @deprecated("Use default constructor", "1.6.0")
  def this(columnNames: IndexedSeq[String], columnValues: IndexedSeq[AnyRef]) =
    this(CassandraRowMetadata.fromColumnNames(columnNames), columnValues)

  override def toString = "CassandraRow" + dataAsString
}

/**
  * All CassandraRows shared data
  *
  * @param columnNames          row column names
  * @param resultSetColumnNames column names from java driver row result set, without connector aliases.
  * @param codecs               cached java driver codecs to avoid registry lookups
  *
  */
case class CassandraRowMetadata(columnNames: IndexedSeq[String],
                                resultSetColumnNames: Option[IndexedSeq[String]] = None,
                                // transient because codecs are not serializable and used only at Row parsing
                                // not and option as deserialized field will be null not None
                                @transient codecs: IndexedSeq[TypeCodec[AnyRef]] = null) {
  @transient
  lazy val namesToIndex: Map[String, Int] = columnNames.zipWithIndex.toMap.withDefaultValue(-1)
  @transient
  lazy val indexOfCqlColumnOrThrow = unaliasedColumnNames.zipWithIndex.toMap.withDefault { name =>
    throw new ColumnNotFoundException(
      s"Column not found: $name. " +
        s"Available columns are: ${columnNames.mkString("[", ", ", "]")}")
  }

  @transient
  lazy val indexOfOrThrow = namesToIndex.withDefault { name =>
    throw new ColumnNotFoundException(
      s"Column not found: $name. " +
        s"Available columns are: ${columnNames.mkString("[", ", ", "]")}")
  }

  def codecs(name: String): TypeCodec[AnyRef] =
    codecs(namesToIndex(name))

  def unaliasedColumnNames = resultSetColumnNames.getOrElse(columnNames)
}

object CassandraRowMetadata {

  def fromResultSet(columnNames: IndexedSeq[String], rs: AsyncResultSet, session: CqlSession): CassandraRowMetadata = {
    fromResultSet(columnNames: IndexedSeq[String], rs, session.getContext.getCodecRegistry)
  }

  def fromResultSet(columnNames: IndexedSeq[String], rs: AsyncResultSet, registry: CodecRegistry): CassandraRowMetadata = {
    fromColumnDefs(columnNames, rs.getColumnDefinitions, registry)
  }

  def fromResultSet(columnNames: IndexedSeq[String], rs: ResultSet, registry: CodecRegistry): CassandraRowMetadata = {
    fromColumnDefs(columnNames, rs.getColumnDefinitions, registry)
  }

  def fromPreparedStatement(columnNames: IndexedSeq[String], statement: PreparedStatement, registry: CodecRegistry): CassandraRowMetadata = {
    fromColumnDefs(columnNames, statement.getResultSetDefinitions, registry)
  }

  private def fromColumnDefs(columnNames: IndexedSeq[String], columnDefs: ColumnDefinitions, registry: CodecRegistry): CassandraRowMetadata = {
    import scala.jdk.CollectionConverters._
    val scalaColumnDefs = columnDefs.asScala.toList
    val rsColumnNames = scalaColumnDefs.map(c => toName(c.getName))
    val codecs = scalaColumnDefs.map(col => registry.codecFor(col.getType))
      .asInstanceOf[List[TypeCodec[AnyRef]]]
    CassandraRowMetadata(columnNames, Some(rsColumnNames.toIndexedSeq), codecs.toIndexedSeq)
  }

  /**
    * create metadata object without codecs. Should be used for testing only
    *
    * @param columnNames
    * @return
    */
  def fromColumnNames(columnNames: IndexedSeq[String]): CassandraRowMetadata =
    CassandraRowMetadata(columnNames, None)

  def fromColumnNames(columnNames: Seq[String]): CassandraRowMetadata =
    fromColumnNames(columnNames.toIndexedSeq)
}

object CassandraRow {

  /** Deserializes first n columns from the given `Row` and returns them as
    * a `CassandraRow` object. The number of columns retrieved is determined by the length
    * of the columnNames argument. The columnNames argument is used as metadata for
    * the newly created `CassandraRow`, but it is not used to fetch data from
    * the input `Row` in order to improve performance. Fetching column values by name is much
    * slower than fetching by index. */
  def fromJavaDriverRow(row: Row, metaData: CassandraRowMetadata): CassandraRow = {
    new CassandraRow(metaData, CassandraRow.dataFromJavaDriverRow(row, metaData).toIndexedSeq)
  }

  def dataFromJavaDriverRow(row: Row, metaData: CassandraRowMetadata): Array[Object] = {
    val length = metaData.columnNames.length
    var i = 0
    val data = new Array[Object](length)

    // Here we use a mutable while loop for performance reasons, scala for loops are
    // converted into range.foreach() and the JVM is unable to inline the foreach closure.
    // 'match' is replaced with 'if' for the same reason.
    // It is also out of the loop for performance.
    if (metaData.codecs == null) {
      //that should not happen in production, but just in case
      while (i < length) {
        data(i) = GettableData.get(row, i)
        i += 1
      }
    }
    else {
      while (i < length) {
        data(i) = GettableData.get(row, i, metaData.codecs(i))
        i += 1
      }
    }
    data
  }

  /** Creates a CassandraRow object from a map with keys denoting column names and
    * values denoting column values. */
  def fromMap(map: Map[String, Any]): CassandraRow = {
    val (columnNames, values) = map.unzip
    new CassandraRow(CassandraRowMetadata.fromColumnNames(columnNames.toIndexedSeq), values.map(_.asInstanceOf[AnyRef]).toIndexedSeq)
  }

}
