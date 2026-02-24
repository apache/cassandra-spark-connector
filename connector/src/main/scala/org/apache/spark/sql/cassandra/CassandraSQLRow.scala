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

package org.apache.spark.sql.cassandra

import java.math.BigInteger
import java.net.InetAddress
import java.sql.Timestamp
import java.time.{Instant, LocalTime}
import java.util.{Date, UUID}

import com.datastax.dse.driver.api.core.data.geometry.Geometry
import com.datastax.dse.driver.api.core.data.time.DateRange
import com.datastax.oss.driver.api.core.`type`.codec.TypeCodecs
import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.driver.api.core.data.CqlDuration
import com.datastax.spark.connector.rdd.reader.{RowReader, ThisRowReaderAsFactory}
import com.datastax.spark.connector.types.TypeConverter
import com.datastax.spark.connector.{CassandraRow, CassandraRowMetadata, GettableData, TupleValue, UDTValue}
import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.{Row => SparkRow}

final class CassandraSQLRow(val metaData: CassandraRowMetadata, val columnValues: IndexedSeq[AnyRef])
  extends GettableData with SparkRow with Serializable {

  protected def fieldNames = metaData

  private[spark] def this() = this(null, null) // required by Kryo for deserialization :(

  /** Generic getter for getting columns of any type.
    * Looks the column up by its index. First column starts at index 0. */
  private def get[T](index: Int)(implicit c: TypeConverter[T]): T =
    c.convert(columnValues(index))

  override def apply(i: Int) = columnValues(i)
  override def copy() = this // immutable
  override def size = super.size

  override def getDouble(i: Int) = get[Double](i)
  override def getFloat(i: Int) = get[Float](i)
  override def getLong(i: Int) = get[Long](i)
  override def getByte(i: Int) = get[Byte](i)
  override def getBoolean(i: Int) = get[Boolean](i)
  override def getShort(i: Int) = get[Short](i)
  override def getInt(i: Int) = get[Int](i)
  override def getString(i: Int) = get[String](i)
  override def get(i: Int) = get[Any](i)

  override def isNullAt(i: Int): Boolean = super[GettableData].isNullAt(i)

  override def toSeq: Seq[Any] = columnValues
}


object CassandraSQLRow {

  def fromJavaDriverRow(row: Row, metaData:CassandraRowMetadata): CassandraSQLRow = {
    val data = CassandraRow.dataFromJavaDriverRow(row, metaData)
    new CassandraSQLRow(metaData, data.map(toSparkSqlType).toIndexedSeq)
  }

  implicit object CassandraSQLRowReader extends RowReader[CassandraSQLRow] with ThisRowReaderAsFactory[CassandraSQLRow] {

    override def read(row: Row, metaData:CassandraRowMetadata): CassandraSQLRow =
      fromJavaDriverRow(row, metaData)

    override def neededColumns = None
    override def targetClass = classOf[CassandraSQLRow]
  }

  def toSparkSqlType(value: Any): AnyRef = {
    val sparkSqlType: PartialFunction[Any, AnyRef] = {
      case geo: Geometry => geo.asWellKnownText()
      case date: Date => new Timestamp(date.getTime)
      case localDate: org.joda.time.LocalDate =>
        new java.sql.Date(localDate.toDateTimeAtStartOfDay().getMillis)
      case localDate: java.time.LocalDate => java.sql.Date.valueOf(localDate)
      case localTime: java.time.LocalTime => localTime.toNanoOfDay.asInstanceOf[AnyRef]
      case duration: java.time.Duration => duration.toMillis.asInstanceOf[AnyRef]
      case duration: CqlDuration => TypeCodecs.DURATION.format(duration)
      case instant: java.time.Instant => new java.sql.Timestamp(instant.toEpochMilli)
      case bigInteger: BigInteger => Decimal(bigInteger.toString)
      case bigDecimal: java.math.BigDecimal => Decimal(bigDecimal)
      case inetAddress: InetAddress => inetAddress.getHostAddress
      case uuid: UUID => uuid.toString
      case set: Set[_] => set.map(toSparkSqlType).toSeq
      case list: List[_] => list.map(toSparkSqlType)
      case map: Map[_, _] => map map { case(k, v) => (toSparkSqlType(k), toSparkSqlType(v))}
      case udt: UDTValue => SparkRow(udt.columnValues.map(toSparkSqlType): _*)
      case tupleValue: TupleValue => SparkRow(tupleValue.values.map(toSparkSqlType): _*)
      case dateRange: DateRange => dateRange.toString
      case _ => value.asInstanceOf[AnyRef]
    }
    sparkSqlType(value)
  }

  def toUnsafeSqlType(value: Any): AnyRef = {
    val sparkSqlType: PartialFunction[Any, AnyRef] = {
      case geom: Geometry => geom.asWellKnownText()
      case duration: CqlDuration => TypeCodecs.DURATION.format(duration)
      case javaDate: java.util.Date => java.sql.Timestamp.from(javaDate.toInstant)
      case localDate: java.time.LocalDate => java.sql.Date.valueOf(localDate)
      case localDate: org.joda.time.LocalDate =>
        java.time.LocalDate.of(localDate.getYear,localDate.getMonthOfYear, localDate.getDayOfMonth)
          .toEpochDay.asInstanceOf[AnyRef]
      case inetAddress: InetAddress => inetAddress.getHostAddress
      case uuid: UUID => uuid.toString
      case set: Set[_] => set.map(toUnsafeSqlType).toSeq
      case list: List[_] => list.map(toUnsafeSqlType)
      case map: Map[_, _] => map map { case(k, v) => (toUnsafeSqlType(k), toUnsafeSqlType(v))}
      case udt: UDTValue => UDTValue(udt.metaData, udt.columnValues.map(toUnsafeSqlType))
      case tupleValue: TupleValue => TupleValue(tupleValue.values.map(toUnsafeSqlType): _*)
      case dateRange: DateRange => dateRange.toString
      case time: LocalTime => time.toNanoOfDay: java.lang.Long
      case instant: Instant => java.sql.Timestamp.from(instant)
      case _ => value.asInstanceOf[AnyRef]
    }
    sparkSqlType(value)
  }


  val empty = new CassandraSQLRow(null, IndexedSeq.empty)
}

