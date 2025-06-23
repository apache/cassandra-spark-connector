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

package com.datastax.spark.connector.types

import java.math.BigInteger
import java.net.InetAddress
import java.nio.ByteBuffer
import java.sql.Timestamp
import java.time.{ZoneId, ZoneOffset, LocalDate => JavaLocalDate}
import java.util.{Calendar, Date, GregorianCalendar, TimeZone, UUID}

import com.datastax.dse.driver.api.core.data.geometry.{LineString, Point, Polygon}
import com.datastax.dse.driver.api.core.data.time.DateRange
import com.datastax.oss.driver.api.core.data.{CqlDuration, CqlVector}
import com.datastax.spark.connector.TupleValue
import com.datastax.spark.connector.UDTValue.UDTValueConverter
import com.datastax.spark.connector.util.ByteBufferUtil
import com.datastax.spark.connector.util.Symbols._
import org.apache.commons.lang3.tuple

import scala.collection.immutable.{TreeMap, TreeSet}
import scala.reflect.runtime.universe._
import scala.jdk.CollectionConverters._

class TypeConversionException(val message: String, cause: Exception = null) extends Exception(message, cause)

/** Machinery for converting objects of any type received from Cassandra into objects of Scala types.
  * Every converter knows how to convert object to one type. See `TypeConverter`
  * companion object for a list of available converters. */
trait TypeConverter[T] extends Serializable {
  /** Compile time type of the converter target */
  type targetType = T

  /** TypeTag for the target type. */
  def targetTypeTag: TypeTag[T]

  /** String representation of the converter target type.*/
  def targetTypeName: String =
    targetTypeTag.tpe.toString

  /** Returns a function converting an object into `T`. */
  def convertPF: PartialFunction[Any, T]

  /** Converts and object or throws TypeConversionException if the object can't be converted. */
  def convert(obj: Any): T = {
    convertPF.applyOrElse(obj, (_: Any) =>
      if (obj != null)
        throw new TypeConversionException(s"Cannot convert object $obj of type ${obj.getClass} to $targetTypeName.")
      else
        throw new TypeConversionException(s"Cannot convert object $obj to $targetTypeName.")
    )
  }
}

/** Handles nullable types and converts any null to null. */
trait NullableTypeConverter[T] extends TypeConverter[T] {
  override def convert(obj: Any): T =
    if (obj != null)
      super.convert(obj)
    else
      null.asInstanceOf[T]
}

/** Chains together several converters converting to the same type.
  * This way you can extend functionality of any converter to support new input types. */
class ChainedTypeConverter[T](converters: TypeConverter[T]*) extends NullableTypeConverter[T] {
  def targetTypeTag = converters.head.targetTypeTag
  def convertPF = converters.map(_.convertPF).reduceLeft(_ orElse _)
}

/** Defines a set of converters and implicit functions used to look up an appropriate converter for
  * a desired type. Thanks to implicit method lookup, it is possible to implement a generic
  * method `CassandraRow#get`, which picks up the right converter basing solely on its type argument. */
object TypeConverter {

  lazy val defaultTimezone = TimeZone.getDefault

  private val AnyTypeTag = implicitly[TypeTag[Any]]

  implicit object AnyConverter extends TypeConverter[Any] {
    def targetTypeTag = AnyTypeTag

    def convertPF = {
      case obj => obj
    }
  }

  private val AnyRefTypeTag = implicitly[TypeTag[AnyRef]]

  implicit object AnyRefConverter extends TypeConverter[AnyRef] {
    def targetTypeTag = AnyRefTypeTag

    def convertPF = {
      case obj => obj.asInstanceOf[AnyRef]
    }
  }

  private val BooleanTypeTag = implicitly[TypeTag[Boolean]]

  implicit object BooleanConverter extends TypeConverter[Boolean] {
    def targetTypeTag = BooleanTypeTag

    def convertPF = {
      case x: java.lang.Boolean => x
      case x: java.lang.Integer => x != 0
      case x: java.lang.Long => x != 0L
      case x: java.math.BigInteger => x != java.math.BigInteger.ZERO
      case x: String => x.toBoolean
    }
  }

  private val JavaBooleanTypeTag = implicitly[TypeTag[java.lang.Boolean]]

  implicit object JavaBooleanConverter extends NullableTypeConverter[java.lang.Boolean] {
    def targetTypeTag = JavaBooleanTypeTag

    def convertPF = BooleanConverter.convertPF.andThen(_.asInstanceOf[java.lang.Boolean])
  }

  private val ByteTypeTag = implicitly[TypeTag[Byte]]

  implicit object ByteConverter extends TypeConverter[Byte] {
    def targetTypeTag = ByteTypeTag

    def convertPF = {
      case x: Number => x.byteValue
      case x: String => x.toByte
    }
  }

  private val JavaByteTypeTag = implicitly[TypeTag[java.lang.Byte]]

  implicit object JavaByteConverter extends NullableTypeConverter[java.lang.Byte] {
    def targetTypeTag = JavaByteTypeTag

    def convertPF = ByteConverter.convertPF.andThen(_.asInstanceOf[java.lang.Byte])
  }

  private val ShortTypeTag = implicitly[TypeTag[Short]]

  implicit object ShortConverter extends TypeConverter[Short] {
    def targetTypeTag = ShortTypeTag

    def convertPF = {
      case x: Number => x.shortValue
      case x: String => x.toShort
    }
  }

  private val JavaShortTypeTag = implicitly[TypeTag[java.lang.Short]]

  implicit object JavaShortConverter extends NullableTypeConverter[java.lang.Short] {
    def targetTypeTag = JavaShortTypeTag

    def convertPF = ShortConverter.convertPF.andThen(_.asInstanceOf[java.lang.Short])
  }

  private val IntTypeTag = implicitly[TypeTag[Int]]

  implicit object IntConverter extends TypeConverter[Int] {
    def targetTypeTag = IntTypeTag

    def convertPF = {
      case x: Number => x.intValue
      case x: String => x.toInt
    }
  }

  private val JavaIntTypeTag = implicitly[TypeTag[java.lang.Integer]]

  implicit object JavaIntConverter extends NullableTypeConverter[java.lang.Integer] {
    def targetTypeTag = JavaIntTypeTag

    def convertPF = IntConverter.convertPF.andThen(_.asInstanceOf[java.lang.Integer])
  }

  private val LongTypeTag = implicitly[TypeTag[Long]]

  implicit object LongConverter extends TypeConverter[Long] {
    def targetTypeTag = LongTypeTag

    def convertPF = {
      case x: Number => x.longValue
      case x: Date => x.getTime
      case x: Calendar => x.getTimeInMillis
      case x: java.time.Instant => x.toEpochMilli
      case x: java.time.LocalTime => x.toNanoOfDay
      case x: java.time.LocalDate => x.toEpochDay
      case x: java.time.Duration => x.toMillis
      case x: String => x.toLong
    }
  }

  private val JavaLongTypeTag = implicitly[TypeTag[java.lang.Long]]

  implicit object JavaLongConverter extends NullableTypeConverter[java.lang.Long] {
    def targetTypeTag = JavaLongTypeTag

    def convertPF = LongConverter.convertPF.andThen(_.asInstanceOf[java.lang.Long])
  }

  private val FloatTypeTag = implicitly[TypeTag[Float]]

  implicit object FloatConverter extends TypeConverter[Float] {
    def targetTypeTag = FloatTypeTag

    def convertPF = {
      case x: Number => x.floatValue
      case x: String => x.toFloat
    }
  }

  private val JavaFloatTypeTag = implicitly[TypeTag[java.lang.Float]]

  implicit object JavaFloatConverter extends NullableTypeConverter[java.lang.Float] {
    def targetTypeTag = JavaFloatTypeTag

    def convertPF = FloatConverter.convertPF.andThen(_.asInstanceOf[java.lang.Float])
  }

  private val DoubleTypeTag = implicitly[TypeTag[Double]]

  implicit object DoubleConverter extends TypeConverter[Double] {
    def targetTypeTag = DoubleTypeTag

    def convertPF = {
      case x: Number => x.doubleValue
      case x: String => x.toDouble
    }
  }

  private val JavaDoubleTypeTag = implicitly[TypeTag[java.lang.Double]]

  implicit object JavaDoubleConverter extends NullableTypeConverter[java.lang.Double] {
    def targetTypeTag = JavaDoubleTypeTag

    def convertPF = DoubleConverter.convertPF.andThen(_.asInstanceOf[java.lang.Double])
  }

  private val StringTypeTag = implicitly[TypeTag[String]]

  implicit object StringConverter extends NullableTypeConverter[String] {
    def targetTypeTag = StringTypeTag

    def convertPF = {
      case x: Date => TimestampFormatter.format(x)
      case x: Array[Byte] => byteArrayToString(x)
      case x: ByteBuffer => byteArrayToString(ByteBufferUtil.toArray(x))
      case x: Map[_, _] => x.map(kv => convert(kv._1) + ": " + convert(kv._2)).mkString("{", ",", "}")
      case x: Set[_] => x.map(convert).mkString("{", ",", "}")
      case x: Seq[_] => x.map(convert).mkString("[", ",", "]")
      case inetAddress: InetAddress => inetAddress.getHostAddress
      case x: Any => x.toString
    }
  }

  private val HEX_CHARS = Array(
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'
  )
  private def byteArrayToString (x: Array[Byte]) : String = {
    val result = new StringBuilder(x.length * 2) 
    x.foreach { b =>
      val v = b & 0xFF
      result.append(HEX_CHARS(v >>> 4))
      result.append(HEX_CHARS(v & 0x0F))
    }
    "0x" + result.toString()
  }
  private def stringToByteArray (x: String)  = new BigInteger(x.substring(2), 16).toByteArray

  private val ByteBufferTypeTag = implicitly[TypeTag[ByteBuffer]]

  implicit object ByteBufferConverter extends NullableTypeConverter[ByteBuffer] {
    def targetTypeTag = ByteBufferTypeTag

    def convertPF = {
      case x: ByteBuffer => x
      case x: Array[Byte] => ByteBuffer.wrap(x)
      case x: String => ByteBuffer.wrap(stringToByteArray(x))
    }
  }

  private val ByteArrayTypeTag = implicitly[TypeTag[Array[Byte]]]

  implicit object ByteArrayConverter extends NullableTypeConverter[Array[Byte]] {
    def targetTypeTag = ByteArrayTypeTag

    def convertPF = {
      case x: Array[Byte] => x
      case x: ByteBuffer => ByteBufferUtil.toArray(x)
      case x: String => stringToByteArray(x)

    }
  }

  /** For backward compatibility this parses following formats YYYY || YYYY'Z' to YYYY-01-01 dates with local or
    * UTC time zones respectively.*/
  private object YearParser {
    def applicable(x: String): Boolean =
      x.length == 4 || x.length == 5

    def parseToLocalDate(date: String): JavaLocalDate = {
      JavaLocalDate.of(date.take(4).toInt, 1, 1)
    }

    def parseToDate(date: String): Date = {
      if (date.length == 5 && date.charAt(4) == 'Z')
        Date.from(parseToLocalDate(date).atStartOfDay.toInstant(ZoneOffset.UTC))
      else
        Date.from(parseToLocalDate(date).atStartOfDay(ZoneId.systemDefault()).toInstant)
    }
  }

  private val DateTypeTag = implicitly[TypeTag[Date]]

  implicit object DateConverter extends NullableTypeConverter[Date] {
    def targetTypeTag = DateTypeTag

    def convertPF = {
      case x: Date => x
      case x: Calendar => x.getTime
      case x: Long => new Date(x)
      case x: UUID if x.version() == 1 => new Date(x.timestamp())
      case x: String if YearParser.applicable(x) => YearParser.parseToDate(x)
      case x: String => TimestampParser.parse(x)
      case x: JavaLocalDate => Date.from(x.atStartOfDay(ZoneId.systemDefault()).toInstant)
      case x: java.time.Instant => Date.from(x)
    }
  }

  private val SqlDateTypeTag = implicitly[TypeTag[java.sql.Date]]

  implicit object SqlDateConverter extends NullableTypeConverter[java.sql.Date] {
    def targetTypeTag = SqlDateTypeTag

    def convertPF = {
      case x: String => SqlDateConverter.convert(DateConverter.convert(x))
      case x: Date => new java.sql.Date(x.getTime)
      case x: JavaLocalDate => new java.sql.Date(x.atStartOfDay(ZoneId.systemDefault()).toInstant.toEpochMilli)
    }
  }

  private val GregorianCalendarTypeTag = implicitly[TypeTag[GregorianCalendar]]

  implicit object GregorianCalendarConverter extends NullableTypeConverter[GregorianCalendar] {
    private[this] def calendar(date: Date): GregorianCalendar = {
      val c = new GregorianCalendar()
      c.setTime(date)
      c
    }
    def targetTypeTag = GregorianCalendarTypeTag
    def convertPF = DateConverter.convertPF.andThen(calendar _)
  }

  private val TimestampTypeTag = implicitly[TypeTag[Timestamp]]

  implicit object TimestampConverter extends NullableTypeConverter[Timestamp] {
    def targetTypeTag = TimestampTypeTag
    override def convertPF = {
      case x: Timestamp => x
      case x => Timestamp.from(DateConverter.convert(x).toInstant())
    }
  }

  private val BigIntTypeTag = implicitly[TypeTag[BigInt]]

  implicit object BigIntConverter extends NullableTypeConverter[BigInt] {
    def targetTypeTag = BigIntTypeTag
    def convertPF = {
      case x: BigInt => x
      case x: java.math.BigInteger => x
      case x: java.lang.Integer => BigInt(x)
      case x: java.lang.Long => BigInt(x)
      case x: String => BigInt(x)
    }
  }

  private val JavaBigIntegerTypeTag = implicitly[TypeTag[java.math.BigInteger]]

  implicit object JavaBigIntegerConverter extends NullableTypeConverter[java.math.BigInteger] {
    def targetTypeTag = JavaBigIntegerTypeTag
    def convertPF = {
      case x: BigInt => x.bigInteger
      case x: java.math.BigInteger => x
      case x: java.lang.Integer => new java.math.BigInteger(x.toString)
      case x: java.lang.Long => new java.math.BigInteger(x.toString)
      case x: String => new java.math.BigInteger(x)
      case x: java.math.BigDecimal if x.scale() <= 0 => x.toBigInteger
      case x: java.math.BigDecimal if x.scale() > 0 => throw new TypeConversionException(
        s"BigDecimal ($x) has scale greater than 0 (Scale: ${x.scale()}) and cannot be converted to BigInteger")
    }
  }

  private val BigDecimalTypeTag = implicitly[TypeTag[BigDecimal]]

  implicit object BigDecimalConverter extends NullableTypeConverter[BigDecimal] {
    def targetTypeTag = BigDecimalTypeTag
    def convertPF = {
      case x: Number => BigDecimal(x.toString)
      case x: String => BigDecimal(x)
    }
  }

  private val JavaBigDecimalTypeTag = implicitly[TypeTag[java.math.BigDecimal]]

  implicit object JavaBigDecimalConverter extends NullableTypeConverter[java.math.BigDecimal] {
    def targetTypeTag = JavaBigDecimalTypeTag
    def convertPF = {
      case x: Number => new java.math.BigDecimal(x.toString)
      case x: String => new java.math.BigDecimal(x)
    }
  }

  private val UUIDTypeTag = implicitly[TypeTag[UUID]]

  implicit object UUIDConverter extends NullableTypeConverter[UUID] {
    def targetTypeTag = UUIDTypeTag
    def convertPF = {
      case x: UUID => x
      case x: String => UUID.fromString(x)
    }
  }

  private val InetAddressTypeTag = implicitly[TypeTag[InetAddress]]

  implicit object InetAddressConverter extends NullableTypeConverter[InetAddress] {
    def targetTypeTag = InetAddressTypeTag
    val ipRegex = """.*?(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3}).*""".r
    def convertPF = {
      case x: InetAddress => x
      case x: String => {
        x match {
          case ipRegex(ip1, ip2, ip3, ip4) => InetAddress.getByName(s"${ip1}.${ip2}.${ip3}.${ip4}")
          case _ => InetAddress.getByName(x)
        }
      }
    }
  }

  private val JavaLocalDateTypeTag = implicitly[TypeTag[JavaLocalDate]]

  implicit object JavaLocalDateConverter extends NullableTypeConverter[JavaLocalDate] {

    private def fromDateFields(date: Date): JavaLocalDate = {
      if (date == null) {
        throw new IllegalArgumentException("The date must not be null")
      }
      if (date.getTime() < 0) {
        // handle years in era BC
        val cal = new GregorianCalendar();
        cal.setTime(date);
        fromCalendarFields(cal);
      } else {
        JavaLocalDate.of(
          date.getYear() + 1900,
          date.getMonth() + 1,
          date.getDate()
        )
      }
    }

    private def fromCalendarFields(calendar : Calendar): JavaLocalDate = {
      if (calendar == null) {
        throw new IllegalArgumentException("The calendar must not be null")
      }
      val era = calendar.get(Calendar.ERA)
      val yearOfEra = calendar.get(Calendar.YEAR)
      JavaLocalDate.of(
        if (era == GregorianCalendar.AD) yearOfEra else 1 - yearOfEra,
        calendar.get(Calendar.MONTH) + 1,
        calendar.get(Calendar.DAY_OF_MONTH)
      )
    }

    def targetTypeTag = JavaLocalDateTypeTag

    def convertPF = {
      case x: JavaLocalDate => x
      case x: String if YearParser.applicable(x) => YearParser.parseToLocalDate(x)
      case x: String => JavaLocalDate.parse(x)
      case x: Int => JavaLocalDate.ofEpochDay(x)
      case x: Long => JavaLocalDate.ofEpochDay(x)
      case x: Date => fromDateFields(x)
    }
  }

  private val JavaLocalTimeTypeTag = implicitly[TypeTag[java.time.LocalTime]]

  implicit object JavaLocalTimeConverter extends NullableTypeConverter[java.time.LocalTime] {
    def targetTypeTag = JavaLocalTimeTypeTag

    def safeParse(str: String): java.time.LocalTime = {
      scala.util.Try(java.time.LocalTime.parse(str))
        .getOrElse(java.time.LocalTime.ofNanoOfDay(str.toLong))
    }

    def convertPF = {
      case x: java.time.LocalTime => x
      case x: String => safeParse(x)
      case x: Long => java.time.LocalTime.ofNanoOfDay(x)
      case x: Int => java.time.LocalTime.ofNanoOfDay(x)
    }
  }

  private val JavaDurationTypeTag = implicitly[TypeTag[java.time.Duration]]

  implicit object JavaDurationConverter extends NullableTypeConverter[java.time.Duration] {
    def targetTypeTag = JavaDurationTypeTag


    def convertPF = {
      case x: java.time.Duration => x
      case x: String => java.time.Duration.parse(x)
      case x: Long => java.time.Duration.ofMillis(x)
      case x: Int => java.time.Duration.ofMillis(x)
    }
  }

  private val DurationTypeTag = TypeTag.synchronized {
    implicitly[TypeTag[CqlDuration]]
  }

  implicit object DurationConverter extends NullableTypeConverter[CqlDuration] {
    def targetTypeTag = DurationTypeTag

    override def convertPF = {
      case x: CqlDuration => x
      case x: String => CqlDuration.from(x)
      case x: Long => CqlDuration.newInstance(0, 0, x)
    }
  }

  private val JavaInstantTypeTag = implicitly[TypeTag[java.time.Instant]]

  implicit object JavaInstantConverter extends NullableTypeConverter[java.time.Instant] {
    def targetTypeTag = JavaInstantTypeTag

    def convertPF = {
      case x: java.time.Instant => x
      case x: String => TimestampParser.parse(x).toInstant
      case x: Long => java.time.Instant.ofEpochMilli(x)
      case x: java.sql.Timestamp => x.toInstant
      case x => java.time.Instant.ofEpochMilli(DateConverter.convert(x).getTime)
    }
  }

  private val PointTypeTag = TypeTag.synchronized {
    implicitly[TypeTag[Point]]
  }

  implicit object PointConverter extends NullableTypeConverter[Point] {
    def targetTypeTag = PointTypeTag
    override def convertPF = {
      case x: Point => x
      case x: String => Point.fromWellKnownText(x)
    }
  }

  private val PolygonTypeTag = TypeTag.synchronized {
    implicitly[TypeTag[Polygon]]
  }

  implicit object PolygonConverter extends NullableTypeConverter[Polygon] {
    def targetTypeTag = PolygonTypeTag
    override def convertPF = {
      case x: Polygon => x
      case x: String => Polygon.fromWellKnownText(x)
    }
  }

  private val LineStringTypeTag = TypeTag.synchronized {
    implicitly[TypeTag[LineString]]
  }

  implicit object LineStringConverter extends NullableTypeConverter[LineString] {
    def targetTypeTag = LineStringTypeTag
    override def convertPF = {
      case x: LineString => x
      case x: String => LineString.fromWellKnownText(x)
    }
  }

  private val DateRangeTypeTag = TypeTag.synchronized( {
    implicitly[TypeTag[DateRange]]
  })

  implicit object DateRangeConverter extends NullableTypeConverter[DateRange] {
    def targetTypeTag = DateRangeTypeTag
    override def convertPF = {
      case x: DateRange => x
      case x: String => DateRange.parse(x)
    }
  }

  class Tuple2Converter[K, V](implicit kc: TypeConverter[K], vc: TypeConverter[V])
    extends TypeConverter[(K, V)] {

    @transient
    lazy val targetTypeTag = {
      implicit val kTag = kc.targetTypeTag
      implicit val vTag = vc.targetTypeTag
      implicitly[TypeTag[(K, V)]]
    }

    def convertPF = {
      case TupleValue(k, v) => (kc.convert(k), vc.convert(v))
      case (k, v) => (kc.convert(k), vc.convert(v))
    }
  }

  class Tuple3Converter[C1 : TypeConverter, C2 : TypeConverter, C3 : TypeConverter]
    extends TypeConverter[(C1, C2, C3)] {

    private val c1 = implicitly[TypeConverter[C1]]
    private val c2 = implicitly[TypeConverter[C2]]
    private val c3 = implicitly[TypeConverter[C3]]

    @transient
    lazy val targetTypeTag = {
      implicit val tag1 = c1.targetTypeTag
      implicit val tag2 = c2.targetTypeTag
      implicit val tag3 = c3.targetTypeTag
      implicitly[TypeTag[(C1, C2, C3)]]
    }

    def convertPF = {
      case TupleValue(a1, a2, a3) => (c1.convert(a1), c2.convert(a2), c3.convert(a3))
      case (a1, a2, a3) => (c1.convert(a1), c2.convert(a2), c3.convert(a3))
    }
  }

  class PairConverter[K, V](implicit kc: TypeConverter[K], vc: TypeConverter[V])
    extends TypeConverter[tuple.Pair[K, V]] {

    @transient
    lazy val targetTypeTag = {
      implicit val kTag = kc.targetTypeTag
      implicit val vTag = vc.targetTypeTag
      implicitly[TypeTag[tuple.Pair[K, V]]]
    }

    def convertPF = {
      case TupleValue(k, v) => tuple.Pair.of(kc.convert(k), vc.convert(v))
      case (k, v) => tuple.Pair.of(kc.convert(k), vc.convert(v))
    }
  }

  class TripleConverter[C1 : TypeConverter, C2 : TypeConverter, C3 : TypeConverter]
    extends TypeConverter[tuple.Triple[C1, C2, C3]] {

    private val c1 = implicitly[TypeConverter[C1]]
    private val c2 = implicitly[TypeConverter[C2]]
    private val c3 = implicitly[TypeConverter[C3]]

    @transient
    lazy val targetTypeTag = {
      implicit val tag1 = c1.targetTypeTag
      implicit val tag2 = c2.targetTypeTag
      implicit val tag3 = c3.targetTypeTag
      implicitly[TypeTag[tuple.Triple[C1, C2, C3]]]
    }

    def convertPF = {
      case TupleValue(a1, a2, a3) => tuple.Triple.of(c1.convert(a1), c2.convert(a2), c3.convert(a3))
      case (a1, a2, a3) => tuple.Triple.of(c1.convert(a1), c2.convert(a2), c3.convert(a3))
    }
  }

  class OptionConverter[T](implicit c: TypeConverter[T]) extends TypeConverter[Option[T]] {

    @transient
    lazy val targetTypeTag = {
      implicit val itemTypeTag = c.targetTypeTag
      implicitly[TypeTag[Option[T]]]
    }

    def convertPF = {
      case null => None
      case None => None
      case other => Some(c.convert(other))
    }
  }

  class CassandraOptionConverter[T](implicit c: TypeConverter[T]) extends
    TypeConverter[CassandraOption[T]] {

    @transient
    lazy val targetTypeTag = TypeTag.synchronized {
      implicit val itemTypeTag = c.targetTypeTag
      implicitly[TypeTag[CassandraOption[T]]]
    }

    def convertPF = {
      case null => CassandraOption.Unset
      case other => CassandraOption.Value(c.convert(other))
    }
  }

  abstract class CollectionConverter[CC, T](implicit c: TypeConverter[T], bf: CanBuildFrom[T, CC])
    extends TypeConverter[CC] {

    protected implicit def itemTypeTag: TypeTag[T] = c.targetTypeTag

    private def newCollection(items: Iterable[Any]) = {
      val builder = bf()
      for (item <- items)
        builder += c.convert(item)
      builder.result()
    }

    def convertPF = {
      case null => bf.apply().result()
      case x: java.util.List[_] => newCollection(x.asScala)
      case x: java.util.Set[_] => newCollection(x.asScala)
      case x: java.util.Map[_, _] => newCollection(x.asScala)
      case x: CqlVector[_] => newCollection(x.asScala)
      case x: Iterable[_] => newCollection(x)
    }
  }

  abstract class AbstractMapConverter[CC, K, V](implicit kc: TypeConverter[K], vc: TypeConverter[V], bf: CanBuildFrom[(K, V), CC])
    extends CollectionConverter[CC, (K, V)] {

    protected implicit def keyTypeTag: TypeTag[K] = kc.targetTypeTag
    protected implicit def valueTypeTag: TypeTag[V] = vc.targetTypeTag
  }


  class ListConverter[T : TypeConverter] extends CollectionConverter[List[T], T] {
    @transient
    lazy val targetTypeTag = {
      implicitly[TypeTag[List[T]]]
    }
  }

  class VectorConverter[T : TypeConverter] extends CollectionConverter[Vector[T], T] {
    @transient
    lazy val targetTypeTag = {
      implicitly[TypeTag[Vector[T]]]
    }
  }

  class SetConverter[T : TypeConverter] extends CollectionConverter[Set[T], T] {
    @transient
    lazy val targetTypeTag = {
      implicitly[TypeTag[Set[T]]]
    }
  }

  class TreeSetConverter[T : TypeConverter : Ordering] extends CollectionConverter[TreeSet[T], T] {
    @transient
    lazy val targetTypeTag = {
      implicitly[TypeTag[TreeSet[T]]]
    }
  }

  class SeqConverter[T : TypeConverter] extends CollectionConverter[Seq[T], T] {
    @transient
    lazy val targetTypeTag = {
      implicitly[TypeTag[Seq[T]]]
    }
  }

  class IndexedSeqConverter[T : TypeConverter] extends CollectionConverter[IndexedSeq[T], T] {
    @transient
    lazy val targetTypeTag = {
      implicitly[TypeTag[IndexedSeq[T]]]
    }
  }

  class IterableConverter[T : TypeConverter] extends CollectionConverter[Iterable[T], T] {
    @transient
    lazy val targetTypeTag = {
      implicitly[TypeTag[Iterable[T]]]
    }
  }

  class JavaListConverter[T : TypeConverter] extends CollectionConverter[java.util.List[T], T] {
    @transient
    lazy val targetTypeTag = {
      implicitly[TypeTag[java.util.List[T]]]
    }
  }

  class CqlVectorConverter[T <: Number : TypeConverter](dimension: Int) extends TypeConverter[CqlVector[T]] {
    val elemConverter = implicitly[TypeConverter[T]]

    implicit def elemTypeTag: TypeTag[T] = elemConverter.targetTypeTag

    @transient
    lazy val targetTypeTag = {
      implicitly[TypeTag[CqlVector[T]]]
    }

    private def newCollection(items: Iterable[Any]): java.util.List[T] = {
      val buf = new java.util.ArrayList[T](dimension)
      for (item <- items) buf.add(elemConverter.convert(item))
      buf
    }

    def convertPF = {
      case x: CqlVector[_] => x.asInstanceOf[CqlVector[T]] // it is an optimization - should we skip converting the elements?
      case x: java.lang.Iterable[_] => CqlVector.newInstance[T](newCollection(x.asScala))
      case x: Iterable[_] => CqlVector.newInstance[T](newCollection(x))
    }
  }

  class JavaArrayListConverter[T : TypeConverter] extends CollectionConverter[java.util.ArrayList[T], T] {
    @transient
    lazy val targetTypeTag = {
      implicitly[TypeTag[java.util.ArrayList[T]]]
    }
  }

  class JavaSetConverter[T : TypeConverter] extends CollectionConverter[java.util.Set[T], T] {
    @transient
    lazy val targetTypeTag = {
      implicitly[TypeTag[java.util.Set[T]]]
    }
  }

  class JavaHashSetConverter[T : TypeConverter] extends CollectionConverter[java.util.HashSet[T], T] {
    @transient
    lazy val targetTypeTag = {
      implicitly[TypeTag[java.util.HashSet[T]]]
    }
  }

  class MapConverter[K : TypeConverter, V : TypeConverter] extends AbstractMapConverter[Map[K, V], K, V] {
    @transient
    lazy val targetTypeTag = {
      implicitly[TypeTag[Map[K, V]]]
    }
  }

  class TreeMapConverter[K : TypeConverter : Ordering, V : TypeConverter] extends AbstractMapConverter[TreeMap[K, V], K, V] {
    @transient
    lazy val targetTypeTag = {
      implicitly[TypeTag[TreeMap[K, V]]]
    }
  }

  class JavaMapConverter[K : TypeConverter, V : TypeConverter] extends AbstractMapConverter[java.util.Map[K, V], K, V] {
    @transient
    lazy val targetTypeTag = {
      implicitly[TypeTag[java.util.Map[K, V]]]
    }
  }

  class JavaHashMapConverter[K : TypeConverter, V : TypeConverter] extends AbstractMapConverter[java.util.HashMap[K, V], K, V] {
    @transient
    lazy val targetTypeTag = {
      implicitly[TypeTag[java.util.HashMap[K, V]]]
    }
  }

  implicit def cassandraOptionConverter[T: TypeConverter]: CassandraOptionConverter[T] =
    new CassandraOptionConverter[T]

  implicit def optionConverter[T : TypeConverter]: OptionConverter[T] =
    new OptionConverter[T]

  implicit def tuple2Converter[K : TypeConverter, V : TypeConverter]: Tuple2Converter[K, V] =
    new Tuple2Converter[K, V]

  implicit def tuple3Converter[A1 : TypeConverter, A2 : TypeConverter, A3 : TypeConverter]
    : Tuple3Converter[A1, A2, A3] = new Tuple3Converter[A1, A2, A3]

  implicit def pairConverter[K : TypeConverter, V : TypeConverter]: PairConverter[K, V] =
    new PairConverter[K, V]

  implicit def tripleConverter[A1 : TypeConverter, A2 : TypeConverter, A3 : TypeConverter]
    : TripleConverter[A1, A2, A3] = new TripleConverter[A1, A2, A3]

  implicit def listConverter[T : TypeConverter]: ListConverter[T] =
    new ListConverter[T]

  implicit def vectorConverter[T : TypeConverter]: VectorConverter[T] =
    new VectorConverter[T]

  implicit def setConverter[T : TypeConverter]: SetConverter[T] =
    new SetConverter[T]

  implicit def treeSetConverter[T : TypeConverter : Ordering]: TreeSetConverter[T] =
    new TreeSetConverter[T]

  implicit def seqConverter[T : TypeConverter]: SeqConverter[T] =
    new SeqConverter[T]

  implicit def indexedSeqConverter[T : TypeConverter]: IndexedSeqConverter[T] =
    new IndexedSeqConverter[T]

  implicit def iterableConverter[T : TypeConverter]: IterableConverter[T] =
    new IterableConverter[T]

  implicit def mapConverter[K : TypeConverter, V : TypeConverter]: MapConverter[K, V] =
    new MapConverter[K, V]

  implicit def treeMapConverter[K: TypeConverter : Ordering, V : TypeConverter]: TreeMapConverter[K, V] =
    new TreeMapConverter[K, V]

  // Support for Java collections:
  implicit def javaListConverter[T : TypeConverter]: JavaListConverter[T] =
    new JavaListConverter[T]

  implicit def javaArrayListConverter[T : TypeConverter]: JavaArrayListConverter[T] =
    new JavaArrayListConverter[T]

  implicit def cqlVectorConverter[T <: Number : TypeConverter](dimension: Int): CqlVectorConverter[T] =
    new CqlVectorConverter[T](dimension)

  implicit def javaSetConverter[T : TypeConverter]: JavaSetConverter[T] =
    new JavaSetConverter[T]

  implicit def javaHashSetConverter[T : TypeConverter]: JavaHashSetConverter[T] =
    new JavaHashSetConverter[T]

  implicit def javaMapConverter[K : TypeConverter, V : TypeConverter]: JavaMapConverter[K, V] =
    new JavaMapConverter[K, V]

  implicit def javaHashMapConverter[K : TypeConverter, V : TypeConverter]: JavaHashMapConverter[K, V] =
    new JavaHashMapConverter[K, V]

  /** Converts Scala Options to Java nullable references. Used when saving data to Cassandra. */
  class OptionToNullConverter(nestedConverter: TypeConverter[_]) extends NullableTypeConverter[AnyRef] {

    def targetTypeTag = implicitly[TypeTag[AnyRef]]

    def cassandraOptionToAnyRef(cassandraOption: CassandraOption[_]) = {
      cassandraOption match {
        case CassandraOption.Value(x) => nestedConverter.convert(x).asInstanceOf[AnyRef]
        case CassandraOption.Unset => Unset
        case CassandraOption.Null => null
      }
    }

    def convertPF = {
      case x: CassandraOption[_] => cassandraOptionToAnyRef(x)
      case Some(x) => nestedConverter.convert(x).asInstanceOf[AnyRef]
      case None => null
      case Unset => Unset
      case x => nestedConverter.convert(x).asInstanceOf[AnyRef]
    }
  }

  private def orderingFor(tpe: Type): Option[Ordering[_]] = {
    if      (tpe =:= typeOf[Boolean]) Some(implicitly[Ordering[Boolean]])
    else if (tpe =:= typeOf[Byte]) Some(implicitly[Ordering[Byte]])
    else if (tpe =:= typeOf[Short]) Some(implicitly[Ordering[Short]])
    else if (tpe =:= typeOf[Int]) Some(implicitly[Ordering[Int]])
    else if (tpe =:= typeOf[Long]) Some(implicitly[Ordering[Long]])
    else if (tpe =:= typeOf[Float]) Some(implicitly[Ordering[Float]])
    else if (tpe =:= typeOf[Double]) Some(implicitly[Ordering[Double]])
    else if (tpe =:= typeOf[String]) Some(implicitly[Ordering[String]])
    else if (tpe =:= typeOf[BigInt]) Some(implicitly[Ordering[BigInt]])
    else if (tpe =:= typeOf[BigDecimal]) Some(implicitly[Ordering[BigDecimal]])
    else if (tpe =:= typeOf[java.math.BigInteger]) Some(implicitly[Ordering[java.math.BigInteger]])
    else if (tpe =:= typeOf[java.math.BigDecimal]) Some(implicitly[Ordering[java.math.BigDecimal]])
    else if (tpe =:= typeOf[java.util.Date]) Some(implicitly[Ordering[java.util.Date]])
    else if (tpe =:= typeOf[java.sql.Date]) Some(Ordering.by((x: java.sql.Date) => x.getTime))
    else if (tpe =:= typeOf[UUID]) Some(implicitly[Ordering[UUID]])
    else None
  }

  private var converters = Seq[TypeConverter[_]](
    AnyConverter,
    AnyRefConverter,
    BooleanConverter,
    JavaBooleanConverter,
    ByteConverter,
    JavaByteConverter,
    ShortConverter,
    JavaShortConverter,
    IntConverter,
    JavaIntConverter,
    LongConverter,
    JavaLongConverter,
    FloatConverter,
    JavaFloatConverter,
    DoubleConverter,
    JavaDoubleConverter,
    StringConverter,
    BigIntConverter,
    BigDecimalConverter,
    JavaBigIntegerConverter,
    JavaBigDecimalConverter,
    DateConverter,
    SqlDateConverter,
    GregorianCalendarConverter,
    TimestampConverter,
    InetAddressConverter,
    UUIDConverter,
    ByteBufferConverter,
    ByteArrayConverter,
    UDTValueConverter,
    JavaLocalDateConverter,
    JavaLocalTimeConverter,
    JavaDurationConverter,
    DurationConverter,
    JavaInstantConverter,
    PointConverter,
    LineStringConverter,
    PolygonConverter,
    DateRangeConverter
  )

  private val originalConverters = converters.toSet

  private def forCollectionType(tpe: Type, moreConverters: Seq[TypeConverter[_]]): TypeConverter[_] = {
    tpe match {
      case TypeRef(_, symbol, List(arg)) =>
        val untypedItemConverter = forType(arg, moreConverters)
        type T = untypedItemConverter.targetType
        implicit val itemConverter = untypedItemConverter.asInstanceOf[TypeConverter[T]]
        implicit val ordering = orderingFor(arg).map(_.asInstanceOf[Ordering[T]]).orNull
        symbol match {
          case CassandraOptionSymbol => cassandraOptionConverter[T]
          case OptionSymbol => optionConverter[T]
          case ListSymbol => listConverter[T]
          case VectorSymbol => vectorConverter[T]
          case SetSymbol => setConverter[T]
          case TreeSetSymbol if ordering != null => treeSetConverter[T]
          case SeqSymbol => seqConverter[T]
          case IndexedSeqSymbol => indexedSeqConverter[T]
          case IterableSymbol => iterableConverter[T]
          case JavaListSymbol => javaListConverter[T]
          case JavaArrayListSymbol => javaArrayListConverter[T]
          case JavaSetSymbol => javaSetConverter[T]
          case JavaHashSetSymbol => javaHashSetConverter[T]
          case _ => throw new IllegalArgumentException(s"Unsupported type: $tpe")
        }

      case TypeRef(_, symbol, List(k, v)) =>
        val untypedKeyConverter = forType(k, moreConverters)
        val untypedValueConverter = forType(v, moreConverters)
        type K = untypedKeyConverter.targetType
        type V = untypedValueConverter.targetType
        implicit val keyConverter = untypedKeyConverter.asInstanceOf[TypeConverter[K]]
        implicit val valueConverter = untypedValueConverter.asInstanceOf[TypeConverter[V]]
        implicit val ordering = orderingFor(k).map(_.asInstanceOf[Ordering[K]]).orNull
        symbol match {
          case PairSymbol => pairConverter[K, V]
          case MapSymbol => mapConverter[K, V]
          case TreeMapSymbol if ordering != null => treeMapConverter[K, V]
          case JavaMapSymbol => javaMapConverter[K, V]
          case JavaHashMapSymbol => javaHashMapConverter[K, V]
          case _ => throw new IllegalArgumentException(s"Unsupported type: $tpe")
        }

      case TypeRef(_, symbol, List(t1, t2, t3)) =>
        val untypedConverter1 = forType(t1, moreConverters)
        val untypedConverter2 = forType(t2, moreConverters)
        val untypedConverter3 = forType(t3, moreConverters)
        type T1 = untypedConverter1.targetType
        type T2 = untypedConverter2.targetType
        type T3 = untypedConverter3.targetType
        implicit val converter1 = untypedConverter1.asInstanceOf[TypeConverter[T1]]
        implicit val converter2 = untypedConverter2.asInstanceOf[TypeConverter[T2]]
        implicit val converter3 = untypedConverter3.asInstanceOf[TypeConverter[T3]]
        symbol match {
          case TripleSymbol => tripleConverter[T1, T2, T3]
          case _ => throw new IllegalArgumentException(s"Unsupported type: $tpe")
        }

      case _ => throw new IllegalArgumentException(s"Unsupported type: $tpe")
    }
  }

  /** Useful for getting converter based on a type received from Scala reflection.
    * Synchronized to workaround Scala 2.10 reflection thread-safety problems. */
  def forType(tpe: Type, moreConverters: Seq[TypeConverter[_]] = Seq.empty): TypeConverter[_] = {
    {
      type T = TypeConverter[_]
      val selectedConverters =
        (converters ++ moreConverters).collect { case c: T if c.targetTypeTag.tpe =:= tpe => c }

      selectedConverters match {
        case Seq() => forCollectionType(tpe, moreConverters)
        case Seq(c) => c
        case Seq(cs @ _*) => new ChainedTypeConverter(cs: _*)
      }
    }
  }

  /** Useful when implicit converters are not in scope, but a TypeTag is.
    * Synchronized to workaround Scala 2.10 reflection thread-safety problems. */
  def forType[T : TypeTag](moreConverters: Seq[TypeConverter[_]]): TypeConverter[T] = {
    {
      forType(implicitly[TypeTag[T]].tpe, moreConverters).asInstanceOf[TypeConverter[T]]
    }
  }

  /** Useful when implicit converters are not in scope, but a TypeTag is.
    * Synchronized to workaround Scala 2.10 reflection thread-safety problems. */
  def forType[T : TypeTag]: TypeConverter[T] = {
    {
      forType(implicitly[TypeTag[T]].tpe).asInstanceOf[TypeConverter[T]]
    }
  }

  def forType(cl: Class[_]): TypeConverter[_] = {
    // scala reflection incorrectly returns Array[T] for byte[]. Force proper behaviour
    if (cl == classOf[Array[Byte]]) forType(ByteArrayTypeTag)
    else forType(runtimeMirror(cl.getClassLoader).classSymbol(cl).toType)
  }

  /** Registers a custom converter */
  def registerConverter(c: TypeConverter[_]) {
    synchronized {
      converters = c +: converters
    }
  }

  /** Remove a custom converter */
  def unregisterConverter(c: TypeConverter[_]) {
    synchronized {
      require(!originalConverters.contains(c), "Cannot unregister built-in converter")
      converters = converters.filterNot(_ == c)
    }
  }
}
