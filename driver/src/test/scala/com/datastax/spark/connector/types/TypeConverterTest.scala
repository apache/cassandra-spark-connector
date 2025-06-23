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
import java.text.SimpleDateFormat
import java.time.{LocalDate, LocalTime, ZoneId, ZoneOffset}
import java.util.{Date, GregorianCalendar, UUID}

import com.datastax.oss.driver.api.core.data.CqlDuration

import scala.collection.immutable.{TreeMap, TreeSet}
import scala.reflect.runtime.universe._
import org.apache.commons.lang3.SerializationUtils
import org.junit.Assert._
import org.junit.Test

class TypeConverterTest {

  final val DefaultHost = "127.0.0.1"

  @Test
  def testBoolean() {
    val c = TypeConverter.forType[Boolean]
    assertEquals(true, c.convert("true"))
    assertEquals(false, c.convert("false"))
    assertEquals(true, c.convert(1))
    assertEquals(false, c.convert(0))
  }

  @Test
  def testJavaBoolean() {
    val c = TypeConverter.forType[java.lang.Boolean]
    assertEquals(true, c.convert("true"))
    assertEquals(false, c.convert("false"))
    assertEquals(true, c.convert(1))
    assertEquals(false, c.convert(0))
  }

  @Test
  def testInt() {
    val c = TypeConverter.forType[Int]
    assertEquals(12345, c.convert("12345"))
    assertEquals(12345, c.convert(12345))
  }

  @Test
  def testJavaInteger() {
    val c = TypeConverter.forType[java.lang.Integer]
    assertEquals(12345, c.convert("12345"))
    assertEquals(12345, c.convert(12345))
  }

  @Test
  def testLong() {
    val c = TypeConverter.forType[Long]
    val instant = java.time.Instant.ofEpochMilli(12345L)
    assertEquals(12345L, c.convert("12345"))
    assertEquals(12345L, c.convert(12345))
    assertEquals(12345L, c.convert(12345L))
    assertEquals(12345L, c.convert(instant))
    assertEquals(12345L, c.convert(Date.from(instant)))
    assertEquals(12345L, c.convert(java.time.LocalTime.ofNanoOfDay(12345L)))
    assertEquals(12345L, c.convert(java.time.LocalDate.ofEpochDay(12345L)))
  }

  @Test
  def testJavaLong() {
    val c = TypeConverter.forType[java.lang.Long]
    assertEquals(12345L, c.convert("12345"))
    assertEquals(12345L, c.convert(12345))
    assertEquals(12345L, c.convert(12345L))
  }

  @Test
  def testFloat() {
    val c = TypeConverter.forType[Float]
    assertEquals(1.0f, c.convert("1.0"), 0.0001f)
    assertEquals(1.0f, c.convert(1.0f), 0.0001f)
  }

  @Test
  def testJavaFloat() {
    val c = TypeConverter.forType[java.lang.Float]
    assertEquals(1.0f, c.convert("1.0").toFloat, 0.0001f)
    assertEquals(1.0f, c.convert(1.0f).toFloat, 0.0001f)
  }

  @Test
  def testDouble() {
    val c = TypeConverter.forType[Double]
    assertEquals(1.0, c.convert("1.0"), 0.0001)
    assertEquals(1.0, c.convert(1.0), 0.0001)
  }

  @Test
  def testJavaDouble() {
    val c = TypeConverter.forType[java.lang.Double]
    assertEquals(1.0, c.convert("1.0"), 0.0001)
    assertEquals(1.0, c.convert(1.0), 0.0001)
  }

  @Test
  def testBigInt() {
    val c = TypeConverter.forType[BigInt]
    assertEquals(BigInt(12345), c.convert(12345))
    assertEquals(BigInt("123456789123456789123456789"), c.convert("123456789123456789123456789"))
  }

  @Test
  def testJavaBigInteger() {
    val c = TypeConverter.forType[BigInteger]
    assertEquals(new BigInteger("12345"), c.convert(12345))
    assertEquals(new BigInteger("123456789123456789123456789"), c.convert("123456789123456789123456789"))
  }

  @Test
  def testBigDecimal() {
    val c = TypeConverter.forType[BigDecimal]
    assertEquals(BigDecimal(12345.25), c.convert(12345.25))
    assertEquals(BigDecimal("123456789123456789.123456789"), c.convert("123456789123456789.123456789"))
  }

  @Test
  def testJavaBigDecimal() {
    val c = TypeConverter.forType[java.math.BigDecimal]
    assertEquals(new java.math.BigDecimal("12345.25"), c.convert(12345.25))
    assertEquals(new java.math.BigDecimal("123456789123456789.123456789"), c.convert("123456789123456789.123456789"))
  }

  @Test
  def testString() {
    val c = TypeConverter.forType[String]
    assertEquals("a string", c.convert("a string"))
  }

  @Test
  def testByteArrayToString() {
    val c = TypeConverter.forType[String]
    val byteArray : Array[Byte] = Array(1,3,-56,45,23,67)
    assertEquals("0x0103c82d1743", c.convert(byteArray))
  }

  @Test
  def testDate() {
    val c = TypeConverter.forType[Date]
    val dateStr = "2014-04-23 11:21:32+0100"
    val dayOnlyStr = "2014-04-23"
    val localDate = LocalDate.of(2014, 4, 23)
    val javaLocalDate = java.time.LocalDate.of(2014, 4, 23)

    val date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ").parse(dateStr)
    val dateDayOnly = new SimpleDateFormat("yyyy-MM-dd").parse(dayOnlyStr)

    assertEquals(dateDayOnly, c.convert(localDate))
    assertEquals(dateDayOnly, c.convert(javaLocalDate))
    assertEquals(dateDayOnly, c.convert(dayOnlyStr))
    assertEquals(date, c.convert(dateStr))

    val date1 = LocalDate.of(1986, 1, 1)
    assertEquals(Date.from(date1.atStartOfDay(ZoneId.systemDefault()).toInstant), c.convert("1986"))
    assertEquals(Date.from(date1.atStartOfDay().toInstant(ZoneOffset.UTC)), c.convert("1986Z"))
  }

  @Test
  def testTimestamp() {
    val c = TypeConverter.forType[Timestamp]
    val dateStr = "2014-04-23 11:21:32+0100"
    val dayOnlyStr = "2014-04-23"
    val localDate = LocalDate.of(2014, 4, 23)
    val javaLocalDate = java.time.LocalDate.of(2014, 4, 23)
    val date = Timestamp.from(new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ").parse(dateStr).toInstant)
    val dateDayOnly = Timestamp.from(new SimpleDateFormat("yyyy-MM-dd").parse(dayOnlyStr).toInstant)

    assertEquals(dateDayOnly, c.convert(localDate))
    assertEquals(dateDayOnly, c.convert(javaLocalDate))
    assertEquals(date, c.convert(dateStr))
  }

  @Test
  def testParsableDate() {
    val c = TypeConverter.forType[Date]

    val validDates = List(
      "1986-01-02 21:05",
      "1986-01-02 21:05+1000",
      "1986-01-02 21:05+10:00",
      "1986-01-02 21:05Z",
      "1986-01-02 21:05:07",
      "1986-01-02 21:05:07+1000",
      "1986-01-02 21:05:07+10:00",
      "1986-01-02 21:05:07Z",
      "1986-01-02 21:05:07.123",
      "1986-01-02 21:05:07.123+1000",
      "1986-01-02 21:05:07.123+10:00",
      "1986-01-02 21:05:07.123Z",
      "1986-01-02T21:05",
      "1986-01-02T21:05+1000",
      "1986-01-02T21:05+10:00",
      "1986-01-02T21:05Z",
      "1986-01-02T21:05:07",
      "1986-01-02T21:05:07+1000",
      "1986-01-02T21:05:07+10:00",
      "1986-01-02T21:05Z",
      "1986-01-02T21:05:07.123",
      "1986-01-02T21:05:07.123+1000",
      "1986-01-02T21:05:07.123Z",
      "1986-01-02",
      "1986-01-02+1000",
      "1986-01-02Z",
      "1986")

    validDates.foreach(c.convert)
  }


  @Test
  def testSqlDate(): Unit = {
    val c = TypeConverter.forType[java.sql.Date]

    val targetDate = java.sql.Date.valueOf("2014-04-23")

    val localDate = LocalDate.of(2014,4,23)
    val javaLocalDate = java.time.LocalDate.of(2014, 4, 23)

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val utilDate = dateFormat.parse("2014-04-23")

    assertEquals(targetDate, c.convert("2014-04-23"))
    assertEquals(targetDate, c.convert(localDate))
    assertEquals(targetDate, c.convert(javaLocalDate))
    assertEquals(targetDate, c.convert(utilDate))

    val targetYear = java.sql.Date.valueOf("2014-01-01")
    assertEquals(targetYear, c.convert("2014"))

  }

  @Test
  def testCalendar1() {
    val c = TypeConverter.forType[GregorianCalendar]
    val dateStr = "2014-04-23 11:21:32+0100"
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ")
    val date = new GregorianCalendar()
    date.setTime(dateFormat.parse(dateStr))
    assertEquals(date, c.convert(dateStr))
  }

  @Test
  def testCalendar2() {
    val c = TypeConverter.forType[Date]
    val dateStr = "2014-04-23 11:21:32+0100"
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ")
    val calendar = new GregorianCalendar()
    calendar.setTime(dateFormat.parse(dateStr))
    assertEquals(calendar.getTime, c.convert(calendar))
  }

  @Test
  def testInetAddress() {
    val c = TypeConverter.forType[InetAddress]
    assertEquals(InetAddress.getByName(DefaultHost), c.convert(DefaultHost))
  }

  @Test
  def testUUID() {
    val c = TypeConverter.forType[UUID]
    val uuidStr = "550e8400-e29b-41d4-a716-446655440000"
    assertEquals(UUID.fromString(uuidStr), c.convert(uuidStr))
  }

  @Test
  def testByteArray() {
    val c = TypeConverter.forType[Array[Byte]]
    val array = Array[Byte](1, 2, 3, 4)
    val buf = ByteBuffer.allocate(4)
    buf.put(array)
    buf.rewind()
    assertSame(array, c.convert(array))
    assertEquals(array.toList, c.convert(buf).toList)
  }

  @Test
  def testLocalDate(): Unit = {
    val c = TypeConverter.forType[LocalDate]
    val testDate = LocalDate.of(1985, 8, 3)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val date = dateFormat.parse("1985-08-03")
    assertEquals(testDate, c.convert("1985-08-03"))
    assertEquals(testDate, c.convert(5693))
    assertEquals(testDate, c.convert(date))
    assertEquals(testDate, c.convert(java.sql.Date.valueOf("1985-08-03")))
    assertEquals(testDate, c.convert(java.time.LocalDate.of(1985, 8, 3)))
  }

  @Test
  def testYearLocalDate(): Unit = {
    val c = TypeConverter.forType[LocalDate]
    val testDate = LocalDate.of(1985, 1, 1)
    assertEquals(testDate, c.convert("1985"))
    assertEquals(testDate, c.convert("1985Z"))
  }

  @Test
  def testLocalTimeType(): Unit = {
    val longC = TypeConverter.LongConverter
    val targetTime = 1482000000L
    val localTime = LocalTime.ofNanoOfDay(targetTime)
    assertEquals(targetTime, longC.convert(targetTime))

    val timeC = TypeConverter.JavaLocalTimeConverter
    assertEquals(localTime, timeC.convert(localTime))
    assertEquals(localTime, timeC.convert(targetTime))
    assertEquals(localTime, timeC.convert("00:00:01.482"))
  }

  @Test
  def testJavaLocalDate(): Unit = {
    val c = TypeConverter.forType(classOf[java.time.LocalDate])
    val testDate = java.time.LocalDate.of(1985, 8, 3)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val date = dateFormat.parse("1985-08-03")
    assertEquals(testDate, c.convert("1985-08-03"))
    assertEquals(testDate, c.convert(5693))
    assertEquals(testDate, c.convert(date))
    assertEquals(testDate, c.convert(java.sql.Date.valueOf("1985-08-03")))
  }

  @Test
  def testJavaLocalTime(): Unit = {
    val c = TypeConverter.forType(classOf[java.time.LocalTime])
    val testTime = java.time.LocalTime.parse("10:15:30")
    assertEquals(testTime, c.convert(testTime.toNanoOfDay))
    assertEquals(testTime, c.convert("10:15:30"))
  }

  @Test
  def testJavaDurationTypeTag(): Unit = {
    val c = TypeConverter.forType(classOf[java.time.Duration])
    val testDuration = java.time.Duration.parse("PT10S")
    assertEquals(testDuration, c.convert(10000))
    assertEquals(testDuration, c.convert("PT10S"))
  }

  @Test
  def testJavaInstantTypeTag(): Unit = {
    val c = TypeConverter.forType(classOf[java.time.Instant])
    val testInstant = java.time.Instant.parse("2007-12-03T10:15:30.00Z")
    val ms = testInstant.toEpochMilli
    assertEquals(testInstant, c.convert(ms))
    assertEquals(testInstant, c.convert(new Timestamp(ms)))
    assertEquals(testInstant, c.convert("2007-12-03T10:15:30.00Z"))

    // for CEST (+0100 at the moment of writing) this gives UTC 2019-08-13 15:15:58.0
    val javaDateInLocalTZ = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").parse("2019-08-13 16:15:58.0")
    assertEquals(javaDateInLocalTZ.toInstant, c.convert("2019-08-13 16:15:58.0"))

    assertEquals(java.time.Instant.parse("1986-01-02T18:05:07.123Z"), c.convert("1986-01-02 21:05:07.123+0300"))
  }

  @Test
  def testOption() {
    val c = TypeConverter.forType[Option[String]]
    assertEquals(None, c.convert(null))
    assertEquals(None, c.convert(None))
    assertEquals(Some("not-null"), c.convert("not-null"))
  }

  @Test
  def testCassandraOption() {
    val c = TypeConverter.forType[CassandraOption[String]]
    val unset = CassandraOption.Unset
    val set = CassandraOption.Value("not-null")
    assertEquals(unset, c.convert(null))
    assertEquals(set, c.convert("not-null"))
  }

  @Test
  def testList() {
    val c = TypeConverter.forType[Vector[Option[Int]]]
    val arrayList = new java.util.ArrayList[String]()
    arrayList.add("1")
    arrayList.add("2")
    arrayList.add(null)
    assertEquals(Vector(Some(1), Some(2), None), c.convert(arrayList))
  }

  @Test
  def testSet() {
    val c = TypeConverter.forType[Set[Int]]
    val arrayList = new java.util.ArrayList[String]()
    arrayList.add("1")
    arrayList.add("2")
    assertEquals(Set(1, 2), c.convert(arrayList))
  }

  @Test
  def testTreeSet() {
    val c = TypeConverter.forType[TreeSet[Int]]
    val arrayList = new java.util.ArrayList[String]()
    arrayList.add("2")
    arrayList.add("1")
    arrayList.add("4")
    arrayList.add("3")
    assertEquals(TreeSet(1, 2, 3, 4), c.convert(arrayList))
  }

  @Test
  def testMap() {
    val c = TypeConverter.forType[Map[Int, Option[String]]]
    val map = new java.util.HashMap[String, String]()
    map.put("1", "a")
    map.put("2", "b")
    map.put("3", null)
    assertEquals(Map(1 -> Some("a"), 2 -> Some("b"), 3 -> None), c.convert(map))
  }

  @Test
  def testTreeMap() {
    val c = TypeConverter.forType[TreeMap[Int, Option[String]]]
    val map = new java.util.HashMap[String, String]()
    map.put("1", "a")
    map.put("2", "b")
    map.put("3", null)
    assertEquals(TreeMap(1 -> Some("a"), 2 -> Some("b"), 3 -> None), c.convert(map))
  }

  private def abstractTestJavaList(c: TypeConverter[_]) {
    val arrayList = new java.util.ArrayList[String]()
    arrayList.add("1")
    arrayList.add("2")
    val outList = new java.util.ArrayList[Int]()
    outList.add(1)
    outList.add(2)
    assertEquals(outList, c.convert(arrayList))
  }

  @Test
  def testJavaList() {
    abstractTestJavaList(TypeConverter.forType[java.util.List[Int]])
  }

  @Test
  def testJavaArrayList() {
    abstractTestJavaList(TypeConverter.forType[java.util.ArrayList[Int]])
  }

  private def abstractTestJavaSet(c: TypeConverter[_]) {
    val c = TypeConverter.forType[java.util.HashSet[Int]]
    val arrayList = new java.util.ArrayList[String]()
    arrayList.add("1")
    arrayList.add("2")
    val outSet = new java.util.HashSet[Int]()
    outSet.add(1)
    outSet.add(2)
    assertEquals(outSet, c.convert(arrayList))
  }

  @Test
  def testJavaSet() {
    abstractTestJavaSet(TypeConverter.forType[java.util.Set[Int]])
  }

  @Test
  def testJavaHashSet() {
    abstractTestJavaSet(TypeConverter.forType[java.util.HashSet[Int]])
  }

  private def abstractTestJavaMap(c: TypeConverter[_]) {
    val map = new java.util.HashMap[String, String]()
    map.put("1", "a")
    map.put("2", "b")
    map.put("3", null)
    val outMap = new java.util.HashMap[Int, Option[String]]()
    outMap.put(1, Some("a"))
    outMap.put(2, Some("b"))
    outMap.put(3, None)

    assertEquals(outMap, c.convert(map))
  }

  @Test
  def testJavaMap() {
    abstractTestJavaMap(TypeConverter.forType[java.util.Map[Int, Option[String]]])
  }

  @Test
  def testJavaHashMap() {
    abstractTestJavaMap(TypeConverter.forType[java.util.HashMap[Int, Option[String]]])
  }

  @Test
  def testPair(): Unit = {
    val scalaPair = (1, 2)
    val converter = TypeConverter.forType[org.apache.commons.lang3.tuple.Pair[Int, String]]
    val commonsPair = converter.convert(scalaPair)
    assertEquals(1, commonsPair.getLeft)
    assertEquals("2", commonsPair.getRight)
  }

  @Test
  def testTriple(): Unit = {
    val scalaTriple = (1, 2, 3)
    val converter = TypeConverter.forType[org.apache.commons.lang3.tuple.Triple[Int, String, Long]]
    val commonsTriple = converter.convert(scalaTriple)
    assertEquals(1, commonsTriple.getLeft)
    assertEquals("2", commonsTriple.getMiddle)
    assertEquals(3L, commonsTriple.getRight)
  }

  @Test
  def testOptionToNullConverter() {
    val c = new TypeConverter.OptionToNullConverter(TypeConverter.IntConverter)
    assertEquals(1.asInstanceOf[AnyRef], c.convert(Some(1)))
    assertEquals(1.asInstanceOf[AnyRef], c.convert(1))
    assertEquals(1.asInstanceOf[AnyRef], c.convert(Some("1")))
    assertEquals(1.asInstanceOf[AnyRef], c.convert("1"))
    assertEquals(null, c.convert(None))
    assertEquals(null, c.convert(null))
  }

  @Test
  def testCassandraOptionToNull() {
    val c = new TypeConverter.OptionToNullConverter(TypeConverter.IntConverter)
    assertEquals(Unset, c.convert(CassandraOption.Unset))
    assertEquals(null, c.convert(CassandraOption.Null))
    assertEquals(1.asInstanceOf[AnyRef], c.convert(CassandraOption.Value(1)))
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testUnsupportedType() {
    TypeConverter.forType[TypeConverterTest]
  }

  @Test
  def testSerializeCollectionConverter() {
    val c1 = TypeConverter.forType[Vector[Int]]
    val c2 = SerializationUtils.roundtrip(c1)

    val arrayList = new java.util.ArrayList[String]()
    arrayList.add("1")
    arrayList.add("2")

    val out = c2.convert(arrayList)
    assertEquals(Vector(1, 2), out)
    assertNotNull(c2.targetTypeTag)
    assertEquals("Vector[Int]", c2.targetTypeName)
  }

  @Test
  def testSerializeMapConverter() {
    val c1 = TypeConverter.forType[Map[Int, Int]]
    val c2 = SerializationUtils.roundtrip(c1)

    val hashMap = new java.util.HashMap[String, String]()
    hashMap.put("1", "10")
    hashMap.put("2", "20")

    val out = c2.convert(hashMap)
    assertEquals(Map(1 -> 10, 2 -> 20), out)
    assertNotNull(c2.targetTypeTag)
    assertEquals("Map[Int,Int]", c2.targetTypeName)
  }

  type StringAlias = String

  @Test
  def testTypeAliases() {
    assertNotNull(TypeConverter.forType[StringAlias])
    assertNotNull(TypeConverter.forType[java.lang.String])
    assertNotNull(TypeConverter.forType[scala.Predef.String])
    assertNotNull(TypeConverter.forType[List[StringAlias]])
    assertNotNull(TypeConverter.forType[List[java.lang.String]])
    assertNotNull(TypeConverter.forType[TreeSet[StringAlias]])
    assertNotNull(TypeConverter.forType[TreeSet[java.lang.String]])
    assertNotNull(TypeConverter.forType[Map[StringAlias, StringAlias]])
    assertNotNull(TypeConverter.forType[TreeMap[StringAlias, StringAlias]])
  }

  @Test
  def testChainedConverters() {
    val standardConverter = TypeConverter.forType[Int]
    val extendedConverter = new TypeConverter[Int] {
      def targetTypeTag = typeTag[Int]
      def convertPF = {
        case Some(x: Int) => x
        case None => 0
      }
    }

    val chainedConverter = new ChainedTypeConverter(standardConverter, extendedConverter)
    assertEquals(1, chainedConverter.convert(1))
    assertEquals(2, chainedConverter.convert("2"))
    assertEquals(3, chainedConverter.convert(Some(3)))
    assertEquals(0, chainedConverter.convert(None))
  }

  case class EMail(email: String)

  @Test
  def testRegisterCustomConverter() {
    val converter = new TypeConverter[EMail] {
      def targetTypeTag = typeTag[EMail]

      def convertPF = { case x: String => EMail(x) }
    }
    TypeConverter.registerConverter(converter)
    try {
      assertSame(converter, TypeConverter.forType[EMail])
    } finally {
      TypeConverter.unregisterConverter(converter)
    }
  }

  @Test
  def testRegisterCustomConverterExtension() {
    val converter = new TypeConverter[Int] {
      def targetTypeTag = typeTag[Int]

      def convertPF = {
        case Some(x: Int) => x
        case None => 0
      }
    }
    TypeConverter.registerConverter(converter)
    try {
      val chainedConverter = TypeConverter.forType[Int]
      assertTrue(chainedConverter.isInstanceOf[ChainedTypeConverter[_]])
      assertEquals(1, chainedConverter.convert(1))
      assertEquals(2, chainedConverter.convert("2"))
      assertEquals(3, chainedConverter.convert(Some(3)))
      assertEquals(0, chainedConverter.convert(None))
    } finally {
      TypeConverter.unregisterConverter(converter)
    }
  }

  @Test
  def testChainedConverterSerializability() {
    val chainedConverter = new ChainedTypeConverter(TypeConverter.forType[Int])
    val chainedConverter2 = SerializationUtils.roundtrip(chainedConverter)
    assertEquals(1, chainedConverter2.convert(1))
    assertEquals(2, chainedConverter2.convert("2"))
  }

  @Test
  def testJavaMapping(): Unit = {
    assertEquals(TypeConverter.JavaBooleanConverter, TypeConverter.forType(classOf[java.lang.Boolean]))
    assertEquals(TypeConverter.JavaShortConverter, TypeConverter.forType(classOf[java.lang.Short]))
    assertEquals(TypeConverter.JavaIntConverter, TypeConverter.forType(classOf[java.lang.Integer]))
    assertEquals(TypeConverter.JavaLongConverter, TypeConverter.forType(classOf[java.lang.Long]))
    assertEquals(TypeConverter.JavaFloatConverter, TypeConverter.forType(classOf[java.lang.Float]))
    assertEquals(TypeConverter.JavaDoubleConverter, TypeConverter.forType(classOf[java.lang.Double]))
    assertEquals(TypeConverter.JavaBigDecimalConverter, TypeConverter.forType(classOf[java.math.BigDecimal]))
    assertEquals(TypeConverter.JavaBigIntegerConverter, TypeConverter.forType(classOf[java.math.BigInteger]))
    assertEquals(TypeConverter.JavaLocalDateConverter, TypeConverter.forType(classOf[java.time.LocalDate]))
    assertEquals(TypeConverter.JavaLocalTimeConverter, TypeConverter.forType(classOf[java.time.LocalTime]))
    assertEquals(TypeConverter.JavaDurationConverter, TypeConverter.forType(classOf[java.time.Duration]))
    assertEquals(TypeConverter.DurationConverter, TypeConverter.forType(classOf[CqlDuration]))
    assertEquals(TypeConverter.JavaInstantConverter, TypeConverter.forType(classOf[java.time.Instant]))
    assertEquals(TypeConverter.StringConverter, TypeConverter.forType(classOf[java.lang.String]))
    assertEquals(TypeConverter.UUIDConverter, TypeConverter.forType(classOf[java.util.UUID]))
    assertEquals(TypeConverter.InetAddressConverter, TypeConverter.forType(classOf[java.net.InetAddress]))
    assertEquals(TypeConverter.ByteArrayConverter, TypeConverter.forType(classOf[Array[Byte]]))
  }
}
