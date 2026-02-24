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

package com.datastax.spark.connector.datasource

import java.util.Locale
import com.datastax.oss.driver.api.core.ProtocolVersion
import com.datastax.oss.driver.api.core.`type`.{DataType, DataTypes, ListType, MapType, SetType, TupleType, UserDefinedType, VectorType}
import com.datastax.oss.driver.api.core.`type`.DataTypes._
import com.datastax.dse.driver.api.core.`type`.DseDataTypes._
import com.datastax.oss.driver.api.core.metadata.schema.{ColumnMetadata, RelationMetadata, TableMetadata}
import com.datastax.spark.connector.cql.CassandraConnectionFactory
import com.datastax.spark.connector.util.{ConfigParameter, DeprecatedConfigParameter, Logging}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{DataType => CatalystType}
import org.apache.spark.sql.types.{BooleanType => SparkSqlBooleanType, DataType => SparkSqlDataType, DateType => SparkSqlDateType, DecimalType => SparkSqlDecimalType, DoubleType => SparkSqlDoubleType, FloatType => SparkSqlFloatType, MapType => SparkSqlMapType, TimestampType => SparkSqlTimestampType, UserDefinedType => SparkSqlUserDefinedType, _}

import scala.jdk.CollectionConverters._
import scala.language.postfixOps
import scala.util.Try

object CassandraSourceUtil extends Logging {


  private val BracketList = "\\[(.*)\\]".r
  private val BracketMap = "\\{(.*)\\}".r

  private val primitiveTypeMap = Map[DataType, CatalystType](
    ASCII -> StringType,
    BIGINT -> LongType,
    BLOB -> BinaryType,
    BOOLEAN -> SparkSqlBooleanType,
    COUNTER -> LongType,
    DATE -> SparkSqlDateType,
    DATE_RANGE -> StringType,
    DECIMAL -> SparkSqlDecimalType(38, 18),
    DOUBLE -> SparkSqlDoubleType,
    DURATION -> StringType,
    FLOAT -> SparkSqlFloatType,
    INET -> StringType,
    INT -> IntegerType,
    LINE_STRING -> StringType,
    POLYGON -> StringType,
    POINT -> StringType,
    SMALLINT -> ShortType,
    TEXT -> StringType,
    TINYINT -> ByteType,
    TIME -> LongType,
    TIMESTAMP -> SparkSqlTimestampType,
    TIMEUUID -> StringType,
    UUID -> StringType,
    VARINT -> SparkSqlDecimalType(38, 0)
  )

  /**
    * Consolidate Cassandra conf settings in the order of
    * table level -> keyspace level -> cluster level ->
    * default. Use the first available setting. Default
    * settings are stored in SparkConf.
    *
    * User options are checked first
    * Then the Spark Session Conf
    * Then finally the SparkConf
    */
  def consolidateConfs(
    sparkConf: SparkConf,
    sqlConf: Map[String, String],
    cluster: String = "default",
    keyspace: String = "",
    userOptions: Map[String, String] = Map.empty): SparkConf = {

    //Default settings
    val conf = sparkConf.clone()

    // Normalize userOptions keys to lowercase for case-insensitive lookup.
    // In Spark 4.x, CaseInsensitiveStringMap.asScala.toMap produces a case-sensitive map
    // with original-case keys, so we must normalize here.
    val lowerCaseUserOptions = userOptions.map { case (k, v) => (k.toLowerCase(Locale.ROOT), v) }

    def consolidate(prop: String): Option[String] = {
      Seq(
        lowerCaseUserOptions.get(prop.toLowerCase(Locale.ROOT)),
        sqlConf.get(s"$cluster:$keyspace/$prop"),
        sqlConf.get(s"$cluster/$prop"),
        sqlConf.get(s"default/$prop"),
        sqlConf.get(prop)).flatten.headOption
    }

    // Custom connection factories may have a set of custom supported properties
    val factoryName = consolidate(CassandraConnectionFactory.FactoryParam.name)
    val customConnectionFactoryProperties =
      CassandraConnectionFactory.fromNameOrDefault(factoryName).properties

    val AllSCCConfNames = ConfigParameter.names ++ DeprecatedConfigParameter.names ++
      customConnectionFactoryProperties

    val SqlPropertyKeys = AllSCCConfNames.flatMap(prop => Seq(
      s"$cluster:$keyspace/$prop",
      s"$cluster/$prop",
      s"default/$prop",
      prop))

    //Keyspace/Cluster level settings
    for (prop <- AllSCCConfNames) {
      val value = consolidate(prop)
      value.foreach(conf.set(prop, _))
    }
    conf.setAll(sqlConf -- SqlPropertyKeys)
    //Set all user properties while avoiding SCC Properties
    conf.setAll(lowerCaseUserOptions -- AllSCCConfNames.map(_.toLowerCase(Locale.ROOT)))
    conf
  }

  def sparkSqlToJavaDriverType(
    dataType: CatalystType,
    protocolVersion: ProtocolVersion = ProtocolVersion.DEFAULT): DataType = {

    def unsupportedType() = throw new IllegalArgumentException(s"Unsupported type: $dataType")

    val pvGt4 = (protocolVersion.getCode >= ProtocolVersion.V4.getCode)

    dataType match {
      case ByteType => if (pvGt4) TINYINT else INT
      case ShortType => if (pvGt4) SMALLINT else INT
      case IntegerType => INT
      case LongType => BIGINT
      case SparkSqlFloatType => FLOAT
      case SparkSqlDoubleType => DOUBLE
      case StringType => TEXT
      case BinaryType => BLOB
      case SparkSqlBooleanType => BOOLEAN
      case SparkSqlTimestampType => TIMESTAMP
      case SparkSqlDateType => if (pvGt4) DATE else TIMESTAMP
      case SparkSqlDecimalType() => DECIMAL
      case ArrayType(sparkSqlElementType, containsNull) =>
        val argType = sparkSqlToJavaDriverType(sparkSqlElementType)
        DataTypes.listOf(argType)
      case SparkSqlMapType(sparkSqlKeyType, sparkSqlValueType, containsNull) =>
        val keyType = sparkSqlToJavaDriverType(sparkSqlKeyType)
        val valueType = sparkSqlToJavaDriverType(sparkSqlValueType)
        DataTypes.mapOf(keyType, valueType)
      case _ =>
        unsupportedType()
    }
  }

  /** Convert Cassandra data type to Catalyst data type */
  def catalystDataType(cassandraType: DataType, nullable: Boolean): SparkSqlDataType = {

    def fromUdt(udt: UserDefinedType): StructType = {
      val fieldsAndType = udt.getFieldNames.asScala.zip(udt.getFieldTypes.asScala)
      val structFields = fieldsAndType.map { case (fieldName, dataType) =>
        StructField(
          fieldName.asInternal(),
          catalystDataType(dataType, nullable = true),
          nullable = true)
      }
      StructType(structFields.asJava)
    }

    def fromTuple(t: TupleType): StructType = {
      val structFields = t.getComponentTypes.asScala.zipWithIndex.map { case (dataType, index) =>
        StructField(
          index.toString,
          catalystDataType(dataType, nullable = true),
          nullable = true)
      }
      StructType(structFields.asJava)
    }

    cassandraType match {
      case s: SetType => ArrayType(catalystDataType(s.getElementType, nullable), nullable)
      case l: ListType => ArrayType(catalystDataType(l.getElementType, nullable), nullable)
      case m: MapType => SparkSqlMapType(catalystDataType(m.getKeyType, nullable), catalystDataType(m.getValueType, nullable), nullable)
      case udt: UserDefinedType => fromUdt(udt)
      case t: TupleType => fromTuple(t)
      case v: VectorType => ArrayType(catalystDataType(v.getElementType, nullable), nullable)
      case VARINT =>
        logWarning("VarIntType is mapped to catalystTypes.DecimalType with unlimited values.")
        primitiveCatalystDataType(cassandraType)
      case _ => primitiveCatalystDataType(cassandraType)
    }
  }

  def primitiveCatalystDataType(cassandraType: DataType): SparkSqlDataType = {
    primitiveTypeMap(cassandraType)
  }

  def toStructField(column: ColumnMetadata, nullable: Boolean): StructField = {
    StructField(
      column.getName.asInternal(),
      catalystDataType(column.getType, nullable = true),
      nullable
    )
  }

  def toStructType(metadata: RelationMetadata): StructType = {
    val partitionKeys = metadata.getPartitionKey.asScala.toSet
    val allColumns = metadata.getColumns.asScala.map(_._2).toSeq
    StructType(allColumns.map(column => toStructField(column, nullable = !partitionKeys.contains(column))))
  }

  def toStructTypeAllNullable(metadata: TableMetadata): StructType = {
    val allColumns = metadata.getColumns.asScala.map(_._2).toSeq

    StructType(allColumns.map(column => toStructField(column, true)))
  }

  def parseList(str: String): List[String] = {
    val arr = str match {
      case BracketList(innerStr) => innerStr.split(",")
    }
    arr.map(_.replaceAll("\\s", "")).toList
  }

  def parseProperty(str: String): Any = {
    Try(parseMap(str).asJava) orElse
      Try(str) get
  }

  def parseMap(str: String): Map[String, String] = {
    def handleInner(innerStr: String) = {
      innerStr.split(",")
        .map(_.replaceAll("\\s", ""))
        .map(_.split("="))
        .map(kv =>
          if (kv.length != 2) {
            throw new IllegalArgumentException(s"Cannot form Map from $str")
          } else {
            (kv(0), kv(1))
          }
        )
    }

    val m = str match {
      case BracketList(innerStr) => handleInner(innerStr)
      case BracketMap(innerStr) => handleInner(innerStr)
    }

    m.toMap
  }

  def optionsListToString(options: List[(String, Any)]): String = {
    options.map {
      case (k: String, v: String) => s"$k='$v'"
      case (k: String, innerM: Map[String, String] @unchecked) => s"$k='${mapToString(innerM)}'"
      case (_, _) => throw new IllegalArgumentException(s"Unable to parse $options")
    }.mkString(",")

  }

  def mapToString(m: Map[String, String]): String = {
    m.map { case (k, v) => (s"$k=$v") }.mkString("{", ",", "}")

  }
}
