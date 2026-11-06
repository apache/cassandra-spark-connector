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

import _root_.io.teknek.arizona.spark.connector
import _root_.io.teknek.arizona.spark.connector.cql.ColumnDef
import _root_.io.teknek.arizona.spark.connector.types.{TupleFieldDef, UDTFieldDef}
import _root_.io.teknek.arizona.spark.connector.util.Logging
import org.apache.spark.sql.{types => catalystTypes}

/** Convert Cassandra data type to Catalyst data type */
object DataTypeConverter extends Logging {

  private[cassandra] val primitiveTypeMap = Map[connector.types.ColumnType[_], catalystTypes.DataType](
    connector.types.TextType       -> catalystTypes.StringType,
    connector.types.AsciiType      -> catalystTypes.StringType,
    connector.types.VarCharType    -> catalystTypes.StringType,
    connector.types.LineStringType -> catalystTypes.StringType,
    connector.types.PolygonType    -> catalystTypes.StringType,
    connector.types.PointType      -> catalystTypes.StringType,

    connector.types.BooleanType    -> catalystTypes.BooleanType,

    connector.types.IntType        -> catalystTypes.IntegerType,
    connector.types.BigIntType     -> catalystTypes.LongType,
    connector.types.CounterType    -> catalystTypes.LongType,
    connector.types.FloatType      -> catalystTypes.FloatType,
    connector.types.DoubleType     -> catalystTypes.DoubleType,
    connector.types.SmallIntType   -> catalystTypes.ShortType,
    connector.types.TinyIntType    -> catalystTypes.ByteType,

    connector.types.VarIntType     -> catalystTypes.DecimalType(38, 0), // no native arbitrary-size integer type
    connector.types.DecimalType    -> catalystTypes.DecimalType(38, 18),

    connector.types.TimestampType  -> catalystTypes.TimestampType,
    connector.types.InetType       -> catalystTypes.StringType,
    connector.types.UUIDType       -> catalystTypes.StringType,
    connector.types.TimeUUIDType   -> catalystTypes.StringType,
    connector.types.BlobType       -> catalystTypes.BinaryType,
    connector.types.DateType       -> catalystTypes.DateType,
    connector.types.TimeType       -> catalystTypes.LongType,
    connector.types.DurationType   -> catalystTypes.StringType,
    connector.types.DateRangeType  -> catalystTypes.StringType
  )

  /** Convert Cassandra data type to Catalyst data type */
  def catalystDataType(cassandraType: connector.types.ColumnType[_], nullable: Boolean): catalystTypes.DataType = {

    def catalystStructField(field: UDTFieldDef): catalystTypes.StructField =
      catalystTypes.StructField(
        field.columnName,
        catalystDataType(field.columnType, nullable = true),
        nullable = true)

    def catalystStructFieldFromTuple(field: TupleFieldDef): catalystTypes.StructField =
      catalystTypes.StructField(
        field.index.toString,
        catalystDataType(field.columnType, nullable = true),
        nullable = true)

    cassandraType match {
      case connector.types.SetType(et, _)                => catalystTypes.ArrayType(catalystDataType(et, nullable), nullable)
      case connector.types.ListType(et, _)               => catalystTypes.ArrayType(catalystDataType(et, nullable), nullable)
      case connector.types.VectorType(et, _)             => catalystTypes.ArrayType(catalystDataType(et, nullable), nullable)
      case connector.types.MapType(kt, vt, _)            => catalystTypes.MapType(catalystDataType(kt, nullable), catalystDataType(vt, nullable), nullable)
      case connector.types.UserDefinedType(_, fields, _) => catalystTypes.StructType(fields.map(catalystStructField))
      case connector.types.TupleType(fields @ _* )       => catalystTypes.StructType(fields.map(catalystStructFieldFromTuple))
      case connector.types.VarIntType                    =>
        logWarning("VarIntType is mapped to catalystTypes.DecimalType with unlimited values.")
        primitiveCatalystDataType(cassandraType)
      case _                                          => primitiveCatalystDataType(cassandraType)
    }
  }

  def primitiveCatalystDataType(cassandraType: connector.types.ColumnType[_]): catalystTypes.DataType = {
    primitiveTypeMap(cassandraType)
  }

  /** Create a Catalyst StructField from a Cassandra Column */
  def toStructField(column: ColumnDef): catalystTypes.StructField = {
    val nullable = !column.isPrimaryKeyColumn
    catalystTypes.StructField(
      column.columnName,
      catalystDataType(column.columnType, nullable),
      nullable
    )
  }

}
