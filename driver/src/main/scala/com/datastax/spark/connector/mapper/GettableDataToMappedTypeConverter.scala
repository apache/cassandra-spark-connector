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

package com.datastax.spark.connector.mapper

import java.lang.reflect.Method

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.StructDef
import com.datastax.spark.connector.types._
import com.datastax.spark.connector.util.{AnyObjectFactory, ReflectionUtil, Symbols}

import scala.language.existentials
import scala.reflect.runtime.universe._

/** Converts a `GettableData` object representing a table row or a UDT value
  * to a tuple or a case class object using the given `ColumnMapper`.
  *
  * @param structDef table or UDT definition to convert from
  * @param columnSelection columns that have been selected from the struct
  */
class GettableDataToMappedTypeConverter[T : TypeTag : ColumnMapper](
    structDef: StructDef,
    columnSelection: IndexedSeq[ColumnRef])
  extends TypeConverter[T] {

  // can't be lazy, because TypeTags are not serializable, and if we made it lazy,
  // the right side would be stored in a hidden serialized variable anyway
  @transient
  private val _targetTypeTag = implicitly[TypeTag[T]]

  override def targetTypeTag: TypeTag[T] =
    Option(_targetTypeTag) match {
      case Some(tt) => tt
      case None => throw new NullPointerException(
        "Requested a TypeTag of the GettableToMappedTypeConverter which can't deserialize TypeTags " +
          "due to Scala 2.10 TypeTag limitation. They come back as nulls and therefore you see this NPE.")
    }

  // we can't serialize the type tag, but we can at least serialize the type name,
  // therefore overriding targetTypeName and making it strict (non-lazy).
  override val targetTypeName: String =
    targetTypeTag.tpe.toString

  // must be serialized directly, for we can't recreate it on the deserialization side, as we don't have
  // the TypeTag anymore
  private val factory = new AnyObjectFactory[T]

  /** To determine which default column mapper to use for UDT fields.
    * If this class is a Java bean, then the UDT fields will be mapped with
    * the `JavaBeanColumnMapper`*/
  private val isJavaBean =
    implicitly[ColumnMapper[T]].isInstanceOf[JavaBeanColumnMapper[_]]

  val columnMap: ColumnMapForReading =
    implicitly[ColumnMapper[T]].columnMapForReading(structDef, columnSelection)

  /** Returns the column mapper associated with the given type.
    * Used to find column mappers for UDT columns.
    * For tuples, returns [[com.datastax.spark.connector.mapper.TupleColumnMapper TupleColumnMapper]],
    * for Java beans uses
    * [[com.datastax.spark.connector.mapper.JavaBeanColumnMapper JavaBeanColumnMapper]],
    * and for everything else uses
    * [[com.datastax.spark.connector.mapper.DefaultColumnMapper DefaultColumnMapper]] */
  private def columnMapper[U : TypeTag]: ColumnMapper[U] = {
    val tpe = typeTag[U].tpe
    if (tpe.typeSymbol.fullName startsWith "scala.Tuple")
      new TupleColumnMapper[U]
    else if (isJavaBean) 
      new JavaBeanColumnMapper[U]()(ReflectionUtil.classTag[U])
    else
      new DefaultColumnMapper[U]
  }

  /** Returns a converter that can convert a Cassandra value of given `ColumnType` to Scala type `U`. */
  private def converter[U : TypeTag](columnType: ColumnType[_]): TypeConverter[U] = {
    /*
    This method does not immediately call `TypeConverter.forType`, because `forType` doesn't know
    the Cassandra `ColumnType` and therefore it can't instantiate appropriate type converters for
    Cassandra user-defined-types, which could be nested. Therefore, for types that can nest other types
    i.e. collections, options or UDTs, we make sure to use this method for any needed nested converters.
    E.g. for a list type, we recursively call this method to get the converter for the
    list items and then we call `TypeConverter.forType` to get a proper converter for lists.
    */
    val tpe = typeTag[U].tpe
    (columnType, tpe) match {
      case (argColumnType, TypeRef(_, Symbols.OptionSymbol, List(argScalaType))) =>
        val argConverter = converter(argColumnType, argScalaType)
        TypeConverter.forType[U](Seq(argConverter))

      case (struct: StructDef, _) =>
        implicit val cm: ColumnMapper[U] = columnMapper[U]
        new GettableDataToMappedTypeConverter[U](struct, struct.columnRefs)

      case (ListType(argColumnType, _), TypeRef(_, _, List(argScalaType))) =>
        val argConverter = converter(argColumnType, argScalaType)
        TypeConverter.forType[U](Seq(argConverter))

      case (VectorType(argColumnType, _), TypeRef(_, _, List(argScalaType))) =>
        val argConverter = converter(argColumnType, argScalaType)
        TypeConverter.forType[U](Seq(argConverter))

      case (SetType(argColumnType, _), TypeRef(_, _, List(argScalaType))) =>
        val argConverter = converter(argColumnType, argScalaType)
        TypeConverter.forType[U](Seq(argConverter))

      case (MapType(keyColumnType, valueColumnType, _), TypeRef(_, _, List(keyScalaType, valueScalaType))) =>
        val keyConverter = converter(keyColumnType, keyScalaType)
        val valueConverter = converter(valueColumnType, valueScalaType)
        TypeConverter.forType[U](Seq(keyConverter, valueConverter))

      case (_, _) =>
        TypeConverter.forType[U]
    }
  }

  /** Returns a converter that can convert
    * a Cassandra value of given `ColumnType` to Scala type given by `Type` */
  private def converter(columnType: ColumnType[_], tpe: Type): TypeConverter[_] = {
    type U = u forSome { type u }
    implicit val tt = ReflectionUtil.typeToTypeTag[U](tpe)
    converter[U](columnType)
  }

  /** Returns the type of the column, basing on the struct definition. */
  private def columnType(columnRef: ColumnRef): ColumnType[_] = {
    columnRef match {
      case TTL(_, _) | WriteTime(_, _) | RowCountRef =>
        BigIntType
      case c:ColumnRef =>
        structDef.columnByName(c.columnName).columnType
    }
  }

  /** Tries to convert the value with the given converter and handles the error, if any */
  private def tryConvert(value: AnyRef, converter: TypeConverter[_], name: String): AnyRef = {
    try
      converter.convert(value).asInstanceOf[AnyRef]
    catch {
      case e: Exception =>
        throw new TypeConversionException(
          s"Failed to convert column $name " +
            s"of ${structDef.name} " +
            s"to ${converter.targetTypeName}: $value", e)
    }
  }

  /** Throws NPE if value is null when it is not allowed to.
    * The `ColumnMapper` decides whether it is allowed or not. */
  private def checkNotNull(value: AnyRef, name: String): AnyRef = {
    if (!columnMap.allowsNull && value == null) {
      throw new scala.NullPointerException(
        s"Unexpected null value of column $name in ${structDef.name}." +
          "If you want to receive null values from Cassandra, please wrap the column type into Option " +
          "or use JavaBeanColumnMapper")
    }
    value
  }

  /** Reads the value of the given column from the input data and converts it with the given converter.*/
  private def convertedColumnValue(
      columnRef: ColumnRef,
      data: GettableData,
      converter: TypeConverter[_]): AnyRef = {
    val name = columnRef.columnName
    val value = data.getRawCql(columnRef.cqlValueName)
    checkNotNull(tryConvert(value, converter, name), name)
  }

  /** Converters for converting each of the constructor parameters */
  private val ctorParamConverters: Seq[TypeConverter[_]] = {
    val ctorParamTypes: Seq[Type] = factory.constructorParamTypes.toIndexedSeq
    val ctorColumnTypes: Seq[ColumnType[_]] = columnMap.constructor.map(columnType)
    for ((ct, pt) <- ctorColumnTypes zip ctorParamTypes) yield
      converter(ct, pt)
  }

  /** Converters for converting values passed to setters */
  private val setterParamConverters: Map[String, TypeConverter[_]] = {
    val targetType = typeTag[T].tpe
    val setterParamTypes: Map[String, Type] =
      for ((s, _) <- columnMap.setters)
        yield (s, ReflectionUtil.methodParamTypes(targetType, s).head)
    val setterColumnTypes: Map[String, ColumnType[_]] =
      columnMap.setters.view.mapValues(columnType).toMap
    for (setterName <- setterParamTypes.keys) yield {
      val ct = setterColumnTypes(setterName)
      val pt = setterParamTypes(setterName)
      (setterName, converter(ct, pt))
    }
  }.toMap

  /** Evaluates the i-th constructor param value, based on given input data.
    * The returned value is already converted to the type expected by the constructor. */
  private def ctorParamValue(i: Int, data: GettableData): AnyRef = {
    val ref = columnMap.constructor(i)
    val converter = ctorParamConverters(i)
    convertedColumnValue(ref, data, converter)
  }

  /** Evaluates the i-th constructor param value, based on given input data.
    * The returned value is already converted to the type expected by the constructor. */
  private def ctorParamValue(i: Int, data: GettableByIndexData): AnyRef = {
    val ref = columnMap.constructor(i)
    val converter = ctorParamConverters(i)
    val value = data.getRaw(i)
    val name = i.toString
    checkNotNull(tryConvert(value, converter, name), name)
  }

  /** Evaluates the argument to be passed to the given setter of type `T`, based on given input data.
    * The returned value is already converted to the type expected by the setter. */
  private def setterParamValue(setter: String, data: GettableData): AnyRef = {
    val ref = columnMap.setters(setter)
    val converter = setterParamConverters(setter)
    convertedColumnValue(ref, data, converter)
  }

  /** Buffer for storing output object constructor arguments.
    * This is defined at this level solely for GC optimisation - to avoid
    * creating this array per every constructed object. */
  @transient
  private lazy val buffer = new ThreadLocal[Array[AnyRef]] {
    override def initialValue() = Array.ofDim[AnyRef](factory.argCount)
  }

  /** Fills buffer with converted constructor arguments */
  private def fillBuffer(data: GettableData, buf: Array[AnyRef]): Unit = {
    for (i <- buf.indices)
      buf(i) = ctorParamValue(i, data)
  }

  /** Fills buffer with converted constructor arguments */
  private def fillBuffer(data: GettableByIndexData, buf: Array[AnyRef]): Unit = {
    for (i <- buf.indices)
      buf(i) = ctorParamValue(i, data)
  }


  /** List of the setters on type `T` with their corresponding column references.
    * Evaluated basing on the information from the `ColumnMapper`.
    * Setters for properties already covered by the constructor of `T` are not included. */
  @transient
  private lazy val setters: Array[(Method, ColumnRef)] = {
    val constructorColumnRefs = columnMap.constructor.toSet
    val methods = factory.javaClass.getMethods.map(m => (m.getName, m)).toMap
    columnMap.setters.toArray.collect {
      case (setterName, columnRef) if !constructorColumnRefs.contains(columnRef) =>
        (methods(setterName), columnRef)
    }
  }

  /** Uses setters of `T` to set values found in GettableData */
  private def invokeSetters(data: GettableData, obj: T): Unit = {
    for ((setter, columnRef) <- setters) {
      val value = setterParamValue(setter.getName, data)
      setter.invoke(obj, value)
    }
  }

  /** The main function to convert a `GettableData`
    * to an object that has a `ColumnMapper` */
  override def convertPF: PartialFunction[Any, T] = {

    case data: GettableData =>
      val buf = buffer.get()
      fillBuffer(data, buf)
      val obj = factory.newInstance(buf.toIndexedSeq: _*)
      invokeSetters(data, obj)
      obj

    case data: GettableByIndexData =>
      val buf = buffer.get()
      fillBuffer(data, buf)
      factory.newInstance(buf.toIndexedSeq: _*)
  }
}