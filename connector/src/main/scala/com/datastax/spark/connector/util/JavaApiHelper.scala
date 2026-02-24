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

package com.datastax.spark.connector.util

import java.lang.{Iterable => JIterable}
import java.util.{Collection => JCollection}
import java.util.{Map => JMap}

import scala.reflect._
import scala.reflect.api.{Mirror, TypeCreator, _}
import scala.reflect.runtime.universe._

import org.apache.spark.api.java.function.{Function => JFunction}

import com.datastax.spark.connector.CassandraRow
import com.datastax.spark.connector.mapper.{ColumnMapper, JavaBeanColumnMapper}
import com.datastax.spark.connector.rdd.reader.RowReaderFactory
import com.datastax.spark.connector.writer.RowWriterFactory
import scala.jdk.CollectionConverters._

/** A helper class to make it possible to access components written in Scala from Java code.
  * INTERNAL API
  */
object JavaApiHelper {

  def mirror = runtimeMirror(Thread.currentThread().getContextClassLoader)

  /** Returns a `TypeTag` for the given class. */
  def getTypeTag[T](clazz: Class[T]): TypeTag[T] = {
    TypeTag.apply(mirror, new TypeCreator {
      override def apply[U <: Universe with Singleton](m: Mirror[U]): U#Type = {
        m.staticClass(clazz.getName).toTypeConstructor
      }
    })
  }

  /** Returns a `TypeTag` for the given class and type parameters. */
  def getTypeTag[T](clazz: Class[_], typeParams: TypeTag[_]*): TypeTag[T] = {
    TypeTag.apply(mirror, new TypeCreator {
      override def apply[U <: Universe with Singleton](m: Mirror[U]) = {
        val ct = m.staticClass(clazz.getName).toTypeConstructor.asInstanceOf[m.universe.Type]
        val tpt = typeParams.map(_.in(m).tpe.asInstanceOf[m.universe.Type]).toList
        m.universe.appliedType(ct, tpt).asInstanceOf[U#Type]
      }
    })
  }

  def getTypeTag[T](clazz: Class[_], typeParams: Array[TypeTag[_]]): TypeTag[T] =
    getTypeTag(clazz, typeParams.toSeq: _*)

  /** Returns a `ClassTag` of a given runtime class. */
  def getClassTag[T](clazz: Class[T]): ClassTag[T] = ClassTag(clazz)

  /** Returns a `ClassTag` of a given runtime class. */
  def getClassTag2[T](clazz: Class[_]): ClassTag[T] = ClassTag(clazz)

  def toScalaFunction1[T1, R](f: JFunction[T1, R]): T1 => R = f.call

  def valuesAsJavaIterable[K, V, IV <: Iterable[V]]: ((K, IV)) => (K, JIterable[V]) = {
    case (k, iterable) => (k, iterable.asJava)
  }

  def valuesAsJavaCollection[K, V, IV <: Iterable[V]]: ((K, IV)) => (K, JCollection[V]) = {
    case (k, iterable) => (k, iterable.asJavaCollection)
  }

  /** Returns a runtime class of a given `TypeTag`. */
  def getRuntimeClass[T](typeTag: TypeTag[T]): Class[T] =
    mirror.runtimeClass(typeTag.tpe).asInstanceOf[Class[T]]

  /** Returns a runtime class of a given `ClassTag`. */
  def getRuntimeClass[T](classTag: ClassTag[T]): Class[T] =
    classTag.runtimeClass.asInstanceOf[Class[T]]

  /** Converts a Java `Map` to a Scala immutable `Map`. */
  def toScalaMap[K, V](map: JMap[K, V]): Map[K, V] = Map(map.asScala.toSeq: _*)

  /** Converts an array to a Scala `Seq`. */
  def toScalaSeq[T](array: Array[T]): Seq[T] = array.toIndexedSeq

  /** Converts an array to a Scala `Seq`. */
  def toScalaImmutableSeq[T](array: Array[T]): scala.collection.immutable.Seq[T] = array.toIndexedSeq

  /** Converts a Java `Iterable` to Scala `Seq`. */
  def toScalaSeq[T](iterable: java.lang.Iterable[T]): Seq[T] = iterable.asScala.toSeq

  /** Returns the default `RowWriterFactory` initialized with the given `ColumnMapper`. */
  def defaultRowWriterFactory[T](typeTag: TypeTag[T], mapper: ColumnMapper[T]): RowWriterFactory[T] = {
    RowWriterFactory.defaultRowWriterFactory(typeTag, mapper)
  }

  /** Returns the `JavaBeanColumnMapper` instance for the given `ClassTag` and column mapping. */
  def javaBeanColumnMapper[T](
    classTag: ClassTag[T],
    columnNameOverride: JMap[String, String]
  ): ColumnMapper[T] =
    new JavaBeanColumnMapper[T](toScalaMap(columnNameOverride))(classTag)

  /** Returns the default `RowReaderFactory`. */
  def genericRowReaderFactory: RowReaderFactory[CassandraRow] = RowReaderFactory.GenericRowReader$

  val none = None

}
