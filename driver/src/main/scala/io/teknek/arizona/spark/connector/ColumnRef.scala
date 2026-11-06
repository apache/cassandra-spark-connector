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

package io.teknek.arizona.spark.connector

import scala.language.implicitConversions

/** A column that can be selected from CQL results set by name. */
sealed trait ColumnRef  {

  /** Returns the column name which this selection bases on.
    * In case of a function, such as `ttl` or `writetime`,
    * it returns the column name passed to that function. */
  def columnName: String

  /** Returns the name of the column to be used by the user in the RDD item object.
    * If the column is selected into a CassandraRow, this name should be used to get the
    * column value. Also this name will be matched to an object property by column mappers. */
  def selectedAs: String

  /** Returns a CQL phrase which has to be passed to the
    * `SELECT` clause with appropriate quotation marks. */
  def cql: String

  /** Returns a name of the value of this column in the CQL result set.
    * Used when retrieving values form the CQL result set.*/
  def cqlValueName: String
}

/** Insert behaviors for Collections. */
sealed trait CollectionBehavior
case object CollectionOverwrite extends CollectionBehavior
case object CollectionAppend extends CollectionBehavior
case object CollectionPrepend extends CollectionBehavior
case object CollectionRemove extends CollectionBehavior


/** References a column by name. */
case class ColumnName(columnName: String, alias: Option[String] = None) extends ColumnRef {
  override val cql = s""""$columnName""""
  override def cqlValueName = columnName
  override def selectedAs = alias.getOrElse(columnName)
  override def toString: String = columnName

  def overwrite = CollectionColumnName(columnName, alias, CollectionOverwrite)
  def add = CollectionColumnName(columnName, alias, CollectionAppend)
  def append = CollectionColumnName(columnName, alias, CollectionAppend)
  def prepend = CollectionColumnName(columnName, alias, CollectionPrepend)
  def remove = CollectionColumnName(columnName, alias, CollectionRemove)

  def as(alias: String) = copy(alias = Some(alias))
}

/** References a collection column by name with insert instructions */
case class CollectionColumnName(
    columnName: String,
    alias: Option[String] = None,
    collectionBehavior: CollectionBehavior = CollectionOverwrite) extends ColumnRef {

  override val cql = s""""$columnName""""
  override def cqlValueName = columnName
  override def selectedAs = alias.getOrElse(columnName)
  override def toString: String = columnName

  def as(alias: String) = copy(alias = Some(alias))

  def overwrite = copy(collectionBehavior = CollectionOverwrite)
  def add = copy(collectionBehavior = CollectionAppend)
  def append = copy(collectionBehavior = CollectionAppend)
  def prepend = copy(collectionBehavior = CollectionPrepend)
  def remove = copy(collectionBehavior = CollectionRemove)
}

/** References TTL of a column. */
case class TTL(columnName: String, alias: Option[String] = None) extends ColumnRef {
  override val cql = s"""TTL("$columnName")"""
  override val cqlValueName = s"ttl($columnName)"
  override def selectedAs = alias.getOrElse(cqlValueName)
  override def toString: String = cqlValueName

  def as(alias: String) = copy(alias = Some(alias))
}

/** References write time of a column. */
case class WriteTime(columnName: String, alias: Option[String] = None) extends ColumnRef {
  override val cql = s"""WRITETIME("$columnName")"""
  override val cqlValueName = s"writetime($columnName)"
  override def selectedAs = alias.getOrElse(cqlValueName)
  override def toString: String = cqlValueName

  def as(alias: String) = copy(alias = Some(alias))
}

/** References a row count value returned from SELECT count(*)  */
case object RowCountRef extends ColumnRef {
  override def selectedAs: String = "count"
  override def columnName: String = "count"
  override def cqlValueName: String = "count"
  override def cql: String = "count(*)"
}

/** References a function call **/
case class FunctionCallRef(
  columnName: String,
  actualParams: Seq[Either[ColumnRef, String]] = Seq.empty,
  alias: Option[String] = None) extends ColumnRef {

  override def selectedAs: String = alias.getOrElse(cqlValueName)
  override def cqlValueName: String = s"$columnName(${stringifyArguments(actualParams, _.cqlValueName)})"
  override def cql: String = s"$columnName(${stringifyArguments(actualParams, _.cql)})"

  def requiredColumns: Seq[ColumnRef] = actualParams flatMap {
    case Left(fcall: FunctionCallRef) => fcall.requiredColumns
    case Left(cref) => Seq(cref)
    case Right(_) => Seq.empty
  }

  private def stringifyArguments(
    child: Seq[Either[ColumnRef, String]],
    stringifyColumn: ColumnRef => String): String = child map {
      case Left(cr) => stringifyColumn(cr)
      case Right(str) => str
    } mkString ","

}