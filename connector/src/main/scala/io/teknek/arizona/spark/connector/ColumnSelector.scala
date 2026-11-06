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

import _root_.io.teknek.arizona.spark.connector.cql.TableDef

sealed trait ColumnSelector {
  def aliases: Map[String, String]
  def selectFrom(table: TableDef): IndexedSeq[ColumnRef]
}

case object AllColumns extends ColumnSelector {
  override def aliases: Map[String, String] = Map.empty.withDefault(x => x)
  override def selectFrom(table: TableDef) =
    table.columns.map(_.ref)
}

case object PartitionKeyColumns extends ColumnSelector {
  override def aliases: Map[String, String] = Map.empty.withDefault(x => x)
  override def selectFrom(table: TableDef) =
    table.partitionKey.map(_.ref).toIndexedSeq
}

case object PrimaryKeyColumns extends ColumnSelector {
  override def aliases: Map[String, String] = Map.empty.withDefault(x => x)
  override def selectFrom(table: TableDef) =
    table.primaryKey.map(_.ref)
}

case class SomeColumns(columns: ColumnRef*) extends ColumnSelector {

  override def aliases: Map[String, String] = columns.map {
    case ref => (ref.selectedAs, ref.cqlValueName)
  }.toMap

  override def selectFrom(table: TableDef): IndexedSeq[ColumnRef] = {
    val missing = table.missingColumns {
      columns flatMap {
        case f: FunctionCallRef => f.requiredColumns //Replaces function calls by their required columns
        case RowCountRef => Seq.empty //Filters RowCountRef from the column list
        case other => Seq(other)
      }
    }
    if (missing.nonEmpty) throw new NoSuchElementException(
      s"Columns not found in table ${table.name}: ${missing.mkString(", ")}")

    columns.toIndexedSeq
  }
}

object SomeColumns {
  @deprecated("Use io.teknek.arizona.spark.connector.rdd.SomeColumns instead of Seq", "1.0")
  implicit def seqToSomeColumns(columns: Seq[String]): SomeColumns =
    SomeColumns(columns.map(x => x: ColumnRef): _*)
}


