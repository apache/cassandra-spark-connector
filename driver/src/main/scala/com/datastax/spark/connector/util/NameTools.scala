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

import java.util.Locale

import com.datastax.oss.driver.api.core.metadata.Metadata
import com.datastax.spark.connector.util.DriverUtil.toName
import org.apache.commons.lang3.StringUtils

import scala.annotation.nowarn
import scala.jdk.CollectionConverters._

object NameTools {

  val MinJWScore = 0.55

  /* Example JaroWinklerScores
  {{{
       |   val wordpairs = Seq(
       |   ("banana","bananastand"),
       |   ("apple","bananastand"),
       |   ("customer_id","customerid"),
       |   ("custid","customerid"),
       |   ("liberty","freedom"),
       |   ("spark","spork"),
       |   ("test","testt"))
       |   wordpairs.foreach( p =>
       |     println(s"${p._1} ~ ${p._2} = ${StringUtils.getJaroWinklerDistance(p._1,p._2)}"))
       |   }}}
  banana ~ bananastand = 0.93
  apple ~ bananastand = 0.43
  customer_id ~ customerid = 0.99
  custid ~ customerid = 0.92
  liberty ~ freedom = 0.43
  spark ~ spork = 0.89
  test ~ testt = 0.96 */

  sealed trait Suggestions

  case class TableSuggestions(tables: Seq[(String)]) extends Suggestions
  case class KeyspaceSuggestions(keyspaces: Seq[String]) extends Suggestions
  case class KeyspaceAndTableSuggestions(keyspaceTablePairs: Seq[(String, String)]) extends Suggestions
  case class KeyspaceOnlySuggestions(keyspaces: Seq[String]) extends Suggestions


  @nowarn("cat=deprecation")
  def getSuggestions(clusterMetadata: Metadata, keyspace: String): Option[Suggestions] = {
    val keyspaceScores = clusterMetadata
      .getKeyspaces
      .values().asScala
      .toSeq
      .map(ks =>
        (toName(ks.getName), StringUtils.getJaroWinklerDistance(toName(ks.getName).toLowerCase(Locale.ROOT), keyspace.toLowerCase(Locale.ROOT))))

    val keyspaceSuggestions = keyspaceScores.filter( _._2 > MinJWScore)
      .sorted
      .map(_._1)

    if (keyspaceSuggestions.nonEmpty) {
      Some(KeyspaceOnlySuggestions(keyspaceSuggestions))
    } else {
      None
    }
  }


  /**
   * Given a Cluster Metadata Object determine if there are any fuzzy matches to the keyspace and table
   * passed in. We attempt to only show relevant results by returning suggestions with the following priority:
   * 1. Perfect match on keyspace name but only fuzzy match on table name else
   * 2. Perfect match on table name but only a fuzzy match on the keyspace name else
   * 3. Fuzzy match on keyspace and fuzzy match on table name else
   * 4. Fuzzy match on table no match on keyspace else
   * 5. None
   */
  @nowarn("cat=deprecation")
  def getSuggestions(clusterMetadata: Metadata, keyspace: String, table: String): Option[Suggestions] = {

    val keyspaceScores = clusterMetadata
      .getKeyspaces
      .values().asScala
      .toSeq
      .map(ks =>
      (ks, StringUtils.getJaroWinklerDistance(toName(ks.getName).toLowerCase(Locale.ROOT), keyspace.toLowerCase(Locale.ROOT))))

    val ktScores = for ((ks, ksScore) <- keyspaceScores; (_, t) <- (ks.getTables.asScala ++ ks.getViews.asScala)) yield {
      val tScore = StringUtils.getJaroWinklerDistance(toName(t.getName).toLowerCase(Locale.ROOT), table.toLowerCase(Locale.ROOT))
      (toName(ks.getName), toName(t.getName), ksScore, tScore)
    }

    //1. Perfect match on keyspace, fuzzy match on table
    val suggestedTables = ktScores
      .collect { case (ks, t, ksScore, tScore) if ks == keyspace && tScore > MinJWScore => t}

    //2. Fuzzy match on table, perfect match on table
    val suggestedKeyspaces = ktScores
      .collect { case (ks, t, ksScore, tScore) if ksScore > MinJWScore && t == table => ks}

    //3. Fuzzy match on keyspace, fuzzy match on table
    val suggestedKeyspaceAndTables = ktScores
      .collect { case (ks, t, ksScore, tScore) if ksScore > MinJWScore && tScore > MinJWScore => (ks, t)}

    //4. No match on keyspace, fuzzy match on table
    val suggestedTablesUnknownKeyspace = ktScores
      .collect { case (ks, t, ksScore, tScore) if tScore > MinJWScore => (ks, t)}

    if (suggestedTables.nonEmpty) Some(TableSuggestions(suggestedTables))
    else if (suggestedKeyspaces.nonEmpty) Some(KeyspaceSuggestions(suggestedKeyspaces))
    else if (suggestedKeyspaceAndTables.nonEmpty) Some(KeyspaceAndTableSuggestions(suggestedKeyspaceAndTables))
    else if (suggestedTablesUnknownKeyspace.nonEmpty) Some(KeyspaceAndTableSuggestions(suggestedTablesUnknownKeyspace))
    else None
  }

  def getErrorString(keyspace: String, table: Option[String], suggestion: Option[Suggestions]): String = suggestion match {
    case None if table.isDefined => s"Couldn't find $keyspace.${table.get} or any similarly named keyspace and table pairs"
    case None => s"Couldn't find $keyspace or any similarly named keyspaces"
    case Some(TableSuggestions(tables)) => s"Couldn't find table ${table.get} in $keyspace - Found similar tables in that keyspace:\n${tables.map(t => s"$keyspace.$t").mkString("\n")}"
    case Some(KeyspaceSuggestions(keyspaces)) => s"Couldn't find table ${table.get} in $keyspace - Found similar keyspaces with that table:\n${keyspaces.map(k => s"$k.$table").mkString("\n")}"
    case Some(KeyspaceAndTableSuggestions(kt)) => s"Couldn't find table ${table.get} or keyspace $keyspace - Found similar keyspaces and tables:\n${kt.map { case (k, t) => s"$k.$t"}.mkString("\n")}"
    case Some(KeyspaceOnlySuggestions(keyspaces)) => s"Couldn't find keyspace $keyspace - Found similar keyspaces: ${keyspaces.mkString("\n", "\n", "\n")}"
  }
}
