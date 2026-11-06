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

import com.datastax.oss.driver.api.core.{DefaultProtocolVersion, ProtocolVersion}
import org.scalatest.{FlatSpec, Matchers}
import _root_.io.teknek.arizona.spark.connector.cql._
import _root_.io.teknek.arizona.spark.connector.types.{IntType, TimeUUIDType}

class PredicatePushDownSpec extends FlatSpec with Matchers {

  // We don't want this test to rely on any Spark code,
  // so we're using our own Filters
  trait Filter
  case class EqFilter(columnName: String) extends Filter
  case class InFilter(columnName: String) extends Filter
  case class LtFilter(columnName: String) extends Filter
  case class GtFilter(columnName: String) extends Filter
  case object UnsupportedFilter extends Filter

  implicit object FilterOps extends PredicateOps[Filter] {
    override def columnName(p: Filter) = p match {
      case EqFilter(name) => name
      case InFilter(name) => name
      case LtFilter(name) => name
      case GtFilter(name) => name
      case UnsupportedFilter => throw new IllegalArgumentException("Unsupported predicate")
    }
    override def isRangePredicate(p: Filter) = p.isInstanceOf[LtFilter] || p.isInstanceOf[GtFilter]
    override def isSingleColumnPredicate(p: Filter) = p != UnsupportedFilter
    override def isEqualToPredicate(p: Filter) = p.isInstanceOf[EqFilter]
    override def isInPredicate(p: Filter) = p.isInstanceOf[InFilter]
  }

  val pk1 = ColumnDef("pk1", PartitionKeyColumn, IntType)
  val pk2 = ColumnDef("pk2", PartitionKeyColumn, IntType)
  val c1 = ColumnDef("c1", ClusteringColumn(0), IntType)
  val c2 = ColumnDef("c2", ClusteringColumn(1), IntType)
  val c3 = ColumnDef("c3", ClusteringColumn(2), IntType)
  val i1 = ColumnDef("i1", RegularColumn, IntType)
  val i2 = ColumnDef("i2", RegularColumn, IntType)
  val r1 = ColumnDef("r1", RegularColumn, IntType)
  val r2 = ColumnDef("r2", RegularColumn, IntType)
  val t1 = ColumnDef("t1", RegularColumn, TimeUUIDType)

  val timeUUIDc1 = ColumnDef("c1", ClusteringColumn(0), TimeUUIDType)

  val table = TableDef(
    keyspaceName = "test",
    tableName = "test",
    partitionKey = Seq(pk1, pk2),
    clusteringColumns = Seq(c1, c2, c3),
    regularColumns = Seq(i1, i2, r1, r2),
    indexes = Seq(
      IndexDef(Some("DummyIndex"), "i1", "IndexOne", Map.empty),
      IndexDef(Some("DummyIndex"), "i2", "IndexTwo", Map.empty))
  )

  val timeUUIDTable = TableDef(
    keyspaceName = "test",
    tableName = "uuidtab",
    partitionKey = Seq(pk1, pk2),
    clusteringColumns = Seq(timeUUIDc1),
    regularColumns = Seq(i1, i2, r1, r2, t1)
  )

  val table2 = TableDef(
    keyspaceName = "test",
    tableName = "test",
    partitionKey = Seq(pk1, pk2),
    clusteringColumns = Seq(c1, c2, c3),
    regularColumns = Seq(i1, i2, r1, r2),
    indexes = Seq(
      IndexDef(Some("DummyIndex"), "i1", "IndexOne", Map.empty),
      IndexDef(Some("DummyIndex"), "i2", "IndexTwo", Map.empty),
      IndexDef(Some("DummyIndex"), "pk1", "IndexThree", Map.empty))
  )

  "BasicCassandraPredicatePushDown" should "push down all equality predicates restricting partition key columns" in {
    val f1 = EqFilter("pk1")
    val f2 = EqFilter("pk2")
    val ppd = new BasicCassandraPredicatePushDown(Set[Filter](f1, f2), table)
    ppd.predicatesToPushDown should contain allOf(f1, f2)
    ppd.predicatesToPreserve shouldBe empty
  }

  it should " work if the user tries to use a TimeUUID on a fully handled predicate" in {
    val f1 = GtFilter("c1")
    val ppd = new BasicCassandraPredicatePushDown(Set[Filter](f1), timeUUIDTable)
    ppd.predicatesToPushDown should contain (f1)
    ppd.predicatesToPreserve shouldBe empty
  }

  it should " work if the user tries to use a TimeUUID column in a eq predicate" in {
    val f1 = EqFilter("c1")
    val ppd = new BasicCassandraPredicatePushDown(Set[Filter](f1), timeUUIDTable)
    ppd.predicatesToPushDown should contain (f1)
    ppd.predicatesToPreserve shouldBe empty
  }

  it should "not push down a partition key predicate for a part of the partition key" in {
    val f1 = EqFilter("pk1")
    val ppd1 = new BasicCassandraPredicatePushDown(Set[Filter](f1), table)
    ppd1.predicatesToPushDown shouldBe empty
    ppd1.predicatesToPreserve should contain(f1)

    val f2 = EqFilter("pk2")
    val ppd2 = new BasicCassandraPredicatePushDown(Set[Filter](f2), table)
    ppd2.predicatesToPushDown shouldBe empty
    ppd2.predicatesToPreserve should contain(f2)
  }

  it should "not push down a range partition key predicate" in {
    val f1 = EqFilter("pk1")
    val f2 = LtFilter("pk2")
    val ppd = new BasicCassandraPredicatePushDown(Set[Filter](f1, f2), table)
    ppd.predicatesToPushDown shouldBe empty
    ppd.predicatesToPreserve should contain allOf(f1, f2)
  }

  it should "push down an IN partition key predicate on the last partition key column" in {
    val f1 = EqFilter("pk1")
    val f2 = InFilter("pk2")
    val ppd = new BasicCassandraPredicatePushDown(Set[Filter](f1, f2), table)
    ppd.predicatesToPushDown should contain allOf(f1, f2)
    ppd.predicatesToPreserve shouldBe empty
  }

  it should "push down an IN partition key predicate on the non-last partition key column" in {
    val f1 = InFilter("pk1")
    val f2 = EqFilter("pk2")
    val ppd = new BasicCassandraPredicatePushDown(Set[Filter](f1, f2), table)
    ppd.predicatesToPushDown should contain allOf(f1, f2)
    ppd.predicatesToPreserve shouldBe empty
  }

  it should "push down an IN partition key predicate on all partition key columns" in {
    val f1 = InFilter("pk1")
    val f2 = InFilter("pk2")
    val ppd = new BasicCassandraPredicatePushDown(Set[Filter](f1, f2), table)
    ppd.predicatesToPushDown should contain allOf(f1, f2)
    ppd.predicatesToPreserve shouldBe empty
  }

  it should "not push down an IN partition key predicate on the non-last partition key column for V3 and older" in {
    val f1 = InFilter("pk1")
    val f2 = EqFilter("pk2")
    val ppd = new BasicCassandraPredicatePushDown(Set[Filter](f1, f2), table, DefaultProtocolVersion.V3)
    ppd.predicatesToPushDown shouldBe empty
    ppd.predicatesToPreserve should contain allOf(f1, f2)
  }

  it should "push down the first clustering column predicate" in {
    val f1 = EqFilter("c1")
    val ppd = new BasicCassandraPredicatePushDown(Set[Filter](f1), table)
    ppd.predicatesToPushDown should contain only f1
    ppd.predicatesToPreserve shouldBe empty
  }

  it should "push down the first and the second clustering column predicate" in {
    val f1 = EqFilter("c1")
    val f2 = LtFilter("c2")
    val ppd = new BasicCassandraPredicatePushDown(Set[Filter](f1, f2), table)
    ppd.predicatesToPushDown should contain only(f1, f2)
    ppd.predicatesToPreserve shouldBe empty
  }

  it should "push down restrictions on only the initial clustering columns" in {
    val f1 = EqFilter("c1")
    val f2 = EqFilter("c3")

    val ppd1 = new BasicCassandraPredicatePushDown(Set[Filter](f1, f2), table)
    ppd1.predicatesToPushDown should contain only f1
    ppd1.predicatesToPreserve should contain only f2

    val ppd2 = new BasicCassandraPredicatePushDown(Set[Filter](f2), table)
    ppd2.predicatesToPushDown shouldBe empty
    ppd2.predicatesToPreserve should contain only f2
  }

  it should "push down only one range predicate restricting the first clustering column, " +
      "if there are more range predicates on different clustering columns" in {
    val f1 = LtFilter("c1")
    val f2 = LtFilter("c2")
    val ppd = new BasicCassandraPredicatePushDown(Set[Filter](f1, f2), table)
    ppd.predicatesToPushDown should contain only f1
    ppd.predicatesToPreserve should contain only f2
  }

  it should "push down multiple range predicates for the same clustering column" in {
    val f1 = LtFilter("c1")
    val f2 = GtFilter("c1")
    val ppd = new BasicCassandraPredicatePushDown(Set[Filter](f1, f2), table)
    ppd.predicatesToPushDown should contain allOf (f1, f2)
    ppd.predicatesToPreserve shouldBe empty
  }

  it should "push down clustering column predicates when the last clustering column is restricted by IN" in {
    val f1 = EqFilter("c1")
    val f2 = EqFilter("c2")
    val f3 = InFilter("c3")
    val ppd = new BasicCassandraPredicatePushDown(Set[Filter](f1, f2, f3), table)
    ppd.predicatesToPushDown should contain only(f1, f2, f3)
    ppd.predicatesToPreserve shouldBe empty
  }

  it should "stop pushing down clustering column predicates on the first range predicate" in {
    val f1 = EqFilter("c1")
    val f2 = LtFilter("c2")
    val f3 = EqFilter("c3")
    val ppd = new BasicCassandraPredicatePushDown(Set[Filter](f1, f2, f3), table)
    ppd.predicatesToPushDown should contain only(f1, f2)
    ppd.predicatesToPreserve should contain only f3
  }

  it should "not push down IN restriction on non-last column for V3 and older" in {
    val f1 = EqFilter("c1")
    val f2 = InFilter("c2")
    val f3 = EqFilter("c3")
    val ppd = new BasicCassandraPredicatePushDown(Set[Filter](f1, f2, f3), table, DefaultProtocolVersion.V3)
    ppd.predicatesToPushDown should contain only f1
    ppd.predicatesToPreserve should contain only (f2, f3)
  }

  it should "push down IN restriction on non-last clustering column" in {
    val f1 = EqFilter("c1")
    val f2 = InFilter("c2")
    val f3 = EqFilter("c3")
    val ppd = new BasicCassandraPredicatePushDown(Set[Filter](f1, f2, f3), table)
    ppd.predicatesToPushDown should contain only (f1, f2, f3)
    ppd.predicatesToPreserve shouldBe empty
  }

  it should "push down IN restriction on all clustering columns" in {
    val f1 = InFilter("c1")
    val f2 = InFilter("c2")
    val f3 = InFilter("c3")
    val ppd = new BasicCassandraPredicatePushDown(Set[Filter](f1, f2, f3), table)
    ppd.predicatesToPushDown should contain only (f1, f2, f3)
    ppd.predicatesToPreserve shouldBe empty
  }

  it should "not push down any clustering column predicates, if the first clustering column is missing" in {
    val f1 = EqFilter("c2")
    val ppd = new BasicCassandraPredicatePushDown(Set[Filter](f1), table)
    ppd.predicatesToPushDown shouldBe empty
    ppd.predicatesToPreserve should contain only f1
  }

  it should "not push down partition key index equality predicates in P3" in {
    val f1 = EqFilter("pk1")
    val ppd = new BasicCassandraPredicatePushDown(Set[Filter](f1), table2, DefaultProtocolVersion.V3)
    ppd.predicatesToPushDown shouldBe empty
    ppd.predicatesToPreserve should contain only f1
  }

  it should "push down partition key index equality predicates " in {
    val f1 = EqFilter("pk1")
    val ppd = new BasicCassandraPredicatePushDown(Set[Filter](f1), table2)
    ppd.predicatesToPushDown should contain only f1
    ppd.predicatesToPreserve shouldBe empty
  }

  it should "push down equality predicates on regular indexed columns" in {
    val f1 = EqFilter("i1")
    val ppd = new BasicCassandraPredicatePushDown(Set[Filter](f1), table)
    ppd.predicatesToPushDown should contain only f1
    ppd.predicatesToPreserve shouldBe empty
  }

  it should "not push down range predicates on regular indexed columns" in {
    val f1 = LtFilter("i1")
    val ppd = new BasicCassandraPredicatePushDown(Set[Filter](f1), table)
    ppd.predicatesToPushDown shouldBe empty
    ppd.predicatesToPreserve should contain only f1
  }

  it should "not push down IN predicates on regular indexed columns" in {
    val f1 = InFilter("i1")
    val ppd = new BasicCassandraPredicatePushDown(Set[Filter](f1), table)
    ppd.predicatesToPushDown shouldBe empty
    ppd.predicatesToPreserve should contain only f1
  }

  it should "push down predicates on regular non-indexed and indexed columns" in {
    val f1 = EqFilter("r1")
    val f2 = EqFilter("r2")
    val f3 = EqFilter("i1")
    val ppd = new BasicCassandraPredicatePushDown(Set[Filter](f1, f2, f3), table)
    ppd.predicatesToPushDown should contain allOf(f1, f2, f3)
    ppd.predicatesToPreserve shouldBe empty
  }

  it should "not push down predicates on regular non-indexed columns if indexed ones are not included" in {
    val f1 = EqFilter("r1")
    val f2 = EqFilter("r2")
    val ppd = new BasicCassandraPredicatePushDown(Set[Filter](f1, f2), table)
    ppd.predicatesToPushDown shouldBe empty
    ppd.predicatesToPreserve should contain allOf(f1, f2)
  }

  it should "prefer to push down equality predicates over range predicates" in {
    val f1 = EqFilter("c1")
    val f2 = EqFilter("c2")
    val f3 = LtFilter("c2")
    val ppd = new BasicCassandraPredicatePushDown(Set[Filter](f1, f2, f3), table)
    ppd.predicatesToPushDown should contain only(f1, f2)
    ppd.predicatesToPreserve should contain only f3
  }

  it should "not push down unsupported predicates" in {
    val f1 = EqFilter("i1")
    val f2 = UnsupportedFilter
    val ppd = new BasicCassandraPredicatePushDown(Set[Filter](f1, f2), table)
    ppd.predicatesToPushDown should contain only f1
    ppd.predicatesToPreserve should contain only f2
  }
}
