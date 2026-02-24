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

package org.apache.spark.sql.cassandra.execution

import com.datastax.spark.connector.datasource.{CassandraScan, CassandraScanBuilder, CassandraTable}
import com.datastax.spark.connector.util.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra.{AlwaysOff, AlwaysOn, Automatic, CassandraSourceRelation}
import org.apache.spark.sql.cassandra.CassandraSourceRelation._
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, ExprId, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.planning.{ExtractEquiJoinKeys, PhysicalOperation}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceV2Relation, DataSourceV2ScanRelation, DataSourceV2Strategy}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan, SparkStrategy}


/**
  * Direct Join Strategy
  * Converts logical plans where the join target is a Cassandra derived branch with joinWithCassandraTable
  * style Join
  */
case class CassandraDirectJoinStrategy(spark: SparkSession) extends SparkStrategy with Serializable {
  import CassandraDirectJoinStrategy._

  val conf = spark.sessionState.conf

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, _, left, right, _)
      if hasValidDirectJoin(joinType, leftKeys, rightKeys, condition, left, right) =>

      val (otherBranch, joinTargetBranch, buildType) = {
        if (leftValid(joinType, leftKeys, rightKeys, condition, left, right)) {
          (right, left, BuildLeft)
        } else {
          (left, right, BuildRight)
        }
      }

      /* We want to take advantage of all of our pushed filter code which happens in
         full table scans. Unfortunately the pushdown code itself is private within the
         DataSourceV2Strategy class. To work around this we will invoke DataSourceV2Strategy on
         our target branch. This will let us know all of the pushable filters that we can
         use in the direct join.
      */
      val dataSourceOptimizedPlan = new DataSourceV2Strategy(spark.asInstanceOf[org.apache.spark.sql.classic.SparkSession])(joinTargetBranch).head
      val cassandraScanExec = getScanExec(dataSourceOptimizedPlan).get

      joinTargetBranch match {
        case PhysicalOperation(attributes, _, DataSourceV2ScanRelation(DataSourceV2Relation(_: CassandraTable, _, _, _, _), _, _, _, _)) =>
          val directJoin =
            CassandraDirectJoinExec(
              leftKeys,
              rightKeys,
              joinType,
              buildType,
              condition,
              planLater(otherBranch),
              aliasMap(attributes),
              cassandraScanExec
            )

          val newPlan = reorderPlan(dataSourceOptimizedPlan, directJoin, plan.output) :: Nil
          val newOutput = (newPlan.head.outputSet, newPlan.head.output.map(_.name))
          val oldOutput = (plan.outputSet, plan.output.map(_.name))
          val noMissingOutput = oldOutput._1.subsetOf(newPlan.head.outputSet)
          require(noMissingOutput, s"Cassandra DirectJoin Optimization produced invalid output. Original plan output: " +
            s"${oldOutput} was not part of ${newOutput} \nOld Plan\n${plan}\nNew Plan\n${newPlan}")

          newPlan
        case _ => Nil //Unable to do optimization on target branch
      }
    case _ => Nil //No valid Target for Join Optimization
  }

  def hasValidDirectJoin(
    joinType: JoinType,
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    condition: Option[Expression],
    left: LogicalPlan,
    right: LogicalPlan): Boolean = {
    leftValid(joinType, leftKeys, rightKeys, condition, left, right) ||
        rightValid(joinType, leftKeys, rightKeys, condition, left, right)
  }


  /**
    * Checks whether the size of the cassandra target is larger than the
    * key plan by the sizeRatio specified in the configuration.
    */
  private def checkSizeRatio(cassandraPlan: LogicalPlan, keyPlan: LogicalPlan): Boolean = {
    val cassandraScan = getCassandraScan(cassandraPlan).get
    CassandraSourceRelation.getDirectJoinSetting(cassandraScan.consolidatedConf) match {
      case AlwaysOn =>
        logDebug("No Size Test for Direct Join. Direct Join is Always On")
        true
      case AlwaysOff =>
        logDebug("Direct Join is Disabled")
        false
      case Automatic =>
        val ratio = BigDecimal(
          cassandraScan.consolidatedConf
            .get(DirectJoinSizeRatioParam.name, DirectJoinSizeRatioParam.default.toString))

        val cassandraSize = BigDecimal(cassandraPlan.stats.sizeInBytes)
        val keySize = BigDecimal(keyPlan.stats.sizeInBytes.doubleValue)

        logDebug(s"Checking if size ratio is good: $cassandraSize * $ratio > $keySize")

        if ((cassandraSize * ratio) > keySize) {
          true
        } else {
          false
        }
      }
  }

  def leftValid (joinType: JoinType,
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    condition: Option[Expression],
    left: LogicalPlan,
    right: LogicalPlan): Boolean = (
    validJoinBranch(left, leftKeys)
      && validJoinType(BuildLeft, joinType)
      && checkSizeRatio(left, right)
    )

  def rightValid (joinType: JoinType,
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    condition: Option[Expression],
    left: LogicalPlan,
    right: LogicalPlan): Boolean = (
    validJoinBranch(right, rightKeys)
      && validJoinType(BuildRight, joinType)
      && checkSizeRatio(right, left)
  )

}

object CassandraDirectJoinStrategy extends Logging {

  /**
    * Returns the single DataSourceScanExec for the branch if there is one and
    * it scans Cassandra
    */
  def getScanExec(plan: SparkPlan): Option[BatchScanExec] = {
    plan.collectFirst {
      case exec @ BatchScanExec(_, _: CassandraScan, _, _, _, _) => exec
    }
  }

  /**
    * Currently we will only support Inner, LeftOuter and RightOuter joins
    * Depending on the side of the CassandraTarget different joins are allowed.
    */
  val validJoins: Map[BuildSide, Seq[JoinType]] = Map(
    BuildRight -> Seq[JoinType](Inner, LeftOuter),
    BuildLeft -> Seq[JoinType](Inner, RightOuter)
  )

  def validJoinType(cassandraSide: BuildSide, joinType: JoinType): Boolean =
    validJoins(cassandraSide).contains(joinType)

  /**
    * Checks whether there is only a single leaf to this plan and that the
    * leaf is a DSV2 Relation Reading from a C* table. If it is returns that table.
    */
  def getDSV2CassandraRelation(plan: LogicalPlan): Option[DataSourceV2ScanRelation] = {
    val children = plan.collectLeaves()
    if (children.length == 1) {
      plan.collectLeaves().collectFirst { case ds @ DataSourceV2ScanRelation(DataSourceV2Relation(_: CassandraTable, _, _, _, _), _, _, _, _) => ds }
    } else {
      None
    }
  }

  /**
    * Checks whether there is only a single leaf to this plan and that the
    * leaf is a CassandraTable. If it is returns that table.
    */
  def getCassandraTable(plan: LogicalPlan): Option[CassandraTable] = {
    val children = plan.collectLeaves()
    if (children.length == 1) {
      children.collectFirst { case DataSourceV2ScanRelation(DataSourceV2Relation(table: CassandraTable, _, _, _, _), _, _, _, _) => table }
    } else {
      None
    }
  }

  def getCassandraScan(plan: LogicalPlan): Option[CassandraScan] = {
    val children = plan.collectLeaves()
    if (children.length == 1) {
      plan.collectLeaves().collectFirst { case DataSourceV2ScanRelation(_: DataSourceV2Relation, cs: CassandraScan, _, _, _) => cs }
    } else {
      None
    }
  }


  /**
    * Checks whether a query plan has either a logical or physical node pulling data from cassandra
    */
  def hasCassandraChild[T <: QueryPlan[T]](plan: T): Boolean = {
    plan.children.size == 1 && plan.children.exists {
      case DataSourceV2ScanRelation(DataSourceV2Relation(_: CassandraTable, _, _, _, _), _, _, _, _) => true
      case BatchScanExec(_, _: CassandraScan, _, _, _, _) => true
      case _ => false
    }
  }

  /**
    * Given our target Cassandra based branch, we remove the node which draws data
    * from cassandra (DataSourceScanExec) and replace it with the passed directJoin plan
    * instead.
    *
    * INPUT :
    *
    * //directJoin: DirectJoinPlan//
    * DirectJoin <-- GetKeyOperations
    *
    * //plan: Optimized CassandraScanBranch/
    * OtherOperations <-- CassandraScan
    *
    * OUTPUT:
    *
    * OtherOperations <-- DirectJoin <-- GetKeyOperations
    *
    * The output of (plan) is not changed, but its children should be
    * changed.
    *
    * This should only be called on optimized Physical Plans
    */
  def reorderPlan(
      plan: SparkPlan,
      directJoin: CassandraDirectJoinExec,
      originalOutput: Seq[Attribute]): SparkPlan = {
    val reordered = plan match {
      //This may be the only node in the Plan
      case BatchScanExec(_, _: CassandraScan, _, _, _, _) => directJoin
      // Plan has children
      case normalPlan => normalPlan.transform {
        case penultimate if hasCassandraChild(penultimate) =>
          penultimate.withNewChildren(Seq(directJoin))
      }
    }

    /*
    The output of our new join node may be missing some aliases which were
    previously applied to columns coming out of cassandra. Take the directJoin output and
    make sure all aliases are correctly applied to the new attributes. Nullability is a
    concern here as columns which may have been non-nullable previously, become nullable in
    a left/right join
    */
    reordered.transform {
      case ProjectExec(projectList, child) =>
        val attrMap = directJoin.output.map {
          case attr => attr.exprId -> attr
        }.toMap

        val aliases = projectList.collect {
          case a @ Alias(child, _) =>
            val newAliasChild = child.transform {
              case attr: Attribute => attrMap.getOrElse(attr.exprId, attr)
            }
            (a.exprId, a.withNewChildren(newAliasChild :: Nil).asInstanceOf[Alias])
        }.toMap

        // The original output of Join
        val reorderedOutput = originalOutput.map {
          case attr if aliases.contains(attr.exprId) => aliases(attr.exprId)
          case other => other
        }

        ProjectExec(reorderedOutput, child)
    }
  }

  /**
    * Checks whether the Plan contains only acceptable logical nodes and all
    * partition keys are joined
    */
  def validJoinBranch(plan: LogicalPlan, keys: Seq[Expression]): Boolean = {
    val safePlan = containsSafePlans(plan)
    val pkConstrained = allPartitionKeysAreJoined(plan, keys)
    if (containsSafePlans(plan)) {
      logDebug(s"Plan was safe")
    }
    if (pkConstrained) {
      logDebug(s"Plan constrained on all partition keys")
    }

    safePlan && pkConstrained
  }

  /**
    * Every partition key column must be a join key
    */
  def allPartitionKeysAreJoined(plan: LogicalPlan, joinKeys: Seq[Expression]): Boolean =
    plan match {
      case PhysicalOperation(
        attributes, _,
        DataSourceV2ScanRelation(DataSourceV2Relation(cassandraTable: CassandraTable, _, _, _, _), _, _, _, _)) =>

        val joinKeysExprId = joinKeys.collect{ case attributeReference: AttributeReference => attributeReference.exprId }

        val joinKeyAliases =
          aliasMap(attributes)
            .filter{ case (_, value) => joinKeysExprId.contains(value) }
        val partitionKeyNames = cassandraTable.tableDef.partitionKey.map(_.columnName)
        val allKeysPresent = partitionKeyNames.forall(joinKeyAliases.contains)

        if (!allKeysPresent) {
          logDebug(s"Not all $partitionKeyNames should be contained within $joinKeyAliases")
        }

        allKeysPresent
    case _ => false
  }

  def getAlias(expr: NamedExpression): Seq[(String, ExprId)] = expr match {
    case a @ Alias(child: AttributeReference, _) => Seq(child.name -> a.exprId)
    case a @ Alias(child, _) =>
      val attrs = child.collect {
        case attr: AttributeReference => attr
      }
      // There might be more than one attribute in the aliased expression.
      // For example, `named_struct(x, y, z) AS alias1`
      attrs.map(attr => attr.name -> a.exprId)
    case attributeReference: AttributeReference => Seq(attributeReference.name -> attributeReference.exprId)
  }

  /**
    * Map Source Cassandra Column Names to ExpressionIds referring to them
    */
  def aliasMap(aliases: Seq[NamedExpression]): Map[String, ExprId] = aliases.flatMap(getAlias).toMap

  /**
  * Checks whether a logical plan contains only Filters, Aliases
  * and CassandraSource Relations
  */
  def containsSafePlans(plan: LogicalPlan): Boolean = {
    plan match {
      case PhysicalOperation(_, _, DataSourceV2ScanRelation(DataSourceV2Relation(_: CassandraTable, _, _, _, _), scan: CassandraScan, _, _, _))
        if getDirectJoinSetting(scan.consolidatedConf) != AlwaysOff => true
      case _ => false
    }
  }
}
