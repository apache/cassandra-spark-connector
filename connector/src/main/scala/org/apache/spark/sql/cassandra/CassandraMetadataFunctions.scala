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

import com.datastax.spark.connector.datasource.CassandraTable
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression, ExpressionInfo, UnaryExpression, Unevaluable}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types._
import org.apache.spark.sql.{CassandraAnalysisException, SparkSession, functions}

trait CassandraMetadataFunction extends UnaryExpression with Unevaluable {
  def confParam: String

  def cql: String = confParam.toUpperCase

  def isCollectionType(dataType: DataType) : Boolean = {
    dataType match {
      case _: ArrayType => true
      case _: MapType => true
      case _ => false
    }
  }
}

case class CassandraTTL(child: Expression) extends CassandraMetadataFunction {
  override def nullable: Boolean = false

  override def sql: String = s"TTL(${child.sql})"

  override def dataType: DataType = IntegerType

  override def confParam: String = CassandraSourceRelation.TTLParam.name

  override protected def withNewChildInternal(newChild: Expression): CassandraTTL = copy(child = newChild)
}

case class CassandraWriteTime(child: Expression) extends CassandraMetadataFunction {
  override def nullable: Boolean = false

  override def sql: String = s"WRITETIME(${child.sql})"

  override def dataType: DataType = LongType

  override def confParam: String = CassandraSourceRelation.WriteTimeParam.name

  override protected def withNewChildInternal(newChild: Expression): CassandraWriteTime = copy(child = newChild)
}

object CassandraMetadataFunction {

  def registerMetadataFunctions(session: SparkSession): Unit = {
    session.sessionState.functionRegistry.registerFunction(
      FunctionIdentifier("ttl"),
      ttlBuilder,
    "")
    session.sessionState.functionRegistry.registerFunction(
      FunctionIdentifier("writetime"),
      writeTimeBuilder,
    "")
  }

  private val ttlBuilder: Seq[Expression] => CassandraTTL = (input: Seq[Expression]) =>
    CassandraMetadataFunction.cassandraTTLFunctionBuilder(input)

  val cassandraTTLFunctionDescriptor  = (
    FunctionIdentifier("ttl"),
    new ExpressionInfo(getClass.getSimpleName, "ttl"),
    ttlBuilder)

  def cassandraTTLFunctionBuilder(args: Seq[Expression]): CassandraTTL = {
    if (args.length != 1) {
      throw CassandraAnalysisException( s"Unable to call Cassandra ttl with more than 1 argument, given $args")
    }
    CassandraTTL(args.head)
  }

  private val writeTimeBuilder: Seq[Expression] => CassandraWriteTime = (input: Seq[Expression]) =>
    CassandraMetadataFunction.cassandraWriteTimeFunctionBuilder(input)

  val cassandraWriteTimeFunctionDescriptor  = (
    FunctionIdentifier("writetime"),
    new ExpressionInfo(getClass.getSimpleName, "writetime"),
    writeTimeBuilder)

  def cassandraWriteTimeFunctionBuilder(args: Seq[Expression]): CassandraWriteTime = {
    if (args.length != 1) {
      throw CassandraAnalysisException( s"Unable to call Cassandra writetime with more than 1 argument, given $args")
    }
    CassandraWriteTime(args.head)
  }
}

//A Nullable version of Unresolved Attribute to Fix Union's Output checking behavior
class NullableUnresolvedAttribute(name: String) extends UnresolvedAttribute(Seq(name)) {
  override def nullable = true;
}

object CassandraMetaDataRule extends Rule[LogicalPlan] {

  def replaceMetadata(metaDataExpression: CassandraMetadataFunction, plan: LogicalPlan)
  : LogicalPlan = {
    if (!metaDataExpression.child.isInstanceOf[AttributeReference]) {
      // Child not yet resolved, return plan unchanged to allow re-application after resolution
      return plan
    }

    val cassandraColumnName = metaDataExpression.child.asInstanceOf[AttributeReference].name
    val cassandraCql = s"${metaDataExpression.cql}($cassandraColumnName)"

    val (cassandraTable) = plan.collectFirst {
      case DataSourceV2Relation(table: CassandraTable, _, _, _, _)
        if table.tableDef.columnByName.contains(cassandraColumnName) => table }
      .getOrElse(throw CassandraAnalysisException( s"Unable to find Cassandra Source Relation for TTL/Writetime for column $cassandraColumnName"))

    val columnDef = cassandraTable.tableDef.columnByName(cassandraColumnName)

    if (columnDef.isPrimaryKeyColumn)
      throw CassandraAnalysisException( s"Unable to use ${metaDataExpression.cql} function on non-normal column ${columnDef.columnName}")

    //Used for CassandraRelation Leaves, giving them a reference to the underlying Metadata
    val (cassandraAttributeReference, cassandraField) = if (columnDef.isMultiCell) {
      (AttributeReference(cassandraCql, ArrayType(metaDataExpression.dataType), nullable = true)(),
        StructField(cassandraCql, ArrayType(metaDataExpression.dataType), true))
      } else {
      (AttributeReference(cassandraCql, metaDataExpression.dataType, nullable = true)(),
        StructField(cassandraCql, metaDataExpression.dataType, true))
      }

    //Used as a placeholder for everywhere except leaf nodes, to be resolved by the Catalyst Analyzer
    val unResolvedAttributeReference =  new NullableUnresolvedAttribute(cassandraCql)

    //Used for any leaf nodes that do not have the ability to produce a true Metadata Value
    val nullAttributeReference = Alias(
      org.apache.spark.sql.classic.ColumnConversions.expression(
        functions.lit(null).cast(metaDataExpression.dataType)), cassandraCql)()

    // Remove Metadata Expressions
    val metadataFunctionRemovedPlan = plan.transformAllExpressions{
      case expression: Expression if expression == metaDataExpression => unResolvedAttributeReference
    }

    // Add Metadata to CassandraSource
    val cassandraSourceModifiedPlan = metadataFunctionRemovedPlan.transform {
      case cassandraRelation@DataSourceV2Relation(table: CassandraTable, _, _, _, _)
        if table.tableDef.columnByName.contains(cassandraColumnName) =>
        val modifiedCassandraTable = table.copy(optionalSchema = Some(table.schema().add(cassandraField)))
        cassandraRelation.copy(
          modifiedCassandraTable,
          cassandraRelation.output :+ cassandraAttributeReference,
        )
    }

    def containsAnyReferenceToTTL(logicalPlan: LogicalPlan): Boolean ={
      val references = Seq(cassandraAttributeReference, nullAttributeReference, unResolvedAttributeReference)
      val input = logicalPlan.inputSet
      references.exists(input.contains)
    }

    /* Find the leaves of unsatisfied TTL references. Replace them either with a Cassandra TTL attribute
    * or a null if no CassandraTTL is possible for that leaf. All other locations are marked as unresolved
    * for the next pass of the Analyzer */
    val fixedPlan = cassandraSourceModifiedPlan.transformDown{
      case plan if (plan.missingInput.contains(unResolvedAttributeReference)) =>
        plan.mapChildren(_.transformUp {
          case child: Project =>
            if (containsAnyReferenceToTTL(child)) {
              //This node's input contains a value with the Cassandra TTL name, add an unresolved reference to it
              child.copy(child.projectList :+ unResolvedAttributeReference, child.child)
            } else {
              /* This node's input is missing any child reference to the Cassandra TTL we are adding add a null column reference
                 with the same name.
                 This is specifically for graphframes which unions Null References with C* columns
               */
              child.copy(child.projectList :+ nullAttributeReference, child.child)
            }
        })
    }

    fixedPlan
  }

  def findMetadataExpressions(logicalPlan: LogicalPlan): Seq[CassandraMetadataFunction] = {
    def findMetadataExpressions(expressions: Seq[Expression]): Seq[CassandraMetadataFunction] = {
      expressions.collect{
        case metadata: CassandraMetadataFunction => Seq(metadata)
        case parent: Expression => findMetadataExpressions(parent.children)
      }.flatten
    }
    findMetadataExpressions(logicalPlan.expressions)
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transform {
      case planWithMetaData: LogicalPlan if findMetadataExpressions(planWithMetaData).nonEmpty =>
        val metadataExpressions = findMetadataExpressions(planWithMetaData)
        metadataExpressions.foldLeft[LogicalPlan](planWithMetaData) {
          case (plan, expression) => replaceMetadata(expression, plan)
        }
    }
  }
}
