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

package io.teknek.arizona.spark.connector.util

import java.util.UUID
import scala.util.parsing.combinator.RegexParsers

object CqlWhereParser extends RegexParsers with Logging {

  sealed trait RelationalOperator
  case object EqualTo extends RelationalOperator
  case object LowerThan extends RelationalOperator
  case object LowerEqual extends RelationalOperator
  case object GreaterThan extends RelationalOperator
  case object GreaterEqual extends RelationalOperator
  case object In extends RelationalOperator

  sealed trait Value
  case object Placeholder extends Value

  sealed trait Literal extends Value { def value: Any }
  case class StringLiteral(value: String) extends Literal
  case class NumberLiteral(value: String) extends Literal
  case class BooleanLiteral(value: Boolean) extends Literal
  case class UUIDLiteral(value: UUID) extends Literal

  case class ValueList(values: Value*)

  case class Identifier(name: String)

  sealed trait Predicate
  sealed trait SingleColumnPredicate extends Predicate {
    def columnName: String
  }
  case class InPredicate(columnName: String) extends SingleColumnPredicate
  case class InListPredicate(columnName: String, values: ValueList) extends SingleColumnPredicate
  case class EqPredicate(columnName: String, value: Value) extends SingleColumnPredicate
  case class RangePredicate(columnName: String, operator: RelationalOperator, value: Value) extends SingleColumnPredicate
  case class UnknownPredicate(text: String) extends Predicate

  private def unquotedIdentifier = """[_\p{L}][_\p{L}\p{Nd}]*""".r ^^ {
    id => Identifier(id.toLowerCase)
  }

  private def quotedIdentifier = "\"" ~> "(\"\"|[^\"])*".r <~ "\"" ^^ {
    def unEscapeQuotes(s: String) = s.replace("\"\"", "\"")
    id => Identifier(unEscapeQuotes(id.toString))
  }

  private def identifier = unquotedIdentifier | quotedIdentifier

  private def num = """-?\d+(\.\d*)?([eE][-\+]?\d+)?""".r ^^ NumberLiteral.apply

  private def bool = ("true" | "false") ^^ {
    s => BooleanLiteral(s.toBoolean)
  }

  private def str = "'" ~> """(''|[^'])*+""".r <~ "'" ^^ {
    def unEscapeQuotes(s: String) = s.replace("''", "'")
    s => StringLiteral(unEscapeQuotes(s))
  }

  private def uuid = """[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}""".r ^^ {
    s => UUIDLiteral(UUID.fromString(s))
  }

  private def placeholder = "?" ^^ (_ => Placeholder)

  private def value: Parser[Value] = placeholder | uuid | num | bool | str

  private def relationalOperator: Parser[RelationalOperator] =
    "<="       ^^ (_ => LowerEqual) |
    ">="       ^^ (_ => GreaterEqual) |
    "<"        ^^ (_ => LowerThan) |
    ">"        ^^ (_ => GreaterThan) |
    "="        ^^ (_ => EqualTo) |
    "(?i)in".r ^^ (_ => In)

  private def valueList: Parser[ValueList] = "(" ~> value ~ rep("," ~> value) <~ ")" ^^ {
    case literal ~ list => ValueList(literal :: list :_*)
  }

  private def and = "(?i)and".r ^^ {_ => "and" }

  private def predicate: Parser[Predicate] = ((identifier ~ relationalOperator ~ (value | valueList)) | ".*".r) ^^ {
    case Identifier(name) ~ In ~ Placeholder                          => InPredicate(name)
    case Identifier(name) ~ In ~ (list : ValueList)                   => InListPredicate(name, list)
    case Identifier(name) ~ EqualTo ~ (v: Value)                      => EqPredicate(name, v)
    case Identifier(name) ~ (op: RelationalOperator) ~ (param: Value) => RangePredicate(name, op, param)
    case other                                                        => UnknownPredicate(other.toString)
  }

  private def whereClause = predicate ~ rep(and ~> predicate) ^^ {
    case expr ~ list => expr :: list
  }

  def parse(cqlWhere: String): Seq[Predicate] = {
    parseAll(whereClause, cqlWhere) match {
      case Success(columns, _) => columns
      case x => logError("Parse error when parsing CQL WHERE clause:" + x.toString); List()
    }
  }
}

