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
import org.scalatest.{Matchers, WordSpec}


class ColumnRefSpec extends WordSpec with Matchers {

  "A FunctionCallRef should" should {

    //Expectations: (generated cql, required columns)
    val valuesAndExpectations: Seq[(FunctionCallRef, (String, Seq[ColumnRef]))] = Seq(
      // 0-arguments function
      FunctionCallRef("f") -> (("f()", Nil)),

      // Literal argument functions
      FunctionCallRef("f", Right("3")::Nil) -> (("f(3)", Nil)),

      //Columns as functions arguments
      FunctionCallRef("f", Left(ColumnName("col01"))::Nil) -> (("""f("col01")""", ColumnName("col01")::Nil)),

      FunctionCallRef("f", Left(ColumnName("col01"))::Left(ColumnName("col02"))::Nil) ->
        (("""f("col01","col02")""", ColumnName("col01")::ColumnName("col02")::Nil)),

      //Mixed columns and literal values as function arguments
      FunctionCallRef("f", Left(ColumnName("col01"))::Right("1")::Right(""""hello"""")::Nil) ->
        (("""f("col01",1,"hello")""", ColumnName("col01")::Nil)),

      //Nested functions
      FunctionCallRef("g",
        Left(ColumnName("col01"))::
          Left(FunctionCallRef("f", Left(ColumnName("col02"))::Right("1")::Right(""""hello"""")::Nil)
          )::Nil
      ) -> (("""g("col01",f("col02",1,"hello"))""", ColumnName("col01")::ColumnName("col02")::Nil))
    )

    //Tests generated CQLs for different function calls
    valuesAndExpectations foreach { case (functionRef, (expectedCql, _)) =>
      s"generate its equivalent CQL code for: $expectedCql" in {
        functionRef.cql shouldEqual expectedCql
      }
    }

    //Tests required columns analysis for each function call
    valuesAndExpectations foreach { case(functionRef, (expectedCql, requiredColumns)) =>
      s"be able to provide a list of columns used as actual parameters by: $expectedCql" in {
        functionRef.requiredColumns shouldEqual requiredColumns
      }
    }

  }

}
