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

package io.teknek.arizona.spark.connector.types

import org.scalatest.{FlatSpec, Matchers}

class CollectionColumnTypeSpec extends FlatSpec with Matchers {

  val udt = UserDefinedType(
    name = "address",
    columns = IndexedSeq(
      UDTFieldDef("street", VarCharType),
      UDTFieldDef("number", IntType)
    )
  )

  "ListType" should "mark nested UDT types as frozen" in {
    ListType(udt).cqlTypeName shouldBe "list<frozen<address>>"
  }

  it should "not mark non UDT types as frozen" in {
    ListType(IntType).cqlTypeName shouldBe "list<int>"
  }

  "SetType" should "mark nested UDT types as frozen" in {
    SetType(udt).cqlTypeName shouldBe "set<frozen<address>>"
  }

  it should "not mark non UDT types as frozen" in {
    SetType(IntType).cqlTypeName shouldBe "set<int>"
  }

  "MapType" should "mark key UDT types as frozen" in {
    MapType(udt, IntType).cqlTypeName shouldBe "map<frozen<address>, int>"
  }

  it should "mark value UDT types as frozen" in {
    MapType(IntType, udt).cqlTypeName shouldBe "map<int, frozen<address>>"
  }

}
