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

package io.teknek.arizona.spark.connector.mapper

import _root_.io.teknek.arizona.spark.connector.{ColumnName, UDTValue}
import _root_.io.teknek.arizona.spark.connector.cql.{ColumnDef, PartitionKeyColumn, RegularColumn, TableDef}
import _root_.io.teknek.arizona.spark.connector.types._
import org.apache.commons.lang3.{SerializationUtils, tuple}
import org.scalatest.{FlatSpec, Matchers}

class MappedToGettableDataConverterSpec extends FlatSpec with Matchers {

  val streetColumn = UDTFieldDef("street", VarCharType)
  val numberColumn = UDTFieldDef("number", IntType)
  val addressType = UserDefinedType("address", IndexedSeq(streetColumn, numberColumn))

  val loginColumn = ColumnDef("login", PartitionKeyColumn, VarCharType)
  val passwordColumn = ColumnDef("password", RegularColumn, VarCharType)
  val addressColumn = ColumnDef("address", RegularColumn, addressType)
  val addressesColumn = ColumnDef("addresses", RegularColumn, ListType(addressType, isFrozen = false))

  private def newTable(columns: ColumnDef*): TableDef = {
    val (partitionKeyColumns, other) = columns.partition(_.isPartitionKeyColumn)
    val (clusteringColumns, regularColumns) = other.partition(_.isClusteringColumn)
    new TableDef("test", "test", partitionKeyColumns, clusteringColumns, regularColumns)
  }


  case class User(login: String, password: String)
  case class Address(street: String, number: Int)

  "MappedToGettableDataConverter" should "be Serializable" in {
    val userTable = newTable(loginColumn, passwordColumn)
    val converter = MappedToGettableDataConverter[User](userTable, userTable.columnRefs)
    SerializationUtils.roundtrip(converter)
  }

  it should "convert a simple case class to a CassandraRow" in {
    val userTable = newTable(loginColumn, passwordColumn)
    val converter = MappedToGettableDataConverter[User](userTable, userTable.columnRefs)
    val value = User("login", "password")
    val row = converter.convert(value)
    row.getString("login") shouldEqual "login"
    row.getString("password") shouldEqual "password"
  }

  it should "convert a simple case class to a UDTValue" in {
    val converter = MappedToGettableDataConverter[Address](addressType, addressType.columnRefs)
    val address = Address("foo", 5)
    val udtValue = converter.convert(address)
    udtValue.getString("street") shouldEqual "foo"
    udtValue.getInt("number") shouldEqual 5
  }

  it should "convert a Scala tuple to a TupleValue" in {
    val tupleType = TupleType(TupleFieldDef(0, VarCharType), TupleFieldDef(1, IntType))
    val converter = MappedToGettableDataConverter[(String, Int)](tupleType, tupleType.columnRefs)
    val tuple = ("foo", 5)
    val tupleValue = converter.convert(tuple)
    tupleValue.getString(0) shouldEqual "foo"
    tupleValue.getInt(1) shouldEqual 5
  }

  case class UserWithAddress(login: String, password: String, address: Address)

  it should "convert nested classes" in {
    val userTable = newTable(loginColumn, passwordColumn, addressColumn)
    val converter = MappedToGettableDataConverter[UserWithAddress](userTable, userTable.columnRefs)
    val address = Address("foo", 5)
    val user = UserWithAddress("login", "password", address)
    val row = converter.convert(user)
    row.getString("login") shouldEqual "login"
    row.getString("password") shouldEqual "password"
    row.getUDTValue("address").getString("street") shouldEqual "foo"
    row.getUDTValue("address").getInt("number") shouldEqual 5
  }

  case class UserWithUDTAddress(login: String, address: UDTValue)

  it should "convert a nested UDTValue to a UDTValue" in {
    val userTable = newTable(loginColumn, addressColumn)
    val converter = MappedToGettableDataConverter[UserWithUDTAddress](userTable, userTable.columnRefs)
    val address = UDTValue.fromMap(Map("street" -> "foo", "number" -> "5"))
    val user = UserWithUDTAddress("login", address)
    val row = converter.convert(user)
    row.getString("login") shouldEqual "login"
    row.getUDTValue("address").getString("street") shouldEqual "foo"
    row.getUDTValue("address").getInt("number") shouldEqual 5
  }

  case class UserWithMultipleAddresses(login: String, addresses: Seq[Address])

  it should "convert user defined types nested in collections" in {
    val userTable = newTable(loginColumn, addressesColumn)
    val converter = MappedToGettableDataConverter[UserWithMultipleAddresses](
      userTable, userTable.columnRefs)
    val address1 = Address("foo", 5)
    val address2 = Address("bar", 123)
    val user = UserWithMultipleAddresses("login", Seq(address1, address2))
    val row = converter.convert(user)

    row.getString("login") shouldEqual "login"
    val addresses = row.getList[UDTValue]("addresses")
    addresses should have length 2
    addresses.head.getString("street") shouldEqual "foo"
    addresses.head.getInt("number") shouldEqual 5
    addresses(1).getString("street") shouldEqual "bar"
    addresses(1).getInt("number") shouldEqual 123
  }

  it should "convert user defined types nested in tuples" in {
    val address = Address("foo", 5)
    val tuple = ("bar", address)
    val tupleType = TupleType(TupleFieldDef(0, VarCharType), TupleFieldDef(1, addressType))
    val converter = MappedToGettableDataConverter[(String, Address)](tupleType, tupleType.columnRefs)
    val result = converter.convert(tuple)
    result.getString(0) shouldEqual "bar"
    result.getUDTValue(1).getString(0) shouldBe "foo"
    result.getUDTValue(1).getInt(1) shouldBe 5
  }

  case class AddressWithCompoundNumber(street: String, number: (Int, Int))

  it should "convert tuples nested in user defined types" in {
    val tupleType = TupleType(TupleFieldDef(0, IntType), TupleFieldDef(1, IntType))
    val addressType = UserDefinedType(
      "address", IndexedSeq(UDTFieldDef("street", VarCharType), UDTFieldDef("number", tupleType)))
    val address = AddressWithCompoundNumber("foo", (12, 34))
    val converter = MappedToGettableDataConverter[AddressWithCompoundNumber](
      addressType, addressType.columnRefs)
    val result = converter.convert(address)
    result.getString(0) shouldEqual "foo"
    result.getTupleValue(1).getInt(0) shouldEqual 12
    result.getTupleValue(1).getInt(1) shouldEqual 34
  }

  case class UserWithOption(login: String, password: Option[String])

  it should "convert nulls to Scala Nones" in {
    val obj = UserWithOption("foo", None)
    val userTable = newTable(loginColumn, passwordColumn)
    val converter = MappedToGettableDataConverter[UserWithOption](userTable, userTable.columnRefs)
    val row = converter.convert(obj)
    row.getString(0) shouldEqual "foo"
    row.isNullAt(1) shouldEqual true
  }

  case class UserWithNestedOption(login: String, address: Option[Address])

  it should "convert None case class to null" in {
    val obj = UserWithNestedOption("foo", None)
    val userTable = newTable(loginColumn, addressColumn)
    val converter = MappedToGettableDataConverter[UserWithNestedOption](userTable, userTable.columnRefs)
    val row = converter.convert(obj)
    row.getString(0) shouldEqual "foo"
    row.isNullAt(1) shouldEqual true
  }

  it should "convert Some case class to UDT" in {
    val address = Address("foo", 5)
    val obj = UserWithNestedOption("bar", Some(address))
    val userTable = newTable(loginColumn, addressColumn)
    val converter = MappedToGettableDataConverter[UserWithNestedOption](userTable, userTable.columnRefs)
    val row = converter.convert(obj)
    row.getString(0) shouldEqual "bar"
    row.isNullAt(1) shouldEqual false
    row.getUDTValue("address").getString("street") shouldEqual "foo"
    row.getUDTValue("address").getInt("number") shouldEqual 5
  }

  case class DifferentlyNamedUser(name: String, pass: String)

  it should "convert using custom column aliases" in {
    val user = DifferentlyNamedUser("foo", "bar")
    val userTable = newTable(loginColumn, passwordColumn)
    val converter = MappedToGettableDataConverter[DifferentlyNamedUser](
      userTable, IndexedSeq(ColumnName("login").as("name"), ColumnName("password").as("pass")))
    val result = converter.convert(user)
    result.getString(loginColumn.columnName) shouldBe "foo"
    result.getString(passwordColumn.columnName) shouldBe "bar"
  }

  class UserBean {
    private[this] var login: String = null
    def getLogin: String = login
    def setLogin(login: String): Unit = { this.login = login }

    private[this] var password: String = null
    def getPassword: String = password
    def setPassword(password: String): Unit = { this.password = password }
  }

  it should "convert a java bean to a CassandraRow" in {
    val userTable = newTable(loginColumn, passwordColumn)
    implicit val cm = new JavaBeanColumnMapper[UserBean]
    val converter = MappedToGettableDataConverter[UserBean](userTable, userTable.columnRefs)
    val value = new UserBean()
    value.setLogin("login")
    value.setPassword("password")
    val row = converter.convert(value)
    row.getString("login") shouldEqual "login"
    row.getString("password") shouldEqual "password"
  }

  class AddressBean {
    private[this] var street: String = null
    def getStreet: String = street
    def setStreet(street: String): Unit = { this.street = street }

    private[this] var number: java.lang.Integer = null
    def getNumber: java.lang.Integer = number
    def setNumber(number: java.lang.Integer): Unit = { this.number = number }
  }

  class UserBeanWithAddress {
    private[this] var login: String = null
    def getLogin: String = login
    def setLogin(login: String): Unit = { this.login = login }

    private[this] var address: AddressBean = null
    def getAddress: AddressBean = address
    def setAddress(address: AddressBean) = { this.address = address }
  }

  it should "convert nested JavaBeans" in {
    val userTable = newTable(loginColumn, addressColumn)
    implicit val cm = new JavaBeanColumnMapper[UserBeanWithAddress]
    val converter = MappedToGettableDataConverter[UserBeanWithAddress](userTable, userTable.columnRefs)

    val address = new AddressBean
    address.setStreet("foo")
    address.setNumber(5)
    val user = new UserBeanWithAddress
    user.setLogin("login")
    user.setAddress(address)

    val row = converter.convert(user)
    row.getString("login") shouldEqual "login"
    row.getUDTValue("address").getString("street") shouldEqual "foo"
    row.getUDTValue("address").getInt("number") shouldEqual 5
  }

  case class UserWithTwoAddresses(login: String, addresses: tuple.Pair[Address, Address])

  it should "convert commons-lang3 Pairs to TupleValues" in {
    val pairType = TupleType(
      TupleFieldDef(0, addressType),
      TupleFieldDef(1, addressType))
    val table = newTable(loginColumn, ColumnDef("addresses", RegularColumn, pairType))
    val converter = MappedToGettableDataConverter[UserWithTwoAddresses](table, table.columnRefs)
    val user = UserWithTwoAddresses("foo", tuple.Pair.of(Address("street1", 1), Address("street2", 2)))
    val row = converter.convert(user)
    row.getTupleValue(1).getUDTValue(0).getString("street") shouldEqual "street1"
    row.getTupleValue(1).getUDTValue(0).getInt("number") shouldEqual 1
    row.getTupleValue(1).getUDTValue(1).getString("street") shouldEqual "street2"
    row.getTupleValue(1).getUDTValue(1).getInt("number") shouldEqual 2
  }

  case class UserWithThreeAddresses(login: String, addresses: tuple.Triple[Address, Address, Address])

  it should "convert commons-lang3 Triples to TupleValues" in {
    val tripleType = TupleType(
      TupleFieldDef(0, addressType),
      TupleFieldDef(1, addressType),
      TupleFieldDef(2, addressType))
    val table = newTable(loginColumn, ColumnDef("addresses", RegularColumn, tripleType))
    val converter = MappedToGettableDataConverter[UserWithThreeAddresses](table, table.columnRefs)
    val addresses = tuple.Triple.of(Address("street1", 1), Address("street2", 2), Address("street3", 3))
    val user = UserWithThreeAddresses("foo", addresses)
    val row = converter.convert(user)
    row.getTupleValue(1).getUDTValue(0).getString("street") shouldEqual "street1"
    row.getTupleValue(1).getUDTValue(0).getInt("number") shouldEqual 1
    row.getTupleValue(1).getUDTValue(1).getString("street") shouldEqual "street2"
    row.getTupleValue(1).getUDTValue(1).getInt("number") shouldEqual 2
    row.getTupleValue(1).getUDTValue(2).getString("street") shouldEqual "street3"
    row.getTupleValue(1).getUDTValue(2).getInt("number") shouldEqual 3
  }

  class UnknownType
  case class UserWithUnknownType(login: String, address: UnknownType)

  it should "throw a meaningful exception when a column has an incorrect type" in {
    val userTable = newTable(loginColumn, addressColumn)
    val user = UserWithUnknownType("foo", new UnknownType)
    val exception = the[IllegalArgumentException] thrownBy {
      val converter = MappedToGettableDataConverter[UserWithUnknownType](userTable, userTable.columnRefs)
      converter.convert(user)
    }
    exception.getMessage should include("UserWithUnknownType")
    exception.getMessage should include("UnknownType")
    exception.getMessage should include("address")
    exception.getMessage should include("test")
  }

  it should "throw a meaningful exception when a tuple field has an incorrect number of components" in {
    val tupleType = TupleType(
      TupleFieldDef(0, IntType),
      TupleFieldDef(1, IntType),
      TupleFieldDef(2, IntType))  // Cassandra expects triples...
    val addressType = UserDefinedType(
      "address", IndexedSeq(UDTFieldDef("street", VarCharType), UDTFieldDef("number", tupleType)))

    val exception = the[IllegalArgumentException] thrownBy {
      // ...but the class has only pairs
      val converter = MappedToGettableDataConverter[AddressWithCompoundNumber](
        addressType, addressType.columnRefs)
    }
    exception.getMessage should include("AddressWithCompoundNumber")
    exception.getMessage should include("number")
    exception.getMessage should include("address")
    exception.getCause.getMessage should include("2")
    exception.getCause.getMessage should include("3")
  }

  it should "work after serialization/deserialization" in {
    val userTable = newTable(loginColumn, passwordColumn)
    val converter1 = MappedToGettableDataConverter[User](userTable, userTable.columnRefs)
    val converter2 = SerializationUtils.roundtrip(converter1)
    val value = User("login", "password")
    val row = converter2.convert(value)
    row.getString("login") shouldEqual "login"
    row.getString("password") shouldEqual "password"
  }
}
