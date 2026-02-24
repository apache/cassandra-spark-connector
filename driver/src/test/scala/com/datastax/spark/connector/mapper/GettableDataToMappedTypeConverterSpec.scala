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

package com.datastax.spark.connector.mapper

import org.apache.commons.lang3.SerializationUtils
import org.scalatest.{FlatSpec, Matchers}
import com.datastax.spark.connector._
import com.datastax.spark.connector.{CassandraRow, UDTValue}
import com.datastax.spark.connector.cql.{ColumnDef, PartitionKeyColumn, RegularColumn, TableDef}
import com.datastax.spark.connector.types._


class GettableDataToMappedTypeConverterSpec extends FlatSpec with Matchers {

  val streetColumn = UDTFieldDef("street", VarCharType)
  val numberColumn = UDTFieldDef("number", IntType)
  val addressType = UserDefinedType("address", IndexedSeq(streetColumn, numberColumn))

  val loginColumn = ColumnDef("login", PartitionKeyColumn, VarCharType)
  val passwordColumn = ColumnDef("password", RegularColumn, VarCharType)
  val addressColumn = ColumnDef("address", RegularColumn, addressType)
  val addressesColumn = ColumnDef("addresses", RegularColumn, addressType)

  val userTable = new TableDef(
    keyspaceName = "test",
    tableName = "test",
    partitionKey = Seq(loginColumn),
    clusteringColumns = Seq.empty,
    regularColumns = Seq(passwordColumn, addressColumn))

  "GettableDataToMappedTypeConverter" should "be Serializable" in {
    val converter = new GettableDataToMappedTypeConverter[User](userTable, userTable.columnRefs)
    SerializationUtils.roundtrip(converter)
  }

  case class User(login: String, password: String)

  it should "convert a CassandraRow to a case class object" in {
    val row = CassandraRow.fromMap(Map("login" -> "foo", "password" -> "bar"))
    val converter = new GettableDataToMappedTypeConverter[User](userTable, userTable.columnRefs)
    val user = converter.convert(row)
    user.login shouldBe "foo"
    user.password shouldBe "bar"
  }

  it should "convert a CassandraRow to a case class object after being serialized/deserialized" in {
    val row = CassandraRow.fromMap(Map("login" -> "foo", "password" -> "bar"))
    val converter =
      SerializationUtils.roundtrip(
        new GettableDataToMappedTypeConverter[User](userTable, userTable.columnRefs))
    val user = converter.convert(row)
    user.login shouldBe "foo"
    user.password shouldBe "bar"
  }

  it should "convert a CassandraRow to a tuple" in {
    val row = CassandraRow.fromMap(Map("login" -> "foo", "password" -> "bar"))
    val converter =
      new GettableDataToMappedTypeConverter[(String, String)](userTable, IndexedSeq("login", "password").map(ColumnName(_)))
    val user = converter.convert(row)
    user._1 shouldBe "foo"
    user._2 shouldBe "bar"
  }

  it should "convert a CassandraRow to a tuple in reversed order" in {
    val row = CassandraRow.fromMap(Map("login" -> "foo", "password" -> "bar"))
    val converter =
      new GettableDataToMappedTypeConverter[(String, String)](userTable, IndexedSeq("password", "login").map(ColumnName(_)))
    val user = converter.convert(row)
    user._1 shouldBe "bar"
    user._2 shouldBe "foo"
  }

  it should "convert a CassandraRow to a tuple with a subset of columns" in {
    val row = CassandraRow.fromMap(Map("password" -> "bar"))
    val converter =
      new GettableDataToMappedTypeConverter[Tuple1[String]](userTable, IndexedSeq("password").map(ColumnName(_)))
    val user = converter.convert(row)
    user._1 shouldBe "bar"
  }

  it should "convert a UDTValue to a case class object" in {
    val row = UDTValue.fromMap(Map("login" -> "foo", "password" -> "bar"))
    val converter = new GettableDataToMappedTypeConverter[User](userTable, userTable.columnRefs)
    val user = converter.convert(row)
    user.login shouldBe "foo"
    user.password shouldBe "bar"
  }

  it should "convert a TupleValue to a Scala tuple" in {
    val tupleValue = TupleValue("foo", "bar")
    val tupleType = TupleType(TupleFieldDef(0, VarCharType), TupleFieldDef(1, VarCharType))
    val converter = new GettableDataToMappedTypeConverter[(String, String)](tupleType, tupleType.columnRefs)
    val tuple = converter.convert(tupleValue)
    tuple._1 shouldBe "foo"
    tuple._2 shouldBe "bar"
  }

  it should "allow for nesting UDTValues inside of TupleValues" in {
    val addressValue = UDTValue.fromMap(Map("street" -> "foo", "number" -> 10))
    val tupleValue = TupleValue("baz", addressValue)
    val tupleType = TupleType(TupleFieldDef(0, VarCharType), TupleFieldDef(1, addressType))
    val converter = new GettableDataToMappedTypeConverter[(String, Address)](tupleType, tupleType.columnRefs)
    val tuple = converter.convert(tupleValue)
    tuple._1 shouldBe "baz"
    tuple._2.street shouldBe "foo"
    tuple._2.number shouldBe 10
  }

  case class AddressWithCompoundNumber(street: String, number: (Int, Int))

  it should "allow for nesting TupleValues inside of UDTValues" in {
    val tupleType = TupleType(TupleFieldDef(0, IntType), TupleFieldDef(1, IntType))
    val numberColumn = UDTFieldDef("number", tupleType)
    val addressType = UserDefinedType("address", IndexedSeq(streetColumn, numberColumn))
    val addressValue = UDTValue.fromMap(Map("street" -> "foo", "number" -> TupleValue(5, 10)))
    val converter = new GettableDataToMappedTypeConverter[AddressWithCompoundNumber](addressType, addressType.columnRefs)
    val address = converter.convert(addressValue)
    address.street shouldBe "foo"
    address.number._1 shouldBe 5
    address.number._2 shouldBe 10
  }

  case class UserWithOption(login: String, password: Option[String])

  it should "convert nulls to Scala Nones" in {
    val row = CassandraRow.fromMap(Map("login" -> "foo", "password" -> null))
    val converter =
      new GettableDataToMappedTypeConverter[UserWithOption](
        userTable, userTable.columnRefs)

    val user = converter.convert(row)
    user.login shouldBe "foo"
    user.password shouldBe None
  }

  case class DifferentlyNamedUser(name: String, pass: String)

  it should "convert using custom column aliases" in {
    val row = CassandraRow.fromMap(Map("login" -> "foo", "password" -> "bar"))
    val converter =
      new GettableDataToMappedTypeConverter[DifferentlyNamedUser](
        userTable,
        IndexedSeq(ColumnName("login").as("name") , ColumnName("password").as("pass")))

    val user = converter.convert(row)
    user.name shouldBe "foo"
    user.pass shouldBe "bar"
  }

  class UserWithSetters {
    var login: String = null
    var password: String = null
  }

  it should "set property values with setters" in {
    val row = CassandraRow.fromMap(Map("login" -> "foo", "password" -> "bar"))
    val converter = new GettableDataToMappedTypeConverter[UserWithSetters](userTable, userTable.columnRefs)
    val user = converter.convert(row)
    user.login shouldBe "foo"
    user.password shouldBe "bar"
  }

  case class Address(street: String, number: Int)

  it should "apply proper type conversions for columns" in {
    val address = UDTValue.fromMap(Map("street" -> "street", "number" -> "5"))  // 5 as String
    val converter = new GettableDataToMappedTypeConverter[Address](addressType, addressType.columnRefs)
    val addressObj = converter.convert(address)
    addressObj.number shouldBe 5
  }

  case class UserWithAddress(login: String, password: String, address: Address)

  it should "convert a CassandraRow with a UDTValue into nested case class objects" in {
    val address = UDTValue.fromMap(Map("street" -> "street", "number" -> 5))
    val row = CassandraRow.fromMap(Map("login" -> "foo", "password" -> "bar", "address" -> address))
    val converter = new GettableDataToMappedTypeConverter[UserWithAddress](
      userTable, userTable.columnRefs)
    val user = converter.convert(row)
    user.login shouldBe "foo"
    user.password shouldBe "bar"
    user.address.street shouldBe "street"
    user.address.number shouldBe 5
  }

  case class UserWithAddressAsTuple(login: String, password: String, address: (String, Int))

  it should "convert a CassandraRow with a UDTValue into a case class with a nested tuple" in {
    val address = UDTValue.fromMap(Map("street" -> "street", "number" -> 5))
    val row = CassandraRow.fromMap(Map("login" -> "foo", "password" -> "bar", "address" -> address))
    val converter = new GettableDataToMappedTypeConverter[UserWithAddressAsTuple](
      userTable, userTable.columnRefs)
    val user = converter.convert(row)
    user.login shouldBe "foo"
    user.password shouldBe "bar"
    user.address._1 shouldBe "street"
    user.address._2 shouldBe 5
  }

  case class UserWithOptionalAddress(login: String, password: String, address: Option[Address])

  it should "convert a CassandraRow with an optional UDTValue" in {
    val address = UDTValue.fromMap(Map("street" -> "street", "number" -> 5))
    val row = CassandraRow.fromMap(Map("login" -> "foo", "password" -> "bar", "address" -> address))
    val converter = new GettableDataToMappedTypeConverter[UserWithOptionalAddress](
      userTable, userTable.columnRefs)
    val user = converter.convert(row)
    user.login shouldBe "foo"
    user.password shouldBe "bar"
    user.address.isDefined shouldBe true
    user.address.get.street shouldBe "street"
    user.address.get.number shouldBe 5
  }

  case class UserWithMultipleAddresses(login: String, addresses: Vector[Address])

  def testUserWithMultipleAddresses(addressesType: ColumnType[_]): Unit = {
    val addressesColumn = ColumnDef("addresses", RegularColumn, addressesType)
    val userTable = new TableDef(
      keyspaceName = "test",
      tableName = "test",
      partitionKey = Seq(loginColumn),
      clusteringColumns = Seq.empty,
      regularColumns = Seq(addressesColumn))

    val address1 = UDTValue.fromMap(Map("street" -> "A street", "number" -> 4))
    val address2 = UDTValue.fromMap(Map("street" -> "B street", "number" -> 7))
    val row = CassandraRow.fromMap(Map("login" -> "foo", "addresses" -> List(address1, address2)))
    val converter = new GettableDataToMappedTypeConverter[UserWithMultipleAddresses](
      userTable, userTable.columnRefs)
    val user = converter.convert(row)
    user.login shouldBe "foo"
    user.addresses(0).street shouldBe "A street"
    user.addresses(0).number shouldBe 4
    user.addresses(1).street shouldBe "B street"
    user.addresses(1).number shouldBe 7
  }

  it should "convert a CassandraRow with a list of UDTValues" in {
    testUserWithMultipleAddresses(ListType(addressType))
  }

  it should "convert a CassandraRow with a set of UDTValues" in {
    testUserWithMultipleAddresses(SetType(addressType))
  }

  case class UserWithMultipleAddressesAsMap(login: String, addresses: Map[Int, Address])

  it should "convert a CassandraRow with a collection of UDTValues" in {
    val addressesColumn = ColumnDef("addresses", RegularColumn, MapType(IntType, addressType))
    val userTable = new TableDef(
      keyspaceName = "test",
      tableName = "test",
      partitionKey = Seq(loginColumn),
      clusteringColumns = Seq.empty,
      regularColumns = Seq(addressesColumn))

    val address1 = UDTValue.fromMap(Map("street" -> "A street", "number" -> 4))
    val address2 = UDTValue.fromMap(Map("street" -> "B street", "number" -> 7))
    val addresses = Map(0 -> address1, 1 -> address2)
    val row = CassandraRow.fromMap(Map("login" -> "foo", "addresses" -> addresses))
    val converter = new GettableDataToMappedTypeConverter[UserWithMultipleAddressesAsMap](
      userTable, userTable.columnRefs)
    val user = converter.convert(row)
    user.login shouldBe "foo"
    user.addresses(0).street shouldBe "A street"
    user.addresses(0).number shouldBe 4
    user.addresses(1).street shouldBe "B street"
    user.addresses(1).number shouldBe 7
  }

  case class UserWithMultipleAddressesAsPairs(login: String, addresses: List[(String, Int)])

  it should "convert a CassandraRow with a collection of tuples" in {
    val tupleType = TupleType(TupleFieldDef(0, VarCharType), TupleFieldDef(1, IntType))
    val addressesColumn = ColumnDef("addresses", RegularColumn, ListType(tupleType))
    val userTable = new TableDef(
      keyspaceName = "test",
      tableName = "test",
      partitionKey = Seq(loginColumn),
      clusteringColumns = Seq.empty,
      regularColumns = Seq(addressesColumn))

    val address1 = TupleValue("A street", 4)
    val address2 = TupleValue("B street", 7)
    val addresses = List(address1, address2)
    val row = CassandraRow.fromMap(Map("login" -> "foo", "addresses" -> addresses))
    val converter = new GettableDataToMappedTypeConverter[UserWithMultipleAddressesAsPairs](
      userTable, userTable.columnRefs)
    val user = converter.convert(row)
    user.login shouldBe "foo"
    user.addresses(0)._1 shouldBe "A street"
    user.addresses(0)._2 shouldBe 4
    user.addresses(1)._1 shouldBe "B street"
    user.addresses(1)._2 shouldBe 7
  }

  class UserBean {
    private[this] var login: String = null
    def getLogin: String = login
    def setLogin(login: String): Unit = { this.login = login }

    private[this] var password: String = null
    def getPassword: String = password
    def setPassword(password: String): Unit = { this.password = password }
  }

  it should "convert a CassandraRow to a JavaBean" in {
    implicit val cm: ColumnMapper[UserBean] = new JavaBeanColumnMapper[UserBean]
    val row = CassandraRow.fromMap(Map("login" -> "foo", "password" -> "bar"))
    val converter = new GettableDataToMappedTypeConverter[UserBean](userTable, userTable.columnRefs)
    val user = converter.convert(row)
    user.getLogin shouldBe "foo"
    user.getPassword shouldBe "bar"
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

  it should "convert a CassandraRow with UDTs to nested JavaBeans" in {
    implicit val cm: ColumnMapper[UserBeanWithAddress] = new JavaBeanColumnMapper[UserBeanWithAddress]
    val address = UDTValue.fromMap(Map("street" -> "street", "number" -> 5))
    val row = CassandraRow.fromMap(Map("login" -> "foo", "address" -> address))
    val converter = new GettableDataToMappedTypeConverter[UserBeanWithAddress](
      userTable, userTable.columnRefs)
    val user = converter.convert(row)
    user.getLogin shouldBe "foo"
    user.getAddress.getStreet shouldBe "street"
    user.getAddress.getNumber shouldBe 5
  }

  class LongBean {
       private[this] var number: java.lang.Long = null
       def getNumber: java.lang.Long = number
       def setNumber(number: java.lang.Long): Unit = { this.number = number }
  }

  class NestedLongBean {
       private[this] var long: LongBean = null
       def getLong: LongBean = long
       def setLong(l: LongBean) = { this.long = long }
  }

  it should "convert a CassandraRow with UDTs with Long null values to nested JavaBeans" in {
       implicit val cm: ColumnMapper[NestedLongBean] = new JavaBeanColumnMapper[NestedLongBean]
       val long = UDTValue.fromMap(Map("number" -> null))
       val row = CassandraRow.fromMap(Map("long" -> long))
       val converter =
         new GettableDataToMappedTypeConverter[NestedLongBean](userTable, userTable.columnRefs)
       val user = converter.convert(row)
       user.getLong shouldBe null
  }

  class UnknownType
  case class UserWithUnknownType(login: String, password: UnknownType)

  it should "throw a meaningful exception when a column type is not supported" in {
    val row = CassandraRow.fromMap(Map("login" -> "foo", "password" -> "bar"))
    val exception = the[IllegalArgumentException] thrownBy {
      val converter = new GettableDataToMappedTypeConverter[UserWithUnknownType](
        userTable, userTable.columnRefs)
      converter.convert(row)
    }
    exception.getMessage should include("UnknownType")
  }

  it should "throw a meaningful exception when a column value fails to be converted" in {
    val address = UDTValue.fromMap(Map("street" -> "street", "number" -> "incorrect_number"))
    val exception = the[TypeConversionException] thrownBy {
      val converter = new GettableDataToMappedTypeConverter[Address](
        addressType, addressType.columnRefs)
      converter.convert(address)

    }
    exception.getMessage should include("address")
    exception.getMessage should include("number")
    exception.getMessage should include("incorrect_number")
    exception.getMessage should include("Int")
  }

  it should "throw NPE with a meaningful message when a column value is null" in {
    val address = UDTValue.fromMap(Map("street" -> null, "number" -> "5"))
    val exception = the[NullPointerException] thrownBy {
      val converter = new GettableDataToMappedTypeConverter[Address](
        addressType, addressType.columnRefs)
      converter.convert(address)
    }
    exception.getMessage should include("address")
    exception.getMessage should include("street")
  }

  it should "throw NPE when trying to access its targetTypeTag after serialization/deserialization" in {
    val converter = new GettableDataToMappedTypeConverter[User](userTable, userTable.columnRefs)
    val deserialized = SerializationUtils.roundtrip(converter)
    a [NullPointerException] should be thrownBy deserialized.targetTypeTag
  }
}