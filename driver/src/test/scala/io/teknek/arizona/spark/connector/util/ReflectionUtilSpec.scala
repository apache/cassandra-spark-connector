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

import _root_.io.teknek.arizona.spark.connector.ColumnName
import org.scalatest.concurrent.Conductors
import org.scalatest.{FlatSpec, Matchers}

import scala.reflect.runtime.universe._

trait ReflectionUtilSpecDummyTrait
object ReflectionUtilSpecDummyObject extends ReflectionUtilSpecDummyTrait

class ReflectionUtilSpec extends FlatSpec with Matchers with Conductors {

  "ReflectionUtil.findGlobalObject" should "be able to find a global object" in {
    val obj = ReflectionUtil.findGlobalObject[ReflectionUtilSpecDummyTrait](
      "io.teknek.arizona.spark.connector.util.ReflectionUtilSpecDummyObject")
    obj should be(ReflectionUtilSpecDummyObject)
  }

  it should "be able to find a global object in a multi-threaded context" in {
    val conductor = new Conductor
    import conductor._

    threadNamed("ThreadA") {
      val obj = ReflectionUtil.findGlobalObject[ReflectionUtilSpecDummyTrait](
        "io.teknek.arizona.spark.connector.util.ReflectionUtilSpecDummyObject")
      obj should be(ReflectionUtilSpecDummyObject)
    }
    threadNamed("ThreadB") {
      val obj = ReflectionUtil.findGlobalObject[ReflectionUtilSpecDummyTrait](
        "io.teknek.arizona.spark.connector.util.ReflectionUtilSpecDummyObject")
      obj should be(ReflectionUtilSpecDummyObject)

    }
    conduct()
  }

  it should "be able to instantiate a singleton object based on Java class name" in {
    val obj = ReflectionUtil.findGlobalObject[String]("java.lang.String")
    obj should be ("")
  }

  it should "cache Java class instances" in {
    val obj1 = ReflectionUtil.findGlobalObject[String]("java.lang.String")
    val obj2 = ReflectionUtil.findGlobalObject[String]("java.lang.String")
    obj1 shouldBe theSameInstanceAs (obj2)
  }

  it should "throw IllegalArgumentException when asked for a Scala object of wrong type" in {
    intercept[IllegalArgumentException] {
      ReflectionUtil.findGlobalObject[ColumnName](
        "io.teknek.arizona.spark.connector.cql.DefaultConnectionFactory")
    }
  }

  it should "throw IllegalArgumentException when asked for class instance of wrong type" in {
    intercept[IllegalArgumentException] {
      ReflectionUtil.findGlobalObject[Integer]("java.lang.String")
    }
  }

  it should "throw IllegalArgumentException when object does not exist" in {
    intercept[IllegalArgumentException] {
      ReflectionUtil.findGlobalObject[ColumnName]("NoSuchObject")
    }
  }

  private class ClassWithConstructor(arg1: Int, arg2: List[String])

  "ReflectionUtil.constructorParams" should "return proper constructor param names and types for a class with a single constructor" in {
    val params = ReflectionUtil.constructorParams[ClassWithConstructor]
    params should have size 2
    params(0)._1 should be("arg1")
    params(1)._1 should be("arg2")
    assert(params(0)._2 =:= typeOf[Int])
    assert(params(1)._2 =:= typeOf[List[String]])
  }

  private class ClassWithTwoConstructors(arg1: Int, arg2: List[String]) {
    def this(arg1: Int) = this(arg1, List.empty)
  }

  it should "return main constructor's param names and types for a class with multiple constructors" in {
    val params = ReflectionUtil.constructorParams[ClassWithTwoConstructors]
    params should have size 2
    params(0)._1 should be("arg1")
    params(1)._1 should be("arg2")
    assert(params(0)._2 =:= typeOf[Int])
    assert(params(1)._2 =:= typeOf[List[String]])
  }

  private class ClassWithGetters(val arg1: Int, val arg2: List[String]) {
    def getterLikeMethod: String = null

    def otherMethod(x: String): String = null
  }

  "ReflectionUtil.getters" should "return getter names and types" in {
    val getters = ReflectionUtil.getters[ClassWithGetters].toMap
    getters should have size 2
    getters.keySet should contain ("arg1")
    getters.keySet should contain ("arg2")
    assert(getters("arg1") =:= typeOf[Int])
    assert(getters("arg2") =:= typeOf[List[String]])
  }

  private class ClassWithSetters(var arg1: Int, var arg2: List[String]) {
    def setterLikeMethod_=(x: String): Unit = {}

    def otherMethod(x: String): Unit = {}
  }

  "ReflectionUtil.setters" should "return setter names and types" in {
    val setters = ReflectionUtil.setters[ClassWithSetters].toMap
    setters should have size 2
    setters.keySet should contain ("arg1_$eq")
    setters.keySet should contain ("arg2_$eq")
    assert(setters("arg1_$eq") =:= typeOf[Int])
    assert(setters("arg2_$eq") =:= typeOf[List[String]])
  }

  "ReflectionUtil.methodParamTypes" should "return method param types" in {
    val paramTypes = ReflectionUtil.methodParamTypes(typeOf[ClassWithGetters], "otherMethod")
    paramTypes should have size 1
    assert(paramTypes.head =:= typeOf[String])
  }

  private class GenericClass[T] {
    def genericMethod(param: T): Unit = {}
  }

  it should "return proper method param types for generic type" in {
    val paramTypes = ReflectionUtil.methodParamTypes(typeOf[GenericClass[Int]], "genericMethod")
    paramTypes should have size 1
    assert(paramTypes.head =:= typeOf[Int], "because returned param type should be Int")
  }

  it should "throw IllegalArgumentException if the requested method is missing" in {
    val exception = the [IllegalArgumentException] thrownBy
      ReflectionUtil.methodParamTypes(typeOf[ClassWithGetters], "unknownMethodName")
    exception.getMessage should include ("unknownMethodName")
    exception.getMessage should include ("ClassWithGetters")
  }

}
