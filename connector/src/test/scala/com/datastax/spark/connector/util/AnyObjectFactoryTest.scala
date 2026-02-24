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

import com.datastax.spark.connector._
import com.datastax.spark.connector.testkit.AbstractSpec
import com.datastax.spark.connector.util.JavaApiHelper
import org.apache.commons.lang3.SerializationUtils

import scala.reflect.runtime.universe._

class AnyObjectFactoryTest extends AbstractSpec {

  "AnyObjectFactory" when {
    "instantiated for a bean class with a single, no-args constructor" should {

      val factory = new AnyObjectFactory[SampleJavaBean]

      "create an instance of that class with newInstance" in {
        val instance = factory.newInstance()
        instance shouldBe a[SampleJavaBean]
      }

      "return 0 with argCount" in {
        factory.argCount should be(0)
      }

      "return empty collection with constructorParamTypes" in {
        factory.constructorParamTypes should have size 0
      }

      "return that class with javaClass" in {
        factory.javaClass should be(classOf[SampleJavaBean])
      }
    }

    "instantiated for a bean class with multiple constructors which include no-args constructor" should {
      val factory = new AnyObjectFactory[SampleJavaBeanWithMultipleCtors]

      "create an instance of that class with newInstance" in {
        val instance = newInstance(factory)
        instance shouldBe a[SampleJavaBeanWithMultipleCtors]
      }

      "return that class with javaClass" in {
        factory.javaClass should be(classOf[SampleJavaBeanWithMultipleCtors])
      }
    }

    "instantiated for an inner Java class" should {
      implicit val tt = JavaApiHelper.getTypeTag(classOf[SampleWithNestedJavaBean#InnerClass])
      val factory = new AnyObjectFactory[SampleWithNestedJavaBean#InnerClass]

      "create an instance of that class with newInstance" in {
        val instance = newInstance(factory)
        instance shouldBe a[SampleWithNestedJavaBean#InnerClass]
      }

      "return that class with javaClass" in {
        factory.javaClass should be(classOf[SampleWithNestedJavaBean#InnerClass])
      }
    }

    "instantiated for a deeply nested inner Java class" should {
      implicit val tt = JavaApiHelper.getTypeTag(classOf[SampleWithDeeplyNestedJavaBean#IntermediateClass#InnerClass])
      val factory = new AnyObjectFactory[SampleWithDeeplyNestedJavaBean#IntermediateClass#InnerClass]

      "create an instance of that class with newInstance" in {
        val instance = newInstance(factory)
        instance shouldBe a[SampleWithDeeplyNestedJavaBean#IntermediateClass#InnerClass]
      }

      "return that class with javaClass" in {
        factory.javaClass should be(classOf[SampleWithDeeplyNestedJavaBean#IntermediateClass#InnerClass])
      }
    }

    "tried to be instantiated for an unsupported bean class" should {

      "throw NoSuchMethodException if class does not have suitable constructor" in {
        intercept[NoSuchMethodException] {
          new AnyObjectFactory[SampleJavaBeanWithoutNoArgsCtor]
        }
      }
    }

    "instantiated for a Scala case class with 2 args constructor" should {
      val factory = new AnyObjectFactory[SampleScalaCaseClass]

      "create an instance of that class with newInstance" in {
        val instance = factory.newInstance(1.asInstanceOf[AnyRef], "one".asInstanceOf[AnyRef])
        instance shouldBe a[SampleScalaCaseClass]
        instance.key should be(1)
        instance.value should be("one")
      }

      "return 2 with argCount because the only constructor of this case class has two args" in {
        factory.argCount should be(2)
      }

      "return collection of {Int, String} types with constructorParamTypes" in {
        factory.constructorParamTypes.zip(Array(typeOf[Int], typeOf[String])).foreach {
          case (t1, t2) => (t1 =:= t2) should be(true)
        }
      }

      "return that class with javaClass" in {
        factory.javaClass should be(classOf[SampleScalaCaseClass])
      }
    }

    "instantiated for a Scala case class with 2 args constructor which is defined inside an object" should {
      val factory = new AnyObjectFactory[SampleObject.ClassInObject]

      "create an instance of that class with newInstance" in {
        val instance = factory.newInstance(1.asInstanceOf[AnyRef], "one".asInstanceOf[AnyRef])
        instance shouldBe a[SampleObject.ClassInObject]
        instance.key should be(1)
        instance.value should be("one")
      }

      "return 2 with argCount because the only constructor of this case class has two args" in {
        factory.argCount should be(2)
      }

      "return collection of {Int, String} types with constructorParamTypes" in {
        factory.constructorParamTypes.zip(Array(typeOf[Int], typeOf[String])).foreach {
          case (t1, t2) => (t1 =:= t2) should be(true)
        }
      }

      "return that class with javaClass" in {
        factory.javaClass should be(classOf[SampleObject.ClassInObject])
      }
    }

    "instantiated for a Scala class with 2 args constructor" should {
      val factory = new AnyObjectFactory[SampleScalaClass]

      "create an instance of that class with newInstance" in {
        val instance = factory.newInstance(1.asInstanceOf[AnyRef], "one".asInstanceOf[AnyRef])
        instance shouldBe a[SampleScalaClass]
        instance.key should be(1)
        instance.value should be("one")
      }

      "return 2 with argCount because the only constructor of this class has 2 args" in {
        factory.argCount should be(2)
      }

      "return collection of {Int, String} types with constructorParamTypes" in {
        factory.constructorParamTypes.zip(Array(typeOf[Int], typeOf[String])).foreach {
          case (t1, t2) => (t1 =:= t2) should be(true)
        }
      }

      "return that class with javaClass" in {
        factory.javaClass should be(classOf[SampleScalaClass])
      }
    }

    "instantiated for a Scala class with 2 args constructor and without fields" should {
      val factory = new AnyObjectFactory[SampleScalaClassWithNoFields]

      "create an instance of that class with newInstance" in {
        val instance = factory.newInstance(1.asInstanceOf[AnyRef], "one".asInstanceOf[AnyRef])
        instance shouldBe a[SampleScalaClassWithNoFields]
      }

      "return 2 with argCount because the only constructor of this class has 2 args" in {
        factory.argCount should be(2)
      }

      "return collection of {Int, String} types with constructorParamTypes" in {
        factory.constructorParamTypes.zip(Array(typeOf[Int], typeOf[String])).foreach {
          case (t1, t2) => (t1 =:= t2) should be(true)
        }
      }

      "return that class with javaClass" in {
        factory.javaClass should be(classOf[SampleScalaClassWithNoFields])
      }
    }

    "instantiated for a Scala class with multiple constructors" should {
      val factory = new AnyObjectFactory[SampleScalaClassWithMultipleCtors]

      "create an instance of that class with newInstance" in {
        val instance = factory.newInstance(1.asInstanceOf[AnyRef], "one".asInstanceOf[AnyRef])
        instance shouldBe a[SampleScalaClassWithMultipleCtors]
      }

      "return that class with javaClass" in {
        factory.javaClass should be(classOf[SampleScalaClassWithMultipleCtors])
      }
    }

    "instantiated for an inner Scala class with 2 args constructor" should {
      val factory = new AnyObjectFactory[SampleWithNestedScalaCaseClass#InnerClass]

      "create an instance of that class with newInstance" in {
        val instance = factory.newInstance(1.asInstanceOf[AnyRef], "one".asInstanceOf[AnyRef])
        instance shouldBe a[SampleWithNestedScalaCaseClass#InnerClass]
        instance.key should be(1)
        instance.value should be("one")
      }

      "return 2 with argCount" in {
        factory.argCount should be(2)
      }

      "return collection of {Int, String} types with constructorParamTypes" in {
        factory.constructorParamTypes.zip(Array(typeOf[Int], typeOf[String])).foreach {
          case (t1, t2) => (t1 =:= t2) should be(true)
        }
      }

      "return that class with javaClass" in {
        factory.javaClass should be(classOf[SampleWithNestedScalaCaseClass#InnerClass])
      }
    }

    "instantiated for a deeply nested inner Scala class" should {
      val factory = new AnyObjectFactory[SampleWithDeeplyNestedScalaCaseClass#IntermediateClass#InnerClass]

      "create an instance of that class with newInstance" in {
        val instance = factory.newInstance(1.asInstanceOf[AnyRef], "one".asInstanceOf[AnyRef])
        instance shouldBe a[SampleWithDeeplyNestedScalaCaseClass#IntermediateClass#InnerClass]
      }

      "return that class with javaClass" in {
        factory.javaClass should be(classOf[SampleWithDeeplyNestedScalaCaseClass#IntermediateClass#InnerClass])
      }
    }

    "serialized" should {
      "allow to be deserialized and reused" in {
        val factory = SerializationUtils.roundtrip(new AnyObjectFactory[SampleScalaCaseClass])
        val obj = factory.newInstance(1.asInstanceOf[AnyRef], "one")
        obj should not be (null)
        obj.key should be (1)
        obj.value should be ("one")
      }
    }

  }


  private def newInstance[T](factory: AnyObjectFactory[T]): T = factory.argCount match {
    case 0 =>factory.newInstance()
    case 1 =>factory.newInstance(1.asInstanceOf[AnyRef])
    case 2 =>factory.newInstance(1.asInstanceOf[AnyRef], "one")
  }

}

class TopLevel(val arg1: String, val arg2: Int)
