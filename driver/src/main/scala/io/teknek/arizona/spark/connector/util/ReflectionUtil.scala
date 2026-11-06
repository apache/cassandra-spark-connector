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

import scala.collection.concurrent.TrieMap
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.util.{Failure, Success, Try}


object ReflectionUtil {
  private val rm = runtimeMirror(getClass.getClassLoader)
  private val singletonCache = TrieMap[String, Any]()

  private def findScalaObject[T : TypeTag](objectName: String): Try[T] = {
    Try {
      val targetType = implicitly[TypeTag[T]].tpe
      val module = rm.staticModule(objectName)
      if (!(module.typeSignature <:< targetType))
        throw new IllegalArgumentException(s"Object $objectName is not instance of $targetType")

      val moduleMirror = rm.reflectModule(module)
      moduleMirror.instance.asInstanceOf[T]
    }
  }

  private def findSingletonClassInstance[T : TypeTag](className: String): Try[T] = {
    Try {
      val targetType = implicitly[TypeTag[T]].tpe
      val targetClass = rm.runtimeClass(targetType.typeSymbol.asClass)
      val instance =
        singletonCache.get(className) match {
          case Some(obj) => obj
          case None =>
            val newInstance = Class.forName(className).getConstructor(Array.empty[Class[_]]: _*).newInstance()
            singletonCache.putIfAbsent(className, newInstance) match {
              case None => newInstance
              case Some(previousInstance) => previousInstance
            }
        }

      if (!targetClass.isInstance(instance))
        throw new IllegalArgumentException(s"Class $className is not $targetType")
      instance.asInstanceOf[T]
    }
  }

  /** Returns either a global Scala object by its fully qualified name or a singleton
    * instance of a Java class identified by its fully qualified class name.
    * Java class instances are cached. The Java class must provide a default constructor. */
  def findGlobalObject[T : TypeTag](objectName: String): T = {
    val scalaObject: Try[T] = findScalaObject[T](objectName)
    val classInstance: Try[T] = findSingletonClassInstance[T](objectName)
    scalaObject orElse classInstance match {
      case Success(obj) => obj
      case Failure(e) => throw new IllegalArgumentException(s"Singleton object not available: $objectName", e)
    }
  }

  /** Returns a list of parameter names and types of the main constructor.
    * The main constructor is assumed to be the one that has the highest number of parameters.
    * In case on ambiguity, this method throws IllegalArgumentException.*/
  def constructorParams(tpe: Type): Seq[(String, Type)] = {
    val ctorSymbol = Reflect.methodSymbol(tpe)
    // the reason we're using typeSignatureIn is because the constructor might be a generic type
    // and we don't really want to get generic type parameters here, but concrete ones:
    val ctorMethod = ctorSymbol.typeSignatureIn(tpe).asInstanceOf[MethodType]
    for (param <- ctorMethod.params) yield
      (param.name.toString, param.typeSignature)
  }

  def constructorParams[T : TypeTag]: Seq[(String, Type)] = {
    constructorParams(implicitly[TypeTag[T]].tpe)
  }

  /** Returns a list of names and return types of 0-argument public methods of a Scala type */
  def getters(tpe: Type): Seq[(String, Type)] = {
    val methods = for (d <- tpe.members.toSeq if d.isMethod && d.isPublic) yield d.asMethod
    for (g <- methods if g.isGetter) yield {
      // the reason we're using typeSignatureIn is because the getter might be a generic type
      // and we don't really want to get generic type here, but a concrete one:
      val returnType = g.typeSignatureIn(tpe).asInstanceOf[NullaryMethodType].resultType
      (g.name.toString, returnType)
    }
  }

  def getters[T : TypeTag]: Seq[(String, Type)] = {
    getters(implicitly[TypeTag[T]].tpe)
  }

  /** Looks up a method by name in a type.
    * The method must exist, otherwise `IllegalArgumentException` is thrown. */
  def method(tpe: Type, methodName: String): Type = {
    require(methodName != null, "Method name must not be null")
    require(methodName.nonEmpty, "Method name must not be empty")
    val member = tpe.member(TermName(methodName))
    require(member != NoSymbol, s"Member $methodName not found in $tpe")
    require(member.isMethod, s"Member $methodName of type $tpe is not a method")
    member.asMethod.typeSignatureIn(tpe)
  }

  /** Returns the type of the parameters of the given method.
    * The method must exist, otherwise `IllegalArgumentException` is thrown. */
  def methodParamTypes(tpe: Type, methodName: String): Seq[Type] = {
    method(tpe, methodName) match {
      case m: MethodType => m.params.map(_.typeSignature)
      case m: NullaryMethodType => Seq.empty
    }
  }

  /** Returns the return type of the method.
    * The method must exist, otherwise `IllegalArgumentException` is thrown. */
  def returnType(tpe: Type, methodName: String): Type = {
    method(tpe, methodName) match {
      case m: MethodType => m.resultType
      case m: NullaryMethodType => m.resultType
    }
  }

  /** Returns a list of names and parameter types of 1-argument public methods of a Scala type,
    * returning no result (Unit) */
  def setters(tpe: Type): Seq[(String, Type)] = {
    val methods = for (d <- tpe.members.toSeq if d.isMethod && d.isPublic) yield d.asMethod
    for (s <- methods if s.isSetter) yield {
      // need a concrete type, not a generic one:
      val paramType = s.typeSignatureIn(tpe).asInstanceOf[MethodType].params.head.typeSignature
      (s.name.toString, paramType)
    }
  }

  def setters[T : TypeTag]: Seq[(String, Type)] = {
    setters(implicitly[TypeTag[T]].tpe)
  }

  /** Creates a corresponding `TypeTag` for the given `Type`.
    * Allows to use reflection-created objects in APIs expecting `TypeTag`. */
  def typeToTypeTag[T](tpe: Type): TypeTag[T] = {
    val mirror = scala.reflect.runtime.currentMirror
    TypeTag(mirror, new reflect.api.TypeCreator {
      def apply[U <: reflect.api.Universe with Singleton](m: reflect.api.Mirror[U]) = {
        assert(m eq mirror, s"TypeTag[$tpe] defined in $mirror cannot be migrated to $m.")
        tpe.asInstanceOf[U#Type]
      }
    })
  }

  /** Creates a corresponding `ClassTag` for the given `TypeTag` */
  def classTag[T : TypeTag]: ClassTag[T] = {
    ClassTag[T](typeTag[T].mirror.runtimeClass(typeTag[T].tpe))
  }

  /** Returns true if the type is scala tuple of any arity */
  def isScalaTuple(symbol: Symbol): Boolean =
    symbol.fullName startsWith "scala.Tuple"

  /** Returns true if the type is scala tuple of any arity */
  def isScalaTuple(tpe: Type): Boolean =
    isScalaTuple(tpe.typeSymbol)

}
