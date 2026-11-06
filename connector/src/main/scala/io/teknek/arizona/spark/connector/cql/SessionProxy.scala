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

package io.teknek.arizona.spark.connector.cql

import java.lang.reflect.{InvocationHandler, InvocationTargetException, Method, Proxy}

import com.datastax.oss.driver.api.core.CqlSession
import _root_.io.teknek.arizona.spark.connector.util.Logging
import org.apache.commons.lang3.ClassUtils

private[cql] trait CloseHandler {
  var closed = false

  def afterClose: CqlSession => Any

  def session: CqlSession

  def onClose(): Any = {
    if (!closed) {
      closed = true
      afterClose(session)
    }
  }
}


/** Wraps a `Session` and intercepts:
  *  - `close` method to invoke `afterClose` handler
  *  - `prepare` methods to cache `PreparedStatement` objects. */
class SessionProxy(val session: CqlSession, val afterClose: CqlSession => Any) extends InvocationHandler with CloseHandler {

  override def invoke(proxy: Any, method: Method, args: Array[AnyRef]) = {
    try {
      (method.getName, method.getParameterTypes) match {
        case ("close", Array()) =>
          onClose()
          null
        case ("closeUnderlying", Array()) =>
          session.close()
          null
        case ("isClosed", Array()) =>
          closed.asInstanceOf[AnyRef]
        case _ =>
          try {
            method.invoke(session, args: _*)
          }
          catch {
            case e: InvocationTargetException =>
              throw e.getCause
          }
      }
    }
    finally {
      if (method.getName == "close") {
        onClose()
      }
    }
  }
}

object SessionProxy extends Logging {

  /** Creates a new `SessionProxy` delegating to the given `Session`.
    * Additionally registers a callback on `Session#close` method.
    *
    * @param afterClose code to be invoked after the session has been closed */
  def wrapWithCloseAction(session: CqlSession)(afterClose: CqlSession => Any): CqlSession = {
    session match {
      case other =>
        val listInterfaces = ClassUtils.getAllInterfaces(session.getClass)
        val availableInterfaces = listInterfaces.toArray[Class[_]](new Array[Class[_]](listInterfaces.size))
        Proxy.newProxyInstance(
          session.getClass.getClassLoader,
          availableInterfaces,
          new SessionProxy(other, afterClose)).asInstanceOf[CqlSession]
    }
  }
}
