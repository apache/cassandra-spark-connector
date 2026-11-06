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

package io.teknek.arizona.spark.connector.writer

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.AsyncResultSet
import _root_.io.teknek.arizona.spark.connector.writer.AsyncExecutor.Handler

class QueryExecutor(
 session: CqlSession,
 maxConcurrentQueries: Int,
 successHandler: Option[Handler[RichStatement]],
 failureHandler: Option[Handler[RichStatement]])

  extends AsyncExecutor[RichStatement, AsyncResultSet](
    stmt => session.executeAsync(stmt.stmt),
    maxConcurrentQueries,
    successHandler,
    failureHandler)

object QueryExecutor {

  /**
    * Builds a query executor whose max requests per connection is limited to the MaxRequests per Connection
    */
  def apply(
    session: CqlSession,
    maxConcurrentQueries: Int,
    successHandler: Option[Handler[RichStatement]],
    failureHandler: Option[Handler[RichStatement]]): QueryExecutor = {

    new QueryExecutor(session, maxConcurrentQueries, successHandler, failureHandler)
  }
}
