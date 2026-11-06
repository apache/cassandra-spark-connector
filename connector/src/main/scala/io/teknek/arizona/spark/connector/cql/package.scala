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

import com.datastax.oss.driver.api.core.cql.Row


/** Contains a [[cql.CassandraConnector]] object which is used to connect
  * to a Cassandra cluster and to send CQL statements to it. `CassandraConnector`
  * provides a Scala-idiomatic way of working with `Session` object
  * and takes care of connection pooling and proper resource disposal.*/
package object cql {

  def getRowBinarySize(row: Row): Int = {
    var size = 0
    for (i <- 0 until row.getColumnDefinitions.size() if !row.isNull(i))
      size += row.getBytesUnsafe(i).remaining()
    size
  }

}
