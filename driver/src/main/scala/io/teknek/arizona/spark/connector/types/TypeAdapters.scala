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

/**
  * Type adapters that serve as a middle step in conversion from one type to another.
  *
  * Adapters are utilized by types with scheme ([[UserDefinedType]]], [[TupleType]]) to convert an instance of
  * an type to corresponding adapter and than to final value of the given type.
  */
private[spark] object TypeAdapters {
  /**
    * Adapter for multi-values types that my be returned as a sequence.
    *
    * It is used to extend conversion capabilities offered by Tuple type.
    */
  trait ValuesSeqAdapter {
    def toSeq(): Seq[Any]
  }

  /**
    * Adapter for multi-value types that may return values by name.
    *
    * It is used to extend conversion capabilities offered by UDT type.
    */
  trait ValueByNameAdapter {
    def getByName(name: String): Any
  }
}
