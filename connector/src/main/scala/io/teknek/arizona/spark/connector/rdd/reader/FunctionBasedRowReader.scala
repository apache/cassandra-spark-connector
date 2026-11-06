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

package io.teknek.arizona.spark.connector.rdd.reader

import com.datastax.oss.driver.api.core.cql.Row
import _root_.io.teknek.arizona.spark.connector.{CassandraRowMetadata, GettableData}
import _root_.io.teknek.arizona.spark.connector.types.TypeConverter

import scala.reflect.ClassTag

// The below fragment may look very repetitive and copy-pasted,
// however there is no other way to code this for functions of different arity
// while preserving good type-safety.

trait FunctionBasedRowReader[R] extends RowReader[R] with ThisRowReaderAsFactory[R] {
  def ct: ClassTag[R]
  override val targetClass: Class[R] = ct.runtimeClass.asInstanceOf[Class[R]]
  override def neededColumns = None
}

class FunctionBasedRowReader1[R, A0](f: A0 => R)(
  implicit a0c: TypeConverter[A0], @transient override val ct: ClassTag[R]) extends FunctionBasedRowReader[R] {

  override def read(row: Row, rowMetaData: CassandraRowMetadata) =
    f(a0c.convert(GettableData.get(row, 0, rowMetaData.codecs(0))))

}

class FunctionBasedRowReader2[R, A0, A1](f: (A0, A1) => R)(
  implicit
  a0c: TypeConverter[A0],
  a1c: TypeConverter[A1],
  @transient override val ct: ClassTag[R])
  extends FunctionBasedRowReader[R] {

  override def read(row: Row, rowMetaData: CassandraRowMetadata) =
    f(
      a0c.convert(GettableData.get(row, 0, rowMetaData.codecs(0))),
      a1c.convert(GettableData.get(row, 1, rowMetaData.codecs(1)))
    )
}

class FunctionBasedRowReader3[R, A0, A1, A2](f: (A0, A1, A2) => R)(
  implicit
  a0c: TypeConverter[A0],
  a1c: TypeConverter[A1],
  a2c: TypeConverter[A2],
  @transient override val ct: ClassTag[R])
  extends FunctionBasedRowReader[R] {

  override def read(row: Row, rowMetaData: CassandraRowMetadata) =
    f(
      a0c.convert(GettableData.get(row, 0, rowMetaData.codecs(0))),
      a1c.convert(GettableData.get(row, 1, rowMetaData.codecs(1))),
      a2c.convert(GettableData.get(row, 2, rowMetaData.codecs(2))))

}

class FunctionBasedRowReader4[R, A0, A1, A2, A3](f: (A0, A1, A2, A3) => R)(
  implicit
  a0c: TypeConverter[A0],
  a1c: TypeConverter[A1],
  a2c: TypeConverter[A2],
  a3c: TypeConverter[A3],
  @transient override val ct: ClassTag[R])
  extends FunctionBasedRowReader[R] {

  override def read(row: Row, rowMetaData: CassandraRowMetadata) =
    f(
      a0c.convert(GettableData.get(row, 0, rowMetaData.codecs(0))),
      a1c.convert(GettableData.get(row, 1, rowMetaData.codecs(1))),
      a2c.convert(GettableData.get(row, 2, rowMetaData.codecs(2))),
      a3c.convert(GettableData.get(row, 3, rowMetaData.codecs(3)))
    )
}

class FunctionBasedRowReader5[R, A0, A1, A2, A3, A4](f: (A0, A1, A2, A3, A4) => R)(
  implicit
  a0c: TypeConverter[A0],
  a1c: TypeConverter[A1],
  a2c: TypeConverter[A2],
  a3c: TypeConverter[A3],
  a4c: TypeConverter[A4],
  @transient override val ct: ClassTag[R])
  extends FunctionBasedRowReader[R] {

  override def read(row: Row, rowMetaData: CassandraRowMetadata) =
    f(
      a0c.convert(GettableData.get(row, 0, rowMetaData.codecs(0))),
      a1c.convert(GettableData.get(row, 1, rowMetaData.codecs(1))),
      a2c.convert(GettableData.get(row, 2, rowMetaData.codecs(2))),
      a3c.convert(GettableData.get(row, 3, rowMetaData.codecs(3))),
      a4c.convert(GettableData.get(row, 4, rowMetaData.codecs(4)))
    )
}

class FunctionBasedRowReader6[R, A0, A1, A2, A3, A4, A5](f: (A0, A1, A2, A3, A4, A5) => R)(
  implicit
  a0c: TypeConverter[A0],
  a1c: TypeConverter[A1],
  a2c: TypeConverter[A2],
  a3c: TypeConverter[A3],
  a4c: TypeConverter[A4],
  a5c: TypeConverter[A5],
  @transient override val ct: ClassTag[R])
  extends FunctionBasedRowReader[R] {

  override def read(row: Row, rowMetaData: CassandraRowMetadata) =
    f(
      a0c.convert(GettableData.get(row, 0, rowMetaData.codecs(0))),
      a1c.convert(GettableData.get(row, 1, rowMetaData.codecs(1))),
      a2c.convert(GettableData.get(row, 2, rowMetaData.codecs(2))),
      a3c.convert(GettableData.get(row, 3, rowMetaData.codecs(3))),
      a4c.convert(GettableData.get(row, 4, rowMetaData.codecs(4))),
      a5c.convert(GettableData.get(row, 5, rowMetaData.codecs(5)))
    )
}

class FunctionBasedRowReader7[R, A0, A1, A2, A3, A4, A5, A6](f: (A0, A1, A2, A3, A4, A5, A6) => R)(
  implicit
  a0c: TypeConverter[A0],
  a1c: TypeConverter[A1],
  a2c: TypeConverter[A2],
  a3c: TypeConverter[A3],
  a4c: TypeConverter[A4],
  a5c: TypeConverter[A5],
  a6c: TypeConverter[A6],
  @transient override val ct: ClassTag[R])
  extends FunctionBasedRowReader[R] {

  override def read(row: Row, rowMetaData: CassandraRowMetadata) =
    f(
      a0c.convert(GettableData.get(row, 0, rowMetaData.codecs(0))),
      a1c.convert(GettableData.get(row, 1, rowMetaData.codecs(1))),
      a2c.convert(GettableData.get(row, 2, rowMetaData.codecs(2))),
      a3c.convert(GettableData.get(row, 3, rowMetaData.codecs(3))),
      a4c.convert(GettableData.get(row, 4, rowMetaData.codecs(4))),
      a5c.convert(GettableData.get(row, 5, rowMetaData.codecs(5))),
      a6c.convert(GettableData.get(row, 6, rowMetaData.codecs(6)))
    )
}

class FunctionBasedRowReader8[R, A0, A1, A2, A3, A4, A5, A6, A7]
(f: (A0, A1, A2, A3, A4, A5, A6, A7) => R)(
  implicit
  a0c: TypeConverter[A0],
  a1c: TypeConverter[A1],
  a2c: TypeConverter[A2],
  a3c: TypeConverter[A3],
  a4c: TypeConverter[A4],
  a5c: TypeConverter[A5],
  a6c: TypeConverter[A6],
  a7c: TypeConverter[A7],
  @transient override val ct: ClassTag[R])
  extends FunctionBasedRowReader[R] {

  override def read(row: Row, rowMetaData: CassandraRowMetadata) =
    f(
      a0c.convert(GettableData.get(row, 0, rowMetaData.codecs(0))),
      a1c.convert(GettableData.get(row, 1, rowMetaData.codecs(1))),
      a2c.convert(GettableData.get(row, 2, rowMetaData.codecs(2))),
      a3c.convert(GettableData.get(row, 3, rowMetaData.codecs(3))),
      a4c.convert(GettableData.get(row, 4, rowMetaData.codecs(4))),
      a5c.convert(GettableData.get(row, 5, rowMetaData.codecs(5))),
      a6c.convert(GettableData.get(row, 6, rowMetaData.codecs(6))),
      a7c.convert(GettableData.get(row, 7, rowMetaData.codecs(7)))
    )
}

class FunctionBasedRowReader9[R, A0, A1, A2, A3, A4, A5, A6, A7, A8]
(f: (A0, A1, A2, A3, A4, A5, A6, A7, A8) => R)(
  implicit
  a0c: TypeConverter[A0],
  a1c: TypeConverter[A1],
  a2c: TypeConverter[A2],
  a3c: TypeConverter[A3],
  a4c: TypeConverter[A4],
  a5c: TypeConverter[A5],
  a6c: TypeConverter[A6],
  a7c: TypeConverter[A7],
  a8c: TypeConverter[A8],
  @transient override val ct: ClassTag[R])
  extends FunctionBasedRowReader[R] {

  override def read(row: Row, rowMetaData: CassandraRowMetadata) =
    f(
      a0c.convert(GettableData.get(row, 0, rowMetaData.codecs(0))),
      a1c.convert(GettableData.get(row, 1, rowMetaData.codecs(1))),
      a2c.convert(GettableData.get(row, 2, rowMetaData.codecs(2))),
      a3c.convert(GettableData.get(row, 3, rowMetaData.codecs(3))),
      a4c.convert(GettableData.get(row, 4, rowMetaData.codecs(4))),
      a5c.convert(GettableData.get(row, 5, rowMetaData.codecs(5))),
      a6c.convert(GettableData.get(row, 6, rowMetaData.codecs(6))),
      a7c.convert(GettableData.get(row, 7, rowMetaData.codecs(7))),
      a8c.convert(GettableData.get(row, 8, rowMetaData.codecs(8)))
    )
}

class FunctionBasedRowReader10[R, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9]
(f: (A0, A1, A2, A3, A4, A5, A6, A7, A8, A9) => R)(
  implicit
  a0c: TypeConverter[A0],
  a1c: TypeConverter[A1],
  a2c: TypeConverter[A2],
  a3c: TypeConverter[A3],
  a4c: TypeConverter[A4],
  a5c: TypeConverter[A5],
  a6c: TypeConverter[A6],
  a7c: TypeConverter[A7],
  a8c: TypeConverter[A8],
  a9c: TypeConverter[A9],
  @transient override val ct: ClassTag[R])
  extends FunctionBasedRowReader[R] {

  override def read(row: Row, rowMetaData: CassandraRowMetadata) =
    f(
      a0c.convert(GettableData.get(row, 0, rowMetaData.codecs(0))),
      a1c.convert(GettableData.get(row, 1, rowMetaData.codecs(1))),
      a2c.convert(GettableData.get(row, 2, rowMetaData.codecs(2))),
      a3c.convert(GettableData.get(row, 3, rowMetaData.codecs(3))),
      a4c.convert(GettableData.get(row, 4, rowMetaData.codecs(4))),
      a5c.convert(GettableData.get(row, 5, rowMetaData.codecs(5))),
      a6c.convert(GettableData.get(row, 6, rowMetaData.codecs(6))),
      a7c.convert(GettableData.get(row, 7, rowMetaData.codecs(7))),
      a8c.convert(GettableData.get(row, 8, rowMetaData.codecs(8))),
      a9c.convert(GettableData.get(row, 9, rowMetaData.codecs(9)))
    )
}

class FunctionBasedRowReader11[R, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10]
(f: (A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10) => R)(
  implicit
  a0c: TypeConverter[A0],
  a1c: TypeConverter[A1],
  a2c: TypeConverter[A2],
  a3c: TypeConverter[A3],
  a4c: TypeConverter[A4],
  a5c: TypeConverter[A5],
  a6c: TypeConverter[A6],
  a7c: TypeConverter[A7],
  a8c: TypeConverter[A8],
  a9c: TypeConverter[A9],
  a10c: TypeConverter[A10],
  @transient override val ct: ClassTag[R])
  extends FunctionBasedRowReader[R] {

  override def read(row: Row, rowMetaData: CassandraRowMetadata) =
    f(
      a0c.convert(GettableData.get(row, 0, rowMetaData.codecs(0))),
      a1c.convert(GettableData.get(row, 1, rowMetaData.codecs(1))),
      a2c.convert(GettableData.get(row, 2, rowMetaData.codecs(2))),
      a3c.convert(GettableData.get(row, 3, rowMetaData.codecs(3))),
      a4c.convert(GettableData.get(row, 4, rowMetaData.codecs(4))),
      a5c.convert(GettableData.get(row, 5, rowMetaData.codecs(5))),
      a6c.convert(GettableData.get(row, 6, rowMetaData.codecs(6))),
      a7c.convert(GettableData.get(row, 7, rowMetaData.codecs(7))),
      a8c.convert(GettableData.get(row, 8, rowMetaData.codecs(8))),
      a9c.convert(GettableData.get(row, 9, rowMetaData.codecs(9))),
      a10c.convert(GettableData.get(row, 10, rowMetaData.codecs(10)))
    )
}

class FunctionBasedRowReader12[R, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11]
(f: (A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11) => R)(
  implicit
  a0c: TypeConverter[A0],
  a1c: TypeConverter[A1],
  a2c: TypeConverter[A2],
  a3c: TypeConverter[A3],
  a4c: TypeConverter[A4],
  a5c: TypeConverter[A5],
  a6c: TypeConverter[A6],
  a7c: TypeConverter[A7],
  a8c: TypeConverter[A8],
  a9c: TypeConverter[A9],
  a10c: TypeConverter[A10],
  a11c: TypeConverter[A11],
  @transient override val ct: ClassTag[R])
  extends FunctionBasedRowReader[R] {

  override def read(row: Row, rowMetaData: CassandraRowMetadata) =
    f(
      a0c.convert(GettableData.get(row, 0, rowMetaData.codecs(0))),
      a1c.convert(GettableData.get(row, 1, rowMetaData.codecs(1))),
      a2c.convert(GettableData.get(row, 2, rowMetaData.codecs(2))),
      a3c.convert(GettableData.get(row, 3, rowMetaData.codecs(3))),
      a4c.convert(GettableData.get(row, 4, rowMetaData.codecs(4))),
      a5c.convert(GettableData.get(row, 5, rowMetaData.codecs(5))),
      a6c.convert(GettableData.get(row, 6, rowMetaData.codecs(6))),
      a7c.convert(GettableData.get(row, 7, rowMetaData.codecs(7))),
      a8c.convert(GettableData.get(row, 8, rowMetaData.codecs(8))),
      a9c.convert(GettableData.get(row, 9, rowMetaData.codecs(9))),
      a10c.convert(GettableData.get(row, 10, rowMetaData.codecs(10))),
      a11c.convert(GettableData.get(row, 11, rowMetaData.codecs(11)))
    )
}

