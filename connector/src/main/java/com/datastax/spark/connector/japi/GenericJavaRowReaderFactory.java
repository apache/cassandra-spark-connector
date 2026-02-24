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

package com.datastax.spark.connector.japi;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.spark.connector.CassandraRowMetadata;
import com.datastax.spark.connector.ColumnRef;
import com.datastax.spark.connector.cql.TableDef;
import com.datastax.spark.connector.rdd.reader.RowReader;
import com.datastax.spark.connector.rdd.reader.RowReaderFactory;
import scala.Option;
import scala.collection.immutable.IndexedSeq;
import scala.collection.immutable.Seq;

public class GenericJavaRowReaderFactory {
    public final static RowReaderFactory<CassandraRow> instance = new RowReaderFactory<CassandraRow>() {

        @Override
        public RowReader<CassandraRow> rowReader(TableDef table, IndexedSeq<ColumnRef> selectedColumns) {
            return JavaRowReader.instance;
        }

        @Override
        public Class<CassandraRow> targetClass() {
            return CassandraRow.class;
        }
    };


    public static class JavaRowReader implements RowReader<CassandraRow> {
        public final static JavaRowReader instance = new JavaRowReader();

        private JavaRowReader() {
        }

        @Override
        public CassandraRow read(Row row, CassandraRowMetadata metaData) {
            assert row.getColumnDefinitions().size() == metaData.columnNames().size() :
                    "Number of columns in a row must match the number of columns in the table metadata";
            return CassandraRow$.MODULE$.fromJavaDriverRow(row, metaData);
        }

        @Override
        public Option<Seq<ColumnRef>> neededColumns() {
            return Option.empty();
        }
    }

}
