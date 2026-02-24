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

import com.datastax.spark.connector.ColumnSelector;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.streaming.DStreamFunctions;
import com.datastax.spark.connector.writer.RowWriterFactory;
import com.datastax.spark.connector.writer.TokenRangeAccumulator;
import com.datastax.spark.connector.writer.WriteConf;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.dstream.DStream;
import scala.Option;

/**
 * A Java API wrapper over {@link DStream} to provide Spark Cassandra Connector functionality.
 * <p/>
 * To obtain an instance of this wrapper, use one of the factory methods in {@link CassandraJavaUtil}
 * class.
 */
@SuppressWarnings("UnusedDeclaration")
public class DStreamJavaFunctions<T> extends RDDAndDStreamCommonJavaFunctions<T> {
    public final DStream<T> dstream;
    private final DStreamFunctions<T> dsf;

    DStreamJavaFunctions(DStream<T> dStream) {
        this.dstream = dStream;
        this.dsf = new DStreamFunctions<>(dStream);
    }

    @Override
    public CassandraConnector defaultConnector() {
        return dsf.connector();
    }

    @Override
    protected SparkConf getConf() {
        return dstream.ssc().conf();
    }

    @Override
    protected void saveToCassandra(String keyspace, String table, RowWriterFactory<T> rowWriterFactory,
                                   ColumnSelector columnNames, WriteConf conf, CassandraConnector connector) {
        dsf.saveToCassandra(keyspace, table, columnNames, conf, Option.<TokenRangeAccumulator>apply(null), connector, rowWriterFactory);
    }
}
