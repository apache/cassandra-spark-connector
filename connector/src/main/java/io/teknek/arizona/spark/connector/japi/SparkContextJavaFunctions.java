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

package io.teknek.arizona.spark.connector.japi;

import scala.Tuple2;

import org.apache.spark.SparkContext;

import io.teknek.arizona.spark.connector.japi.rdd.CassandraJavaPairRDD;
import io.teknek.arizona.spark.connector.japi.rdd.CassandraJavaRDD;
import io.teknek.arizona.spark.connector.japi.rdd.CassandraTableScanJavaRDD;
import io.teknek.arizona.spark.connector.rdd.CassandraRDD;
import io.teknek.arizona.spark.connector.rdd.CassandraTableScanRDD;
import io.teknek.arizona.spark.connector.rdd.CassandraTableScanRDD$;
import io.teknek.arizona.spark.connector.rdd.reader.RowReaderFactory;

import static io.teknek.arizona.spark.connector.util.JavaApiHelper.getClassTag;

/**
 * Java API wrapper over {@link org.apache.spark.SparkContext} to provide Spark Cassandra Connector functionality.
 *
 * <p>To obtain an instance of this wrapper, use one of the factory methods in {@link
 * io.teknek.arizona.spark.connector.japi.CassandraJavaUtil} class.</p>
 */
@SuppressWarnings("UnusedDeclaration")
public class SparkContextJavaFunctions {
    public final SparkContext sparkContext;

    SparkContextJavaFunctions(SparkContext sparkContext) {
        this.sparkContext = sparkContext;
    }

    /**
     * Converts {@code CassandraRDD} into {@code CassandraJavaRDD}.
     */
    public <T> CassandraJavaRDD<T> toJavaRDD(CassandraRDD<T> rdd, Class<T> targetClass) {
        return new CassandraJavaRDD<>(rdd, getClassTag(targetClass));
    }

    /**
     * Converts {@code CassandraRDD} of {@code Tuple2} into {@code CassandraJavaPairRDD}.
     */
    public <K, V> CassandraJavaPairRDD<K, V> toJavaPairRDD(CassandraRDD<Tuple2<K, V>> rdd, Class<K> keyClass, Class<V> valueClass) {
        return new CassandraJavaPairRDD<>(rdd, getClassTag(keyClass), getClassTag(valueClass));
    }


    /**
     * Returns a view of a Cassandra table as a {@link io.teknek.arizona.spark.connector.japi.rdd.CassandraJavaRDD}.
     *
     * <p>With this method, each row is converted to a {@code CassandraRow} object.</p>
     *
     * <p>Example:
     * <pre>
     * CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };
     * CREATE TABLE test.words (word text PRIMARY KEY, count int);
     * INSERT INTO test.words (word, count) VALUES ('foo', 20);
     * INSERT INTO test.words (word, count) VALUES ('bar', 20);
     * ...
     *
     * // Obtaining RDD of CassandraRow objects:
     * CassandraJavaRDD<CassandraRow> rdd = CassandraJavaUtil.javaFunctions(jsc).cassandraTable("test", "words");
     * rdd.first().getString("word");     // foo
     * rdd.first().getInt("count");       // 20
     * </pre>
     * </p>
     *
     * @since 1.0.0
     */
    public CassandraTableScanJavaRDD<CassandraRow> cassandraTable(String keyspace, String table) {
        RowReaderFactory<CassandraRow> rtf = GenericJavaRowReaderFactory.instance;
        return cassandraTable(keyspace, table, rtf);
    }

    /**
     * Returns a view of a Cassandra table as a {@link io.teknek.arizona.spark.connector.japi.rdd.CassandraJavaRDD}.
     *
     * <p>With this method, each row is converted to a object of type {@code T} by a specified row reader factory
     * {@code rrf}. Row reader factories can be easily obtained with one of utility methods in {@link
     * io.teknek.arizona.spark.connector.japi.CassandraJavaUtil}.</p>
     *
     * @param keyspace the name of the keyspace which contains the accessed table
     * @param table    the accessed Cassandra table name
     * @param rrf      a row reader factory to convert rows into target values
     * @param <T>      target value type
     *
     * @return {@link io.teknek.arizona.spark.connector.japi.rdd.CassandraJavaRDD} of type {@code T}
     *
     * @since 1.1.0
     */
    public <T> CassandraTableScanJavaRDD<T> cassandraTable(
            String keyspace,
            String table,
            RowReaderFactory<T> rrf) {
        CassandraTableScanRDD<T> rdd = CassandraTableScanRDD$.MODULE$
                .apply(sparkContext, keyspace, table, getClassTag(rrf.targetClass()), rrf);
        return new CassandraTableScanJavaRDD<>(rdd, getClassTag(rrf.targetClass()));
    }

}
