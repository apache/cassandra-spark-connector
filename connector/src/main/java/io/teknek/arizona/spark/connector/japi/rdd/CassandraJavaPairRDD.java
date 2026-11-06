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

package io.teknek.arizona.spark.connector.japi.rdd;

import java.util.Collection;

import scala.Tuple2;
import scala.collection.Seq;
import scala.reflect.ClassTag;

import static io.teknek.arizona.spark.connector.japi.CassandraJavaUtil.toSelectableColumnRefs;
import static io.teknek.arizona.spark.connector.util.JavaApiHelper.getClassTag;
import static io.teknek.arizona.spark.connector.util.JavaApiHelper.toScalaSeq;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

import io.teknek.arizona.spark.connector.ColumnRef;
import io.teknek.arizona.spark.connector.cql.CassandraConnector;
import io.teknek.arizona.spark.connector.japi.PairRDDJavaFunctions;
import io.teknek.arizona.spark.connector.rdd.CassandraRDD;
import io.teknek.arizona.spark.connector.rdd.ReadConf;
import io.teknek.arizona.spark.connector.util.JavaApiHelper;

@SuppressWarnings("UnusedDeclaration")
public class CassandraJavaPairRDD<K, V> extends JavaPairRDD<K, V> {

    public CassandraJavaPairRDD(
            CassandraRDD<Tuple2<K, V>> rdd,
            ClassTag<K> keyClassTag,
            ClassTag<V> valueClassTag
    ) {
        super(rdd, keyClassTag, valueClassTag);
    }

    public CassandraJavaPairRDD(
            CassandraRDD<Tuple2<K, V>> rdd,
            Class<K> keyClass,
            Class<V> valueClass
    ) {
        super(rdd, getClassTag(keyClass), getClassTag(valueClass));
    }

    private CassandraJavaPairRDD<K, V> wrap(CassandraRDD<Tuple2<K, V>> newRDD) {
        return new CassandraJavaPairRDD<>(newRDD, kClassTag(), vClassTag());
    }

    @Override
    public CassandraRDD<Tuple2<K, V>> rdd() {
        return (CassandraRDD<Tuple2<K, V>>) super.rdd();
    }

    /**
     * Narrows down the selected set of columns.
     * <p/>
     * Use this for better performance, when you don't need all the columns in the result RDD. When
     * called multiple times, it selects the subset of the already selected columns, so after a column
     * was removed by the previous {@code select} call, it is not possible to add it back.
     */
    @SuppressWarnings("unchecked")
    public CassandraJavaPairRDD<K, V> select(String... columnNames) {
        Seq<ColumnRef> columnRefs = toScalaSeq(toSelectableColumnRefs(columnNames));
        CassandraRDD<Tuple2<K, V>> newRDD = rdd().select(columnRefs.toSeq());
        return wrap(newRDD);
    }

    /**
     * Narrows down the selected set of columns.
     * <p/>
     * Use this for better performance, when you don't need all the columns in the result RDD. When
     * called multiple times, it selects the subset of the already selected columns, so after a column
     * was removed by the previous {@code select} call, it is not possible to add it back.
     */
    @SuppressWarnings("unchecked")
    public CassandraJavaPairRDD<K, V> select(ColumnRef... selectionColumns) {
        Seq<ColumnRef> columnRefs = JavaApiHelper.toScalaSeq(selectionColumns);
        CassandraRDD<Tuple2<K, V>> newRDD = rdd().select(columnRefs.toSeq());
        return wrap(newRDD);
    }

    /**
     * Adds a CQL {@code WHERE} predicate(s) to the query.
     * <p/>
     * Useful for leveraging secondary indexes in Cassandra. Implicitly adds an {@code ALLOW FILTERING}
     * clause to the {@code WHERE} clause, however beware that some predicates might be rejected by
     * Cassandra, particularly in cases when they filter on an unindexed, non-clustering column.
     */
    @SuppressWarnings("unchecked")
    public CassandraJavaPairRDD<K, V> where(String cqlWhereClause, Object... args) {
        CassandraRDD<Tuple2<K, V>> newRDD = rdd().where(cqlWhereClause, toScalaSeq(args));
        return wrap(newRDD);
    }

    /**
     * Forces the rows within a selected Cassandra partition to be returned in ascending order (if
     * possible).
     */
    @SuppressWarnings("unchecked")
    public CassandraJavaPairRDD<K, V> withAscOrder() {
        CassandraRDD<Tuple2<K, V>> newRDD = rdd().withAscOrder();
        return wrap(newRDD);
    }

    /**
     * Forces the rows within a selected Cassandra partition to be returned in descending order (if
     * possible).
     */
    @SuppressWarnings("unchecked")
    public CassandraJavaPairRDD<K, V> withDescOrder() {
        CassandraRDD<Tuple2<K, V>> newRDD = rdd().withDescOrder();
        return wrap(newRDD);
    }

    /**
     * Adds the limit clause to CQL select statement. The limit will be applied for each created Spark
     * partition. In other words, unless the data are fetched from a single Cassandra partition the
     * number of results is unpredictable.
     * <p/>
     * The main purpose of passing limit clause is to fetch top n rows from a single Cassandra partition
     * when the table is designed so that it uses clustering keys and a partition key predicate is passed
     * to the where clause.
     */
    @SuppressWarnings("unchecked")
    public CassandraJavaPairRDD<K, V> limit(Long rowsNumber) {
        CassandraRDD<Tuple2<K, V>> newRDD = rdd().limit(rowsNumber);
        return wrap(newRDD);
    }

    /**
     * Adds the PER PARTITION LIMIT clause to CQL select statement. The limit will be applied for each
     * Cassandra Partition
     */
    @SuppressWarnings("unchecked")
    public CassandraJavaPairRDD<K, V> perPartitionLimit(Long rowsNumber) {
        CassandraRDD<Tuple2<K, V>> newRDD = rdd().perPartitionLimit(rowsNumber);
        return wrap(newRDD);
    }

    /**
     * Returns the names of columns to be selected from the table.
     */
    @SuppressWarnings("RedundantCast")
    public ColumnRef[] selectedColumnRefs() {
        ClassTag<ColumnRef> classTag = getClassTag(ColumnRef.class);
        return (ColumnRef[]) rdd().selectedColumnRefs().toArray(classTag);
    }

    /**
     * Returns the names of columns to be selected from the table.
     */
    @SuppressWarnings("RedundantCast")
    public String[] selectedColumnNames() {
        ClassTag<String> classTag = getClassTag(String.class);
        return (String[]) rdd().selectedColumnNames().toArray(classTag);
    }

    /**
     * Returns a copy of this RDD with connector changed to the specified one.
     */
    @SuppressWarnings("unchecked")
    public CassandraJavaPairRDD<K, V> withConnector(CassandraConnector connector) {
        CassandraRDD<Tuple2<K, V>> newRDD = rdd().withConnector(connector);
        return wrap(newRDD);
    }

    /**
     * Returns a copy of this RDD with read configuration changed to the specified one.
     */
    @SuppressWarnings("unchecked")
    public CassandraJavaPairRDD<K, V> withReadConf(ReadConf config) {
        CassandraRDD<Tuple2<K, V>> newRDD = rdd().withReadConf(config);
        return wrap(newRDD);
    }

    /**
     * Produces the empty CassandraRDD which has the same signature and properties, but it does not
     * perform any validation and it does not even try to return any rows.
     */
    public CassandraJavaPairRDD<K, V> toEmptyCassandraRDD() {
        CassandraRDD<Tuple2<K, V>> newRDD = rdd().toEmptyCassandraRDD();
        return wrap(newRDD);
    }

    /**
     * Groups items with the same key, assuming the items with the same key are next to each other in the
     * collection. It does not perform shuffle, therefore it is much faster than using much more
     * universal Spark RDD `groupByKey`. For this method to be useful with Cassandra tables, the key must
     * represent a prefix of the primary key, containing at least the partition key of the Cassandra
     * table.
     */
    public JavaPairRDD<K, Collection<V>> spanByKey() {
        return new PairRDDJavaFunctions<>(rdd()).spanByKey(kClassTag());
    }

    /**
     * Applies a function to each item, and groups consecutive items having the same value together.
     * Contrary to `groupBy`, items from the same group must be already next to each other in the
     * original collection. Works locally on each partition, so items from different partitions will
     * never be placed in the same group.
     */
    public <U> JavaPairRDD<U, Iterable<Tuple2<K, V>>> spanBy(
            Function<Tuple2<K, V>, U> function,
            ClassTag<U> uClassTag
    ) {
        return new PairRDDJavaFunctions<>(rdd()).spanBy(function, uClassTag);
    }

    /** @see {@link #spanBy(Function, ClassTag)} */
    public <U> JavaPairRDD<U, Iterable<Tuple2<K, V>>> spanBy(
            Function<Tuple2<K, V>, U> function,
            Class<U> uClass
    ) {
        return new PairRDDJavaFunctions<>(rdd()).spanBy(function, getClassTag(uClass));
    }

    /**
     * Counts the number of items in this RDD by selecting count(*) on Cassandra table.
     */
    public long cassandraCount() {
        return rdd().cassandraCount();
    }

}
