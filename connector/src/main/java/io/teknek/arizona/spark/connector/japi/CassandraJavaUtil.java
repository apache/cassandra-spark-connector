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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import scala.Option;
import scala.Tuple1;
import scala.Tuple10;
import scala.Tuple11;
import scala.Tuple12;
import scala.Tuple13;
import scala.Tuple14;
import scala.Tuple15;
import scala.Tuple16;
import scala.Tuple17;
import scala.Tuple18;
import scala.Tuple19;
import scala.Tuple2;
import scala.Tuple20;
import scala.Tuple21;
import scala.Tuple22;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple5;
import scala.Tuple6;
import scala.Tuple7;
import scala.Tuple8;
import scala.Tuple9;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;
import scala.reflect.api.TypeTags;

import io.teknek.arizona.spark.connector.AllColumns$;
import io.teknek.arizona.spark.connector.BatchSize;
import io.teknek.arizona.spark.connector.BatchSize$;
import io.teknek.arizona.spark.connector.BytesInBatch$;
import io.teknek.arizona.spark.connector.ColumnName;
import io.teknek.arizona.spark.connector.ColumnName$;
import io.teknek.arizona.spark.connector.ColumnRef;
import io.teknek.arizona.spark.connector.ColumnSelector;
import io.teknek.arizona.spark.connector.RowsInBatch$;
import io.teknek.arizona.spark.connector.SomeColumns$;
import io.teknek.arizona.spark.connector.TTL;
import io.teknek.arizona.spark.connector.WriteTime;
import io.teknek.arizona.spark.connector.cql.CassandraConnector;
import io.teknek.arizona.spark.connector.mapper.ColumnMapper;
import io.teknek.arizona.spark.connector.mapper.JavaBeanColumnMapper;
import io.teknek.arizona.spark.connector.mapper.TupleColumnMapper;
import io.teknek.arizona.spark.connector.rdd.reader.ClassBasedRowReaderFactory;
import io.teknek.arizona.spark.connector.rdd.reader.RowReaderFactory;
import io.teknek.arizona.spark.connector.rdd.reader.ValueRowReaderFactory;
import io.teknek.arizona.spark.connector.types.TypeConverter;
import io.teknek.arizona.spark.connector.types.TypeConverter$;
import io.teknek.arizona.spark.connector.util.JavaApiHelper;
import io.teknek.arizona.spark.connector.util.JavaApiHelper$;
import io.teknek.arizona.spark.connector.writer.BatchGroupingKey;
import io.teknek.arizona.spark.connector.writer.RowWriterFactory;

import static io.teknek.arizona.spark.connector.util.JavaApiHelper.defaultRowWriterFactory;
import static io.teknek.arizona.spark.connector.util.JavaApiHelper.javaBeanColumnMapper;

/**
 * The main entry point to Spark Cassandra Connector Java API.
 * <p/>
 * There are several helpful static factory methods which build useful wrappers around Spark Context, and
 * RDD. There are also helper methods to build reader and writer factories which configure their
 * parameters.
 */
@SuppressWarnings("UnusedDeclaration")
public class CassandraJavaUtil {

    private CassandraJavaUtil() {
        assert false;
    }

    // -------------------------------------------------------------------------
    //              Java API wrappers factory methods 
    // -------------------------------------------------------------------------

    /**
     * A static factory method to create a {@link SparkContextJavaFunctions} based on an existing {@link
     * SparkContext} instance.
     */
    public static SparkContextJavaFunctions javaFunctions(SparkContext sparkContext) {
        return new SparkContextJavaFunctions(sparkContext);
    }

    /**
     * A static factory method to create a {@link SparkContextJavaFunctions} based on an existing {@link
     * JavaSparkContext} instance.
     */
    public static SparkContextJavaFunctions javaFunctions(JavaSparkContext sparkContext) {
        return new SparkContextJavaFunctions(JavaSparkContext.toSparkContext(sparkContext));
    }

    /**
     * A static factory method to create a {@link RDDJavaFunctions} based on an existing {@link RDD}
     * instance.
     */
    public static <T> RDDJavaFunctions<T> javaFunctions(RDD<T> rdd) {
        return new RDDJavaFunctions<>(rdd);
    }

    /**
     * A static factory method to create a {@link RDDJavaFunctions} based on an existing {@link JavaRDD}
     * instance.
     */
    public static <T> RDDJavaFunctions<T> javaFunctions(JavaRDD<T> rdd) {
        return new RDDJavaFunctions<>(rdd.rdd());
    }

    /**
     * A static factory method to create a {@link PairRDDJavaFunctions} based on an existing {@link
     * JavaPairRDD} instance.
     */
    public static <K, V> PairRDDJavaFunctions<K, V> javaFunctions(JavaPairRDD<K, V> rdd) {
        return new PairRDDJavaFunctions<>(rdd.rdd());
    }

    // -------------------------------------------------------------------------
    //              Type tag resolvers 
    // -------------------------------------------------------------------------

    public static <T> ClassTag<T> safeClassTag(Class<T> cls) {
        return JavaApiHelper$.MODULE$.getClassTag(cls);
    }

    public static <T> ClassTag<T> classTag(Class<?> cls) {
        return JavaApiHelper$.MODULE$.getClassTag2(cls);
    }

    @SuppressWarnings("unchecked")
    public static <T> ClassTag<T> anyClassTag() {
        return (ClassTag<T>) ClassTag$.MODULE$.AnyRef();
    }

    /**
     * Creates a type tag representing a given class type without any type parameters.
     */
    public static <T> TypeTags.TypeTag<T> safeTypeTag(Class<T> main) {
        return JavaApiHelper.getTypeTag(main);
    }

    /**
     * Creates a type tag representing a given class type without any type parameters.
     */
    @SuppressWarnings("unchecked")
    public static <T> TypeTags.TypeTag<T> typeTag(Class<?> main) {
        return JavaApiHelper.getTypeTag((Class<T>) main);
    }

    /**
     * Creates a type tag representing a given class type with given type parameters.
     * <p/>
     * This is the most generic method to create type tags. One can use it to create type tags of
     * arbitrary class types with any number of type parameters and any depth of type parameter nesting.
     * <p/>
     * For example, the following code: {@code typeTag(Map.class, typeTag(String.class),
     * typeTag(List.class, typeTag(String.class))} returns the type tag of {@code Map<String,
     * List<String>>}
     */
    public static <T> TypeTags.TypeTag<T> typeTag(
            Class<?> main,
            TypeTags.TypeTag<?> typeParam1,
            TypeTags.TypeTag<?>... typeParams
    ) {
        return JavaApiHelper.getTypeTag(main, ArrayUtils.add(typeParams, 0, typeParam1));
    }

    /**
     * Creates a type tag representing a given class type with given type parameters.
     * <p/>
     * With this factory method you can create type tags of class types with any number of type
     * parameters. However, the type parameters are class types without their own type parameters.
     * <p/>
     * For example, the following code: {@code typeTag(List.class, String.class)} returns a type tag of
     * {@code List<String>}, and the following code: {@code typeTag(Map.class, String.class,
     * Integer.class)} returns a type tag of {@code Map<String, Integer>}.
     * <p/>
     * This is a short-hand method for calling {@link #typeTag(Class, TypeTags.TypeTag,
     * TypeTags.TypeTag[])} and providing type parameters with {@link #safeTypeTag(Class)}.
     */
    public static <T> TypeTags.TypeTag<T> typeTag(Class<?> main, Class<?> typeParam1, Class<?>... typeParams) {
        TypeTags.TypeTag<?>[] typeParamsTags = new TypeTags.TypeTag[typeParams.length + 1];
        typeParamsTags[0] = typeTag(typeParam1);
        for (int i = 0; i < typeParams.length; i++) {
            typeParamsTags[i + 1] = typeTag(typeParams[i]);
        }
        return JavaApiHelper.getTypeTag(main, typeParamsTags);
    }

    // -------------------------------------------------------------------------
    //              Type converter resolvers
    // -------------------------------------------------------------------------

    /**
     * Retrieves a type converter for the given class type.
     * <p/>
     * This is a short-hand method for calling {@link #typeConverter(TypeTags.TypeTag)} with an argument
     * obtained by {@link #safeTypeTag(Class)}.
     */
    public static <T> TypeConverter<T> safeTypeConverter(Class<T> main) {
        return TypeConverter$.MODULE$.forType(safeTypeTag(main));
    }

    /**
     * Retrieves a type converter for a type represented by a given type tag.
     */
    @SuppressWarnings("unchecked")
    public static <T> TypeConverter<T> typeConverter(TypeTags.TypeTag<?> typeTag) {
        return (TypeConverter<T>) TypeConverter$.MODULE$.forType(typeTag);
    }

    /**
     * Retrieves a type converter for the given class type.
     * <p/>
     * This is a short-hand method for calling {@link #typeConverter(TypeTags.TypeTag)} with an argument
     * obtained by {@link #safeTypeTag(Class)}.
     */
    @SuppressWarnings("unchecked")
    public static <T> TypeConverter<T> typeConverter(Class<?> main) {
        return TypeConverter$.MODULE$.forType(CassandraJavaUtil.<T>typeTag(main));
    }

    /**
     * Retrieves a type converter for the given class type with the given type parameters.
     * <p/>
     * This is a short-hand method for calling {@link #typeConverter(TypeTags.TypeTag)} with an argument
     * obtained by {@link #typeTag(Class, Class, Class[])}.
     */
    @SuppressWarnings("unchecked")
    public static <T> TypeConverter<T> typeConverter(Class<?> main, Class<?> typeParam1, Class<?>... typeParams) {
        return TypeConverter$.MODULE$.forType(CassandraJavaUtil.<T>typeTag(main, typeParam1, typeParams));
    }

    /**
     * Retrieves a type converter for the given class type with the given type parameters.
     * <p/>
     * This is a short-hand method for calling {@link #typeConverter(TypeTags.TypeTag)} with an argument
     * obtained by {@link #typeTag(Class, TypeTags.TypeTag, TypeTags.TypeTag[])}.
     */
    @SuppressWarnings("unchecked")
    public static <T> TypeConverter<T> typeConverter(
            Class<?> main,
            TypeTags.TypeTag<?> typeParam1,
            TypeTags.TypeTag<?>... typeParams
    ) {
        return TypeConverter$.MODULE$.forType(CassandraJavaUtil.<T>typeTag(main, typeParam1, typeParams));
    }

    // -------------------------------------------------------------------------
    //              Single column mappers 
    // -------------------------------------------------------------------------

    /**
     * Constructs a row reader factory which maps a single column from a row to the instance of a given
     * class.
     * <p/>
     * It is a short-hand method for calling {@link #mapColumnTo(TypeConverter)} with the type converter
     * resolved by {@link #typeConverter(Class)} method.
     */
    public static <T> RowReaderFactory<T> mapColumnTo(Class<T> targetClass) {
        return new ValueRowReaderFactory<>(CassandraJavaUtil.<T>typeConverter(targetClass));
    }

    /**
     * Constructs a row reader factory which maps a single column from a row to the list of objects of a
     * specified type.
     * <p/>
     * The method should be used when we have to deal with a column of some collection type (either a set
     * or a list).
     * <p/>
     * For example the following code: {@code javaFunctions(sc).cassandraTable("ks", "tab",
     * mapColumnToListOf(String.class))} returns an RDD of {@code List<String>}.
     * <p/>
     * It is a short-hand method for calling {@link #mapColumnTo(Class, Class, Class[])} with the main
     * type being {@link java.util.List} and the type parameter being the given target class.
     */
    public static <T> RowReaderFactory<List<T>> mapColumnToListOf(Class<T> targetClass) {
        return new ValueRowReaderFactory<>(CassandraJavaUtil.<List<T>>typeConverter(List.class, targetClass));
    }

    /**
     * Constructs a row reader factory which maps a single column from a row to the set of objects of a
     * specified type.
     * <p/>
     * The method should be used when we have to deal with a column of some collection type (either a set
     * or a list).
     * <p/>
     * For example the following code: {@code javaFunctions(sc).cassandraTable("ks", "tab",
     * mapColumnToSetOf(String.class))} returns an RDD of {@code Set<String>}.
     * <p/>
     * It is a short-hand method for calling {@link #mapColumnTo(Class, Class, Class[])} with the main
     * type being {@link java.util.Set} and the type parameter being the given target class.
     */
    public static <T> RowReaderFactory<Set<T>> mapColumnToSetOf(Class<T> targetClass) {
        return new ValueRowReaderFactory<>(CassandraJavaUtil.<Set<T>>typeConverter(Set.class, targetClass));
    }

    /**
     * Constructs a row reader factory which maps a single column from a row to the map of pairs of
     * specified key and value types.
     * <p/>
     * The method should be used when we have to deal with a column of a map type.
     * <p/>
     * For example the following code: {@code javaFunctions(sc).cassandraTable("ks", "tab",
     * mapColumnToMapOf(String.class, Integer.class))} returns an RDD of {@code Map<String, Integer>}.
     * <p/>
     * It is a short-hand method for calling {@link #mapColumnTo(Class, Class, Class[])} with the main
     * type being {@link java.util.Map} and the type parameters being the given target key and target
     * value classes.
     */
    public static <K, V> RowReaderFactory<Map<K, V>> mapColumnToMapOf(
            Class<K> targetKeyClass,
            Class<V> targetValueClass
    ) {
        return new ValueRowReaderFactory<>(
                CassandraJavaUtil.<Map<K, V>>typeConverter(Map.class, targetKeyClass, targetValueClass));
    }

    /**
     * Constructs a row reader factory which maps a single column from a row to the objects of a
     * specified type with given type parameters.
     * <p/>
     * It is a short-hand method for calling {@link #mapColumnTo(TypeConverter)} with a type converter
     * resolved by calling {@link #typeConverter(Class, Class, Class[])}.
     */
    @SuppressWarnings("unchecked")
    public static <T> RowReaderFactory<T> mapColumnTo(Class targetClass, Class typeParam1, Class... typeParams) {
        return new ValueRowReaderFactory<>(CassandraJavaUtil.<T>typeConverter(targetClass, typeParam1, typeParams));
    }

    /**
     * Constructs a row reader factory which maps a single column from a row using a specified type
     * converter.
     * <p/>
     * Unlike {@code mapRowTo(...)} methods, this method converts the first available column from the
     * result set. Therefore, it is strongly recommended to explicitly specify the projection on the RDD
     * in order to decrease the amount of data to be fetched and to make it explicit which column is to
     * be mapped.
     */
    public static <T> RowReaderFactory<T> mapColumnTo(TypeConverter<T> typeConverter) {
        return new ValueRowReaderFactory<>(typeConverter);
    }

    // -------------------------------------------------------------------------
    //              Entire row mappers 
    // -------------------------------------------------------------------------

    /**
     * Constructs a row reader factory which maps an entire row to an object of a specified type.
     * <p/>
     * The given target class is considered to follow the Java bean style convention. It should have a
     * public, no-args constructor, as well as a set of getters and setters. The names of the attributes
     * must follow the names of columns in the table (however, different conventions are allowed - see
     * the guide for more info). The default mapping of attributes to column names can be overridden by
     * providing a custom map of attribute-column mappings for the pairs which do not follow the general
     * convention.
     * <p/>
     * Another version of this method {@link #mapRowTo(Class, Pair[])} can be considered much more handy
     * for inline invocations.
     * <p/>
     * The method uses {@link JavaBeanColumnMapper} as the column mapper. If another column mapper has to
     * be used, see {@link #mapRowTo(Class, ColumnMapper)} method.
     */
    public static <T> RowReaderFactory<T> mapRowTo(Class<T> targetClass, Map<String, String> columnMapping) {
        TypeTags.TypeTag<T> tt = typeTag(targetClass);
        ColumnMapper<T> mapper = javaBeanColumnMapper(safeClassTag(targetClass), columnMapping);
        return new ClassBasedRowReaderFactory<>(tt, mapper);
    }

    /**
     * Constructs a row reader factory which maps an entire row to an object of a specified type.
     * <p/>
     * The given target class is considered to follow the Java bean style convention. It should have a
     * public, no-args constructor, as well as a set of getters and setters. The names of the attributes
     * must follow the names of columns in the table (however, different conventions are allowed - see
     * the guide for more info). The default mapping of attributes to column names can be changed by
     * providing a custom map of attribute-column mappings for the pairs which do not follow the general
     * convention.
     * <p/>
     * The method uses {@link JavaBeanColumnMapper} as the column mapper. If another column mapper has to
     * be used, see {@link #mapRowTo(Class, ColumnMapper)} method.
     */
    public static <T> RowReaderFactory<T> mapRowTo(Class<T> targetClass, Pair... columnMappings) {
        TypeTags.TypeTag<T> tt = typeTag(targetClass);
        ColumnMapper<T> mapper = javaBeanColumnMapper(safeClassTag(targetClass), convertToMap(columnMappings));
        return new ClassBasedRowReaderFactory<>(tt, mapper);
    }

    /**
     * Constructs a row reader factory which maps an entire row using a specified column mapper. Follow
     * the guide to find out more about column mappers.
     */
    public static <T> RowReaderFactory<T> mapRowTo(Class<T> targetClass, ColumnMapper<T> columnMapper) {
        TypeTags.TypeTag<T> tt = typeTag(targetClass);
        return new ClassBasedRowReaderFactory<>(tt, columnMapper);
    }

    // -------------------------------------------------------------------------
    //              Row to tuples mapper
    // -------------------------------------------------------------------------

    /**
     * Creates a RowReaderFactory instance for reading tuples with given parameter types.
     */
    public static <A> RowReaderFactory<Tuple1<A>> mapRowToTuple(
            Class<A> a
    ) {
        final TypeTags.TypeTag<Tuple1<A>> tupleTT =
                typeTag(Tuple1.class,
                        typeTag(a)
                );
        return new ClassBasedRowReaderFactory<>(tupleTT, tuple1ColumnMapper(a));
    }

    /**
     * Creates a RowReaderFactory instance for reading tuples with given parameter types.
     */
    public static <A, B> RowReaderFactory<Tuple2<A, B>> mapRowToTuple(
            Class<A> a,
            Class<B> b
    ) {
        final TypeTags.TypeTag<Tuple2<A, B>> tupleTT =
                typeTag(Tuple2.class,
                        typeTag(a),
                        typeTag(b)
                );
        return new ClassBasedRowReaderFactory<>(tupleTT, tuple2ColumnMapper(a, b));
    }

    /**
     * Creates a RowReaderFactory instance for reading tuples with given parameter types.
     */
    public static <A, B, C> RowReaderFactory<Tuple3<A, B, C>> mapRowToTuple(
            Class<A> a,
            Class<B> b,
            Class<C> c
    ) {
        final TypeTags.TypeTag<Tuple3<A, B, C>> tupleTT =
                typeTag(Tuple3.class,
                        typeTag(a),
                        typeTag(b),
                        typeTag(c)
                );
        return new ClassBasedRowReaderFactory<>(tupleTT, tuple3ColumnMapper(a, b, c));
    }

    /**
     * Creates a RowReaderFactory instance for reading tuples with given parameter types.
     */
    public static <A, B, C, D> RowReaderFactory<Tuple4<A, B, C, D>> mapRowToTuple(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d
    ) {
        final TypeTags.TypeTag<Tuple4<A, B, C, D>> tupleTT =
                typeTag(Tuple4.class,
                        typeTag(a),
                        typeTag(b),
                        typeTag(c),
                        typeTag(d)
                );
        return new ClassBasedRowReaderFactory<>(tupleTT, tuple4ColumnMapper(a, b, c, d));
    }

    /**
     * Creates a RowReaderFactory instance for reading tuples with given parameter types.
     */
    public static <A, B, C, D, E> RowReaderFactory<Tuple5<A, B, C, D, E>> mapRowToTuple(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e
    ) {
        final TypeTags.TypeTag<Tuple5<A, B, C, D, E>> tupleTT =
                typeTag(Tuple5.class,
                        typeTag(a),
                        typeTag(b),
                        typeTag(c),
                        typeTag(d),
                        typeTag(e)
                );
        return new ClassBasedRowReaderFactory<>(tupleTT, tuple5ColumnMapper(a, b, c, d, e));
    }

    /**
     * Creates a RowReaderFactory instance for reading tuples with given parameter types.
     */
    public static <A, B, C, D, E, F> RowReaderFactory<Tuple6<A, B, C, D, E, F>> mapRowToTuple(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f
    ) {
        final TypeTags.TypeTag<Tuple6<A, B, C, D, E, F>> tupleTT =
                typeTag(Tuple6.class,
                        typeTag(a),
                        typeTag(b),
                        typeTag(c),
                        typeTag(d),
                        typeTag(e),
                        typeTag(f)
                );
        return new ClassBasedRowReaderFactory<>(tupleTT, tuple6ColumnMapper(a, b, c, d, e, f));
    }

    /**
     * Creates a RowReaderFactory instance for reading tuples with given parameter types.
     */
    public static <A, B, C, D, E, F, G> RowReaderFactory<Tuple7<A, B, C, D, E, F, G>> mapRowToTuple(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f,
            Class<G> g
    ) {
        final TypeTags.TypeTag<Tuple7<A, B, C, D, E, F, G>> tupleTT =
                typeTag(Tuple7.class,
                        typeTag(a),
                        typeTag(b),
                        typeTag(c),
                        typeTag(d),
                        typeTag(e),
                        typeTag(f),
                        typeTag(g)
                );
        return new ClassBasedRowReaderFactory<>(tupleTT, tuple7ColumnMapper(a, b, c, d, e, f, g));
    }

    /**
     * Creates a RowReaderFactory instance for reading tuples with given parameter types.
     */
    public static <A, B, C, D, E, F, G, H> RowReaderFactory<Tuple8<A, B, C, D, E, F, G, H>> mapRowToTuple(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f,
            Class<G> g,
            Class<H> h
    ) {
        final TypeTags.TypeTag<Tuple8<A, B, C, D, E, F, G, H>> tupleTT =
                typeTag(Tuple8.class,
                        typeTag(a),
                        typeTag(b),
                        typeTag(c),
                        typeTag(d),
                        typeTag(e),
                        typeTag(f),
                        typeTag(g),
                        typeTag(h)
                );
        return new ClassBasedRowReaderFactory<>(tupleTT, tuple8ColumnMapper(a, b, c, d, e, f, g, h));
    }

    /**
     * Creates a RowReaderFactory instance for reading tuples with given parameter types.
     */
    public static <A, B, C, D, E, F, G, H, I> RowReaderFactory<Tuple9<A, B, C, D, E, F, G, H, I>> mapRowToTuple(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f,
            Class<G> g,
            Class<H> h,
            Class<I> i
    ) {
        final TypeTags.TypeTag<Tuple9<A, B, C, D, E, F, G, H, I>> tupleTT =
                typeTag(Tuple9.class,
                        typeTag(a),
                        typeTag(b),
                        typeTag(c),
                        typeTag(d),
                        typeTag(e),
                        typeTag(f),
                        typeTag(g),
                        typeTag(h),
                        typeTag(i)
                );
        return new ClassBasedRowReaderFactory<>(tupleTT, tuple9ColumnMapper(a, b, c, d, e, f, g, h, i));
    }

    /**
     * Creates a RowReaderFactory instance for reading tuples with given parameter types.
     */
    public static <A, B, C, D, E, F, G, H, I, J> RowReaderFactory<Tuple10<A, B, C, D, E, F, G, H, I, J>> mapRowToTuple(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f,
            Class<G> g,
            Class<H> h,
            Class<I> i,
            Class<J> j
    ) {
        final TypeTags.TypeTag<Tuple10<A, B, C, D, E, F, G, H, I, J>> tupleTT =
                typeTag(Tuple10.class,
                        typeTag(a),
                        typeTag(b),
                        typeTag(c),
                        typeTag(d),
                        typeTag(e),
                        typeTag(f),
                        typeTag(g),
                        typeTag(h),
                        typeTag(i),
                        typeTag(j)
                );
        return new ClassBasedRowReaderFactory<>(tupleTT, tuple10ColumnMapper(a, b, c, d, e, f, g, h, i, j));
    }

    /**
     * Creates a RowReaderFactory instance for reading tuples with given parameter types.
     */
    public static <A, B, C, D, E, F, G, H, I, J, K> RowReaderFactory<Tuple11<A, B, C, D, E, F, G, H, I, J, K>> mapRowToTuple(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f,
            Class<G> g,
            Class<H> h,
            Class<I> i,
            Class<J> j,
            Class<K> k
    ) {
        final TypeTags.TypeTag<Tuple11<A, B, C, D, E, F, G, H, I, J, K>> tupleTT =
                typeTag(Tuple11.class,
                        typeTag(a),
                        typeTag(b),
                        typeTag(c),
                        typeTag(d),
                        typeTag(e),
                        typeTag(f),
                        typeTag(g),
                        typeTag(h),
                        typeTag(i),
                        typeTag(j),
                        typeTag(k)
                );
        return new ClassBasedRowReaderFactory<>(tupleTT, tuple11ColumnMapper(a, b, c, d, e, f, g, h, i, j, k));
    }

    /**
     * Creates a RowReaderFactory instance for reading tuples with given parameter types.
     */
    public static <A, B, C, D, E, F, G, H, I, J, K, L> RowReaderFactory<Tuple12<A, B, C, D, E, F, G, H, I, J, K, L>> mapRowToTuple(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f,
            Class<G> g,
            Class<H> h,
            Class<I> i,
            Class<J> j,
            Class<K> k,
            Class<L> l
    ) {
        final TypeTags.TypeTag<Tuple12<A, B, C, D, E, F, G, H, I, J, K, L>> tupleTT =
                typeTag(Tuple12.class,
                        typeTag(a),
                        typeTag(b),
                        typeTag(c),
                        typeTag(d),
                        typeTag(e),
                        typeTag(f),
                        typeTag(g),
                        typeTag(h),
                        typeTag(i),
                        typeTag(j),
                        typeTag(k),
                        typeTag(l)
                );
        return new ClassBasedRowReaderFactory<>(tupleTT, tuple12ColumnMapper(a, b, c, d, e, f, g, h, i, j, k, l));
    }

    /**
     * Creates a RowReaderFactory instance for reading tuples with given parameter types.
     */
    public static <A, B, C, D, E, F, G, H, I, J, K, L, M> RowReaderFactory<Tuple13<A, B, C, D, E, F, G, H, I, J, K, L, M>> mapRowToTuple(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f,
            Class<G> g,
            Class<H> h,
            Class<I> i,
            Class<J> j,
            Class<K> k,
            Class<L> l,
            Class<M> m
    ) {
        final TypeTags.TypeTag<Tuple13<A, B, C, D, E, F, G, H, I, J, K, L, M>> tupleTT =
                typeTag(Tuple13.class,
                        typeTag(a),
                        typeTag(b),
                        typeTag(c),
                        typeTag(d),
                        typeTag(e),
                        typeTag(f),
                        typeTag(g),
                        typeTag(h),
                        typeTag(i),
                        typeTag(j),
                        typeTag(k),
                        typeTag(l),
                        typeTag(m)
                );
        return new ClassBasedRowReaderFactory<>(tupleTT, tuple13ColumnMapper(a, b, c, d, e, f, g, h, i, j, k, l, m));
    }

    /**
     * Creates a RowReaderFactory instance for reading tuples with given parameter types.
     */
    public static <A, B, C, D, E, F, G, H, I, J, K, L, M, N> RowReaderFactory<Tuple14<A, B, C, D, E, F, G, H, I, J, K, L, M, N>> mapRowToTuple(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f,
            Class<G> g,
            Class<H> h,
            Class<I> i,
            Class<J> j,
            Class<K> k,
            Class<L> l,
            Class<M> m,
            Class<N> n
    ) {
        final TypeTags.TypeTag<Tuple14<A, B, C, D, E, F, G, H, I, J, K, L, M, N>> tupleTT =
                typeTag(Tuple14.class,
                        typeTag(a),
                        typeTag(b),
                        typeTag(c),
                        typeTag(d),
                        typeTag(e),
                        typeTag(f),
                        typeTag(g),
                        typeTag(h),
                        typeTag(i),
                        typeTag(j),
                        typeTag(k),
                        typeTag(l),
                        typeTag(m),
                        typeTag(n)
                );
        return new ClassBasedRowReaderFactory<>(tupleTT, tuple14ColumnMapper(a, b, c, d, e, f, g, h, i, j, k, l, m, n));
    }

    /**
     * Creates a RowReaderFactory instance for reading tuples with given parameter types.
     */
    public static <A, B, C, D, E, F, G, H, I, J, K, L, M, N, O> RowReaderFactory<Tuple15<A, B, C, D, E, F, G, H, I, J, K, L, M, N, O>> mapRowToTuple(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f,
            Class<G> g,
            Class<H> h,
            Class<I> i,
            Class<J> j,
            Class<K> k,
            Class<L> l,
            Class<M> m,
            Class<N> n,
            Class<O> o
    ) {
        final TypeTags.TypeTag<Tuple15<A, B, C, D, E, F, G, H, I, J, K, L, M, N, O>> tupleTT =
                typeTag(Tuple15.class,
                        typeTag(a),
                        typeTag(b),
                        typeTag(c),
                        typeTag(d),
                        typeTag(e),
                        typeTag(f),
                        typeTag(g),
                        typeTag(h),
                        typeTag(i),
                        typeTag(j),
                        typeTag(k),
                        typeTag(l),
                        typeTag(m),
                        typeTag(n),
                        typeTag(o)
                );
        return new ClassBasedRowReaderFactory<>(tupleTT, tuple15ColumnMapper(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o));
    }

    /**
     * Creates a RowReaderFactory instance for reading tuples with given parameter types.
     */
    public static <A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P> RowReaderFactory<Tuple16<A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P>> mapRowToTuple(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f,
            Class<G> g,
            Class<H> h,
            Class<I> i,
            Class<J> j,
            Class<K> k,
            Class<L> l,
            Class<M> m,
            Class<N> n,
            Class<O> o,
            Class<P> p
    ) {
        final TypeTags.TypeTag<Tuple16<A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P>> tupleTT =
                typeTag(Tuple16.class,
                        typeTag(a),
                        typeTag(b),
                        typeTag(c),
                        typeTag(d),
                        typeTag(e),
                        typeTag(f),
                        typeTag(g),
                        typeTag(h),
                        typeTag(i),
                        typeTag(j),
                        typeTag(k),
                        typeTag(l),
                        typeTag(m),
                        typeTag(n),
                        typeTag(o),
                        typeTag(p)
                );
        return new ClassBasedRowReaderFactory<>(tupleTT, tuple16ColumnMapper(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p));
    }

    /**
     * Creates a RowReaderFactory instance for reading tuples with given parameter types.
     */
    public static <A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q> RowReaderFactory<Tuple17<A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q>> mapRowToTuple(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f,
            Class<G> g,
            Class<H> h,
            Class<I> i,
            Class<J> j,
            Class<K> k,
            Class<L> l,
            Class<M> m,
            Class<N> n,
            Class<O> o,
            Class<P> p,
            Class<Q> q
    ) {
        final TypeTags.TypeTag<Tuple17<A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q>> tupleTT =
                typeTag(Tuple17.class,
                        typeTag(a),
                        typeTag(b),
                        typeTag(c),
                        typeTag(d),
                        typeTag(e),
                        typeTag(f),
                        typeTag(g),
                        typeTag(h),
                        typeTag(i),
                        typeTag(j),
                        typeTag(k),
                        typeTag(l),
                        typeTag(m),
                        typeTag(n),
                        typeTag(o),
                        typeTag(p),
                        typeTag(q)
                );
        return new ClassBasedRowReaderFactory<>(tupleTT, tuple17ColumnMapper(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q));
    }

    /**
     * Creates a RowReaderFactory instance for reading tuples with given parameter types.
     */
    public static <A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R> RowReaderFactory<Tuple18<A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R>> mapRowToTuple(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f,
            Class<G> g,
            Class<H> h,
            Class<I> i,
            Class<J> j,
            Class<K> k,
            Class<L> l,
            Class<M> m,
            Class<N> n,
            Class<O> o,
            Class<P> p,
            Class<Q> q,
            Class<R> r
    ) {
        final TypeTags.TypeTag<Tuple18<A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R>> tupleTT =
                typeTag(Tuple18.class,
                        typeTag(a),
                        typeTag(b),
                        typeTag(c),
                        typeTag(d),
                        typeTag(e),
                        typeTag(f),
                        typeTag(g),
                        typeTag(h),
                        typeTag(i),
                        typeTag(j),
                        typeTag(k),
                        typeTag(l),
                        typeTag(m),
                        typeTag(n),
                        typeTag(o),
                        typeTag(p),
                        typeTag(q),
                        typeTag(r)
                );
        return new ClassBasedRowReaderFactory<>(tupleTT, tuple18ColumnMapper(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r));
    }

    /**
     * Creates a RowReaderFactory instance for reading tuples with given parameter types.
     */
    public static <A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S> RowReaderFactory<Tuple19<A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S>> mapRowToTuple(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f,
            Class<G> g,
            Class<H> h,
            Class<I> i,
            Class<J> j,
            Class<K> k,
            Class<L> l,
            Class<M> m,
            Class<N> n,
            Class<O> o,
            Class<P> p,
            Class<Q> q,
            Class<R> r,
            Class<S> s
    ) {
        final TypeTags.TypeTag<Tuple19<A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S>> tupleTT =
                typeTag(Tuple19.class,
                        typeTag(a),
                        typeTag(b),
                        typeTag(c),
                        typeTag(d),
                        typeTag(e),
                        typeTag(f),
                        typeTag(g),
                        typeTag(h),
                        typeTag(i),
                        typeTag(j),
                        typeTag(k),
                        typeTag(l),
                        typeTag(m),
                        typeTag(n),
                        typeTag(o),
                        typeTag(p),
                        typeTag(q),
                        typeTag(r),
                        typeTag(s)
                );
        return new ClassBasedRowReaderFactory<>(tupleTT, tuple19ColumnMapper(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s));
    }

    /**
     * Creates a RowReaderFactory instance for reading tuples with given parameter types.
     */
    public static <A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T> RowReaderFactory<Tuple20<A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T>> mapRowToTuple(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f,
            Class<G> g,
            Class<H> h,
            Class<I> i,
            Class<J> j,
            Class<K> k,
            Class<L> l,
            Class<M> m,
            Class<N> n,
            Class<O> o,
            Class<P> p,
            Class<Q> q,
            Class<R> r,
            Class<S> s,
            Class<T> t
    ) {
        final TypeTags.TypeTag<Tuple20<A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T>> tupleTT =
                typeTag(Tuple20.class,
                        typeTag(a),
                        typeTag(b),
                        typeTag(c),
                        typeTag(d),
                        typeTag(e),
                        typeTag(f),
                        typeTag(g),
                        typeTag(h),
                        typeTag(i),
                        typeTag(j),
                        typeTag(k),
                        typeTag(l),
                        typeTag(m),
                        typeTag(n),
                        typeTag(o),
                        typeTag(p),
                        typeTag(q),
                        typeTag(r),
                        typeTag(s),
                        typeTag(t)
                );
        return new ClassBasedRowReaderFactory<>(tupleTT, tuple20ColumnMapper(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t));
    }

    /**
     * Creates a RowReaderFactory instance for reading tuples with given parameter types.
     */
    public static <A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U> RowReaderFactory<Tuple21<A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U>> mapRowToTuple(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f,
            Class<G> g,
            Class<H> h,
            Class<I> i,
            Class<J> j,
            Class<K> k,
            Class<L> l,
            Class<M> m,
            Class<N> n,
            Class<O> o,
            Class<P> p,
            Class<Q> q,
            Class<R> r,
            Class<S> s,
            Class<T> t,
            Class<U> u
    ) {
        final TypeTags.TypeTag<Tuple21<A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U>> tupleTT =
                typeTag(Tuple21.class,
                        typeTag(a),
                        typeTag(b),
                        typeTag(c),
                        typeTag(d),
                        typeTag(e),
                        typeTag(f),
                        typeTag(g),
                        typeTag(h),
                        typeTag(i),
                        typeTag(j),
                        typeTag(k),
                        typeTag(l),
                        typeTag(m),
                        typeTag(n),
                        typeTag(o),
                        typeTag(p),
                        typeTag(q),
                        typeTag(r),
                        typeTag(s),
                        typeTag(t),
                        typeTag(u)
                );
        return new ClassBasedRowReaderFactory<>(tupleTT, tuple21ColumnMapper(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u));
    }

    /**
     * Creates a RowReaderFactory instance for reading tuples with given parameter types.
     */
    public static <A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V> RowReaderFactory<Tuple22<A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V>> mapRowToTuple(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f,
            Class<G> g,
            Class<H> h,
            Class<I> i,
            Class<J> j,
            Class<K> k,
            Class<L> l,
            Class<M> m,
            Class<N> n,
            Class<O> o,
            Class<P> p,
            Class<Q> q,
            Class<R> r,
            Class<S> s,
            Class<T> t,
            Class<U> u,
            Class<V> v
    ) {
        final TypeTags.TypeTag<Tuple22<A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V>> tupleTT =
                typeTag(Tuple22.class,
                        typeTag(a),
                        typeTag(b),
                        typeTag(c),
                        typeTag(d),
                        typeTag(e),
                        typeTag(f),
                        typeTag(g),
                        typeTag(h),
                        typeTag(i),
                        typeTag(j),
                        typeTag(k),
                        typeTag(l),
                        typeTag(m),
                        typeTag(n),
                        typeTag(o),
                        typeTag(p),
                        typeTag(q),
                        typeTag(r),
                        typeTag(s),
                        typeTag(t),
                        typeTag(u),
                        typeTag(v)
                );
        return new ClassBasedRowReaderFactory<>(tupleTT, tuple22ColumnMapper(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v));
    }

    // -------------------------------------------------------------------------
    //              Row writer factory creation methods 
    // -------------------------------------------------------------------------

    /**
     * Creates a row writer factory which can map objects supported by a given column mapper to columns
     * in a table.
     */
    @SuppressWarnings("unchecked")
    public static <T> RowWriterFactory<T> mapToRow(Class<?> targetClass, ColumnMapper<T> columnMapper) {
        TypeTags.TypeTag<T> tt = typeTag(targetClass);
        return defaultRowWriterFactory(tt, columnMapper);
    }

    /**
     * Creates a row writer factory which can map objects supported by a given column mapper to columns
     * in a table.
     */
    public static <T> RowWriterFactory<T> safeMapToRow(Class<T> targetClass, ColumnMapper<T> columnMapper) {
        TypeTags.TypeTag<T> tt = safeTypeTag(targetClass);
        return defaultRowWriterFactory(tt, columnMapper);
    }

    /**
     * Creates a row writer factory which maps objects of a given class type to columns in table.
     * <p/>
     * The given source class is considered to follow the Java bean style convention. It should have a
     * public, no-args constructor, as well as a set of getters and setters. The names of the attributes
     * must follow the names of columns in the table (however, different conventions are allowed - see
     * the guide for more info). The default mapping of attributes to column names can be changed by
     * providing a custom map of attribute-column mappings for the pairs which do not follow the general
     * convention.
     * <p/>
     * Another version of this method {@link #mapToRow(Class, Pair[])} can be considered much more handy
     * for inline invocations.
     * <p/>
     * The method uses {@link JavaBeanColumnMapper} as the column mapper. If another column mapper has to
     * be used, see {@link #mapToRow(Class, ColumnMapper)} method.
     */
    public static <T> RowWriterFactory<T> mapToRow(
            Class<T> sourceClass, Map<String, String> columnNameMappings
    ) {
        ColumnMapper<T> mapper = javaBeanColumnMapper(safeClassTag(sourceClass), columnNameMappings);
        return mapToRow(sourceClass, mapper);
    }

    /**
     * Creates a row writer factory which maps objects of a given class type to columns in table.
     * <p/>
     * The given source class is considered to follow the Java bean style convention. It should have a
     * public, no-args constructor, as well as a set of getters and setters. The names of the attributes
     * must follow the names of columns in the table (however, different conventions are allowed - see
     * the guide for more info). The default mapping of attributes to column names can be changed by
     * providing a custom map of attribute-column mappings for the pairs which do not follow the general
     * convention.
     * <p/>
     * The method uses {@link JavaBeanColumnMapper} as the column mapper. If another column mapper has to
     * be used, see {@link #mapToRow(Class, ColumnMapper)} method.
     */
    public static <T> RowWriterFactory<T> mapToRow(Class<T> sourceClass, Pair... columnNameMappings) {
        return mapToRow(sourceClass, convertToMap(columnNameMappings));
    }

    // -------------------------------------------------------------------------
    //              Tuples to row mapper
    // -------------------------------------------------------------------------

    /**
     * Creates a RowWriterFactory instance for writing tuples with given parameter types.
     */
    public static <A> RowWriterFactory<Tuple1<A>> mapTupleToRow(
            Class<A> a
    ) {
        return mapToRow(Tuple1.class, tuple1ColumnMapper(a));
    }

    /**
     * Creates a RowWriterFactory instance for writing tuples with given parameter types.
     */
    public static <A, B> RowWriterFactory<Tuple2<A, B>> mapTupleToRow(
            Class<A> a,
            Class<B> b
    ) {
        return mapToRow(Tuple2.class, tuple2ColumnMapper(a, b));
    }

    /**
     * Creates a RowWriterFactory instance for writing tuples with given parameter types.
     */
    public static <A, B, C> RowWriterFactory<Tuple3<A, B, C>> mapTupleToRow(
            Class<A> a,
            Class<B> b,
            Class<C> c
    ) {
        return mapToRow(Tuple3.class, tuple3ColumnMapper(a, b, c));
    }

    /**
     * Creates a RowWriterFactory instance for writing tuples with given parameter types.
     */
    public static <A, B, C, D> RowWriterFactory<Tuple4<A, B, C, D>> mapTupleToRow(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d
    ) {
        return mapToRow(Tuple4.class, tuple4ColumnMapper(a, b, c, d));
    }

    /**
     * Creates a RowWriterFactory instance for writing tuples with given parameter types.
     */
    public static <A, B, C, D, E> RowWriterFactory<Tuple5<A, B, C, D, E>> mapTupleToRow(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e
    ) {
        return mapToRow(Tuple5.class, tuple5ColumnMapper(a, b, c, d, e));
    }

    /**
     * Creates a RowWriterFactory instance for writing tuples with given parameter types.
     */
    public static <A, B, C, D, E, F> RowWriterFactory<Tuple6<A, B, C, D, E, F>> mapTupleToRow(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f
    ) {
        return mapToRow(Tuple6.class, tuple6ColumnMapper(a, b, c, d, e, f));
    }

    /**
     * Creates a RowWriterFactory instance for writing tuples with given parameter types.
     */
    public static <A, B, C, D, E, F, G> RowWriterFactory<Tuple7<A, B, C, D, E, F, G>> mapTupleToRow(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f,
            Class<G> g
    ) {
        return mapToRow(Tuple7.class, tuple7ColumnMapper(a, b, c, d, e, f, g));
    }

    /**
     * Creates a RowWriterFactory instance for writing tuples with given parameter types.
     */
    public static <A, B, C, D, E, F, G, H> RowWriterFactory<Tuple8<A, B, C, D, E, F, G, H>> mapTupleToRow(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f,
            Class<G> g,
            Class<H> h
    ) {
        return mapToRow(Tuple8.class, tuple8ColumnMapper(a, b, c, d, e, f, g, h));
    }

    /**
     * Creates a RowWriterFactory instance for writing tuples with given parameter types.
     */
    public static <A, B, C, D, E, F, G, H, I> RowWriterFactory<Tuple9<A, B, C, D, E, F, G, H, I>> mapTupleToRow(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f,
            Class<G> g,
            Class<H> h,
            Class<I> i
    ) {
        return mapToRow(Tuple9.class, tuple9ColumnMapper(a, b, c, d, e, f, g, h, i));
    }

    /**
     * Creates a RowWriterFactory instance for writing tuples with given parameter types.
     */
    public static <A, B, C, D, E, F, G, H, I, J> RowWriterFactory<Tuple10<A, B, C, D, E, F, G, H, I, J>> mapTupleToRow(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f,
            Class<G> g,
            Class<H> h,
            Class<I> i,
            Class<J> j
    ) {
        return mapToRow(Tuple10.class, tuple10ColumnMapper(a, b, c, d, e, f, g, h, i, j));
    }

    /**
     * Creates a RowWriterFactory instance for writing tuples with given parameter types.
     */
    public static <A, B, C, D, E, F, G, H, I, J, K> RowWriterFactory<Tuple11<A, B, C, D, E, F, G, H, I, J, K>> mapTupleToRow(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f,
            Class<G> g,
            Class<H> h,
            Class<I> i,
            Class<J> j,
            Class<K> k
    ) {
        return mapToRow(Tuple11.class, tuple11ColumnMapper(a, b, c, d, e, f, g, h, i, j, k));
    }

    /**
     * Creates a RowWriterFactory instance for writing tuples with given parameter types.
     */
    public static <A, B, C, D, E, F, G, H, I, J, K, L> RowWriterFactory<Tuple12<A, B, C, D, E, F, G, H, I, J, K, L>> mapTupleToRow(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f,
            Class<G> g,
            Class<H> h,
            Class<I> i,
            Class<J> j,
            Class<K> k,
            Class<L> l
    ) {
        return mapToRow(Tuple12.class, tuple12ColumnMapper(a, b, c, d, e, f, g, h, i, j, k, l));
    }

    /**
     * Creates a RowWriterFactory instance for writing tuples with given parameter types.
     */
    public static <A, B, C, D, E, F, G, H, I, J, K, L, M> RowWriterFactory<Tuple13<A, B, C, D, E, F, G, H, I, J, K, L, M>> mapTupleToRow(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f,
            Class<G> g,
            Class<H> h,
            Class<I> i,
            Class<J> j,
            Class<K> k,
            Class<L> l,
            Class<M> m
    ) {
        return mapToRow(Tuple13.class, tuple13ColumnMapper(a, b, c, d, e, f, g, h, i, j, k, l, m));
    }

    /**
     * Creates a RowWriterFactory instance for writing tuples with given parameter types.
     */
    public static <A, B, C, D, E, F, G, H, I, J, K, L, M, N> RowWriterFactory<Tuple14<A, B, C, D, E, F, G, H, I, J, K, L, M, N>> mapTupleToRow(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f,
            Class<G> g,
            Class<H> h,
            Class<I> i,
            Class<J> j,
            Class<K> k,
            Class<L> l,
            Class<M> m,
            Class<N> n
    ) {
        return mapToRow(Tuple14.class, tuple14ColumnMapper(a, b, c, d, e, f, g, h, i, j, k, l, m, n));
    }

    /**
     * Creates a RowWriterFactory instance for writing tuples with given parameter types.
     */
    public static <A, B, C, D, E, F, G, H, I, J, K, L, M, N, O> RowWriterFactory<Tuple15<A, B, C, D, E, F, G, H, I, J, K, L, M, N, O>> mapTupleToRow(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f,
            Class<G> g,
            Class<H> h,
            Class<I> i,
            Class<J> j,
            Class<K> k,
            Class<L> l,
            Class<M> m,
            Class<N> n,
            Class<O> o
    ) {
        return mapToRow(Tuple15.class, tuple15ColumnMapper(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o));
    }

    /**
     * Creates a RowWriterFactory instance for writing tuples with given parameter types.
     */
    public static <A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P> RowWriterFactory<Tuple16<A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P>> mapTupleToRow(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f,
            Class<G> g,
            Class<H> h,
            Class<I> i,
            Class<J> j,
            Class<K> k,
            Class<L> l,
            Class<M> m,
            Class<N> n,
            Class<O> o,
            Class<P> p
    ) {
        return mapToRow(Tuple16.class, tuple16ColumnMapper(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p));
    }

    /**
     * Creates a RowWriterFactory instance for writing tuples with given parameter types.
     */
    public static <A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q> RowWriterFactory<Tuple17<A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q>> mapTupleToRow(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f,
            Class<G> g,
            Class<H> h,
            Class<I> i,
            Class<J> j,
            Class<K> k,
            Class<L> l,
            Class<M> m,
            Class<N> n,
            Class<O> o,
            Class<P> p,
            Class<Q> q
    ) {
        return mapToRow(Tuple17.class, tuple17ColumnMapper(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q));
    }

    /**
     * Creates a RowWriterFactory instance for writing tuples with given parameter types.
     */
    public static <A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R> RowWriterFactory<Tuple18<A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R>> mapTupleToRow(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f,
            Class<G> g,
            Class<H> h,
            Class<I> i,
            Class<J> j,
            Class<K> k,
            Class<L> l,
            Class<M> m,
            Class<N> n,
            Class<O> o,
            Class<P> p,
            Class<Q> q,
            Class<R> r
    ) {
        return mapToRow(Tuple18.class, tuple18ColumnMapper(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r));
    }

    /**
     * Creates a RowWriterFactory instance for writing tuples with given parameter types.
     */
    public static <A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S> RowWriterFactory<Tuple19<A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S>> mapTupleToRow(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f,
            Class<G> g,
            Class<H> h,
            Class<I> i,
            Class<J> j,
            Class<K> k,
            Class<L> l,
            Class<M> m,
            Class<N> n,
            Class<O> o,
            Class<P> p,
            Class<Q> q,
            Class<R> r,
            Class<S> s
    ) {
        return mapToRow(Tuple19.class, tuple19ColumnMapper(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s));
    }

    /**
     * Creates a RowWriterFactory instance for writing tuples with given parameter types.
     */
    public static <A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T> RowWriterFactory<Tuple20<A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T>> mapTupleToRow(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f,
            Class<G> g,
            Class<H> h,
            Class<I> i,
            Class<J> j,
            Class<K> k,
            Class<L> l,
            Class<M> m,
            Class<N> n,
            Class<O> o,
            Class<P> p,
            Class<Q> q,
            Class<R> r,
            Class<S> s,
            Class<T> t
    ) {
        return mapToRow(Tuple20.class, tuple20ColumnMapper(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t));
    }

    /**
     * Creates a RowWriterFactory instance for writing tuples with given parameter types.
     */
    public static <A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U> RowWriterFactory<Tuple21<A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U>> mapTupleToRow(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f,
            Class<G> g,
            Class<H> h,
            Class<I> i,
            Class<J> j,
            Class<K> k,
            Class<L> l,
            Class<M> m,
            Class<N> n,
            Class<O> o,
            Class<P> p,
            Class<Q> q,
            Class<R> r,
            Class<S> s,
            Class<T> t,
            Class<U> u
    ) {
        return mapToRow(Tuple21.class, tuple21ColumnMapper(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u));
    }

    /**
     * Creates a RowWriterFactory instance for writing tuples with given parameter types.
     */
    public static <A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V> RowWriterFactory<Tuple22<A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V>> mapTupleToRow(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f,
            Class<G> g,
            Class<H> h,
            Class<I> i,
            Class<J> j,
            Class<K> k,
            Class<L> l,
            Class<M> m,
            Class<N> n,
            Class<O> o,
            Class<P> p,
            Class<Q> q,
            Class<R> r,
            Class<S> s,
            Class<T> t,
            Class<U> u,
            Class<V> v
    ) {
        return mapToRow(Tuple22.class, tuple22ColumnMapper(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v));
    }

    // -------------------------------------------------------------------------
    //              Batch size factory methods and constants 
    // -------------------------------------------------------------------------

    /**
     * The default automatic batch size.
     */
    public static final BatchSize automaticBatchSize = BatchSize$.MODULE$.Automatic();

    /**
     * Creates a batch size which bases on the given number of rows - the total batch size in bytes
     * doesn't matter in this case.
     */
    public static BatchSize rowsInBatch(int batchSizeInRows) {
        return RowsInBatch$.MODULE$.apply(batchSizeInRows);
    }

    /**
     * Creates a batch size which bases on total size in bytes - the number of rows doesn't matter in
     * this case.
     */
    public static BatchSize bytesInBatch(int batchSizeInBytes) {
        return BytesInBatch$.MODULE$.apply(batchSizeInBytes);
    }

    public static final BatchGroupingKey BATCH_GROUPING_KEY_NONE = BatchGroupingKey.None$.MODULE$;
    public static final BatchGroupingKey BATCH_GROUPING_KEY_PARTITION = BatchGroupingKey.Partition$.MODULE$;
    public static final BatchGroupingKey BATCH_GROUPING_KEY_REPLICA_SET = BatchGroupingKey.ReplicaSet$.MODULE$;

    // -------------------------------------------------------------------------
    //              Column selector factory methods and constants 
    // -------------------------------------------------------------------------

    /**
     * Column selector to select all the columns from a table.
     */
    public static final ColumnSelector allColumns = AllColumns$.MODULE$;

    /**
     * Creates a column selector with a given columns projection.
     */
    public static ColumnSelector someColumns(String... columnNames) {
        ColumnRef[] columnsSelection = new ColumnRef[columnNames.length];
        for (int i = 0; i < columnNames.length; i++) {
            columnsSelection[i] = ColumnName$.MODULE$.apply(columnNames[i], Option.<String>empty());
        }

        return SomeColumns$.MODULE$.apply(JavaApiHelper.<ColumnRef>toScalaImmutableSeq(columnsSelection));
    }

    public static ColumnName column(String columnName) {
        return new ColumnName(columnName, Option.<String>empty());
    }

    public static TTL ttl(String columnName) {
        return new TTL(columnName, Option.<String>empty());
    }

    public static WriteTime writeTime(String columnName) {
        return new WriteTime(columnName, Option.<String>empty());
    }

    public static ColumnRef[] toSelectableColumnRefs(String... columnNames) {
        ColumnName[] refs = new ColumnName[columnNames.length];
        for (int i = 0; i < columnNames.length; i++) {
            refs[i] = column(columnNames[i]);
        }

        return refs;
    }

    // -------------------------------------------------------------------------
    //              Utility methods 
    // -------------------------------------------------------------------------

    /**
     * A simple method that converts an array of pairs into a map.
     */
    public static Map<String, String> convertToMap(Pair[] pairs) {
        Map<String, String> map = new HashMap<>();
        for (Pair pair : pairs)
            map.put(String.valueOf(pair.getKey()), String.valueOf(pair.getValue()));

        return map;
    }

    /**
     * A simple method which wraps the given connector instance into Option.
     */
    public static Option<CassandraConnector> connector(CassandraConnector connector) {
        return Option.apply(connector);
    }

    // -------------------------------------------------------------------------
    //              Tuple column mappers
    // -------------------------------------------------------------------------

    /**
     * Provides a column mapper for the tuple with given parameter types.
     */
    public static <A> TupleColumnMapper<Tuple1<A>> tuple1ColumnMapper(
            Class<A> a
    ) {
        final TypeTags.TypeTag<Tuple1<A>> typeTag =
                typeTag(Tuple1.class,
                        typeTag(a)
                );
        return new TupleColumnMapper<>(typeTag);
    }

    /**
     * Provides a column mapper for the tuple with given parameter types.
     */
    public static <A, B> TupleColumnMapper<Tuple2<A, B>> tuple2ColumnMapper(
            Class<A> a,
            Class<B> b
    ) {
        final TypeTags.TypeTag<Tuple2<A, B>> typeTag =
                typeTag(Tuple2.class,
                        typeTag(a),
                        typeTag(b)
                );
        return new TupleColumnMapper<>(typeTag);
    }

    /**
     * Provides a column mapper for the tuple with given parameter types.
     */
    public static <A, B, C> TupleColumnMapper<Tuple3<A, B, C>> tuple3ColumnMapper(
            Class<A> a,
            Class<B> b,
            Class<C> c
    ) {
        final TypeTags.TypeTag<Tuple3<A, B, C>> typeTag =
                typeTag(Tuple3.class,
                        typeTag(a),
                        typeTag(b),
                        typeTag(c)
                );
        return new TupleColumnMapper<>(typeTag);
    }

    /**
     * Provides a column mapper for the tuple with given parameter types.
     */
    public static <A, B, C, D> TupleColumnMapper<Tuple4<A, B, C, D>> tuple4ColumnMapper(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d
    ) {
        final TypeTags.TypeTag<Tuple4<A, B, C, D>> typeTag =
                typeTag(Tuple4.class,
                        typeTag(a),
                        typeTag(b),
                        typeTag(c),
                        typeTag(d)
                );
        return new TupleColumnMapper<>(typeTag);
    }

    /**
     * Provides a column mapper for the tuple with given parameter types.
     */
    public static <A, B, C, D, E> TupleColumnMapper<Tuple5<A, B, C, D, E>> tuple5ColumnMapper(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e
    ) {
        final TypeTags.TypeTag<Tuple5<A, B, C, D, E>> typeTag =
                typeTag(Tuple5.class,
                        typeTag(a),
                        typeTag(b),
                        typeTag(c),
                        typeTag(d),
                        typeTag(e)
                );
        return new TupleColumnMapper<>(typeTag);
    }

    /**
     * Provides a column mapper for the tuple with given parameter types.
     */
    public static <A, B, C, D, E, F> TupleColumnMapper<Tuple6<A, B, C, D, E, F>> tuple6ColumnMapper(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f
    ) {
        final TypeTags.TypeTag<Tuple6<A, B, C, D, E, F>> typeTag =
                typeTag(Tuple6.class,
                        typeTag(a),
                        typeTag(b),
                        typeTag(c),
                        typeTag(d),
                        typeTag(e),
                        typeTag(f)
                );
        return new TupleColumnMapper<>(typeTag);
    }

    /**
     * Provides a column mapper for the tuple with given parameter types.
     */
    public static <A, B, C, D, E, F, G> TupleColumnMapper<Tuple7<A, B, C, D, E, F, G>> tuple7ColumnMapper(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f,
            Class<G> g
    ) {
        final TypeTags.TypeTag<Tuple7<A, B, C, D, E, F, G>> typeTag =
                typeTag(Tuple7.class,
                        typeTag(a),
                        typeTag(b),
                        typeTag(c),
                        typeTag(d),
                        typeTag(e),
                        typeTag(f),
                        typeTag(g)
                );
        return new TupleColumnMapper<>(typeTag);
    }

    /**
     * Provides a column mapper for the tuple with given parameter types.
     */
    public static <A, B, C, D, E, F, G, H> TupleColumnMapper<Tuple8<A, B, C, D, E, F, G, H>> tuple8ColumnMapper(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f,
            Class<G> g,
            Class<H> h
    ) {
        final TypeTags.TypeTag<Tuple8<A, B, C, D, E, F, G, H>> typeTag =
                typeTag(Tuple8.class,
                        typeTag(a),
                        typeTag(b),
                        typeTag(c),
                        typeTag(d),
                        typeTag(e),
                        typeTag(f),
                        typeTag(g),
                        typeTag(h)
                );
        return new TupleColumnMapper<>(typeTag);
    }

    /**
     * Provides a column mapper for the tuple with given parameter types.
     */
    public static <A, B, C, D, E, F, G, H, I> TupleColumnMapper<Tuple9<A, B, C, D, E, F, G, H, I>> tuple9ColumnMapper(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f,
            Class<G> g,
            Class<H> h,
            Class<I> i
    ) {
        final TypeTags.TypeTag<Tuple9<A, B, C, D, E, F, G, H, I>> typeTag =
                typeTag(Tuple9.class,
                        typeTag(a),
                        typeTag(b),
                        typeTag(c),
                        typeTag(d),
                        typeTag(e),
                        typeTag(f),
                        typeTag(g),
                        typeTag(h),
                        typeTag(i)
                );
        return new TupleColumnMapper<>(typeTag);
    }

    /**
     * Provides a column mapper for the tuple with given parameter types.
     */
    public static <A, B, C, D, E, F, G, H, I, J> TupleColumnMapper<Tuple10<A, B, C, D, E, F, G, H, I, J>> tuple10ColumnMapper(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f,
            Class<G> g,
            Class<H> h,
            Class<I> i,
            Class<J> j
    ) {
        final TypeTags.TypeTag<Tuple10<A, B, C, D, E, F, G, H, I, J>> typeTag =
                typeTag(Tuple10.class,
                        typeTag(a),
                        typeTag(b),
                        typeTag(c),
                        typeTag(d),
                        typeTag(e),
                        typeTag(f),
                        typeTag(g),
                        typeTag(h),
                        typeTag(i),
                        typeTag(j)
                );
        return new TupleColumnMapper<>(typeTag);
    }

    /**
     * Provides a column mapper for the tuple with given parameter types.
     */
    public static <A, B, C, D, E, F, G, H, I, J, K> TupleColumnMapper<Tuple11<A, B, C, D, E, F, G, H, I, J, K>> tuple11ColumnMapper(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f,
            Class<G> g,
            Class<H> h,
            Class<I> i,
            Class<J> j,
            Class<K> k
    ) {
        final TypeTags.TypeTag<Tuple11<A, B, C, D, E, F, G, H, I, J, K>> typeTag =
                typeTag(Tuple11.class,
                        typeTag(a),
                        typeTag(b),
                        typeTag(c),
                        typeTag(d),
                        typeTag(e),
                        typeTag(f),
                        typeTag(g),
                        typeTag(h),
                        typeTag(i),
                        typeTag(j),
                        typeTag(k)
                );
        return new TupleColumnMapper<>(typeTag);
    }

    /**
     * Provides a column mapper for the tuple with given parameter types.
     */
    public static <A, B, C, D, E, F, G, H, I, J, K, L> TupleColumnMapper<Tuple12<A, B, C, D, E, F, G, H, I, J, K, L>> tuple12ColumnMapper(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f,
            Class<G> g,
            Class<H> h,
            Class<I> i,
            Class<J> j,
            Class<K> k,
            Class<L> l
    ) {
        final TypeTags.TypeTag<Tuple12<A, B, C, D, E, F, G, H, I, J, K, L>> typeTag =
                typeTag(Tuple12.class,
                        typeTag(a),
                        typeTag(b),
                        typeTag(c),
                        typeTag(d),
                        typeTag(e),
                        typeTag(f),
                        typeTag(g),
                        typeTag(h),
                        typeTag(i),
                        typeTag(j),
                        typeTag(k),
                        typeTag(l)
                );
        return new TupleColumnMapper<>(typeTag);
    }

    /**
     * Provides a column mapper for the tuple with given parameter types.
     */
    public static <A, B, C, D, E, F, G, H, I, J, K, L, M> TupleColumnMapper<Tuple13<A, B, C, D, E, F, G, H, I, J, K, L, M>> tuple13ColumnMapper(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f,
            Class<G> g,
            Class<H> h,
            Class<I> i,
            Class<J> j,
            Class<K> k,
            Class<L> l,
            Class<M> m
    ) {
        final TypeTags.TypeTag<Tuple13<A, B, C, D, E, F, G, H, I, J, K, L, M>> typeTag =
                typeTag(Tuple13.class,
                        typeTag(a),
                        typeTag(b),
                        typeTag(c),
                        typeTag(d),
                        typeTag(e),
                        typeTag(f),
                        typeTag(g),
                        typeTag(h),
                        typeTag(i),
                        typeTag(j),
                        typeTag(k),
                        typeTag(l),
                        typeTag(m)
                );
        return new TupleColumnMapper<>(typeTag);
    }

    /**
     * Provides a column mapper for the tuple with given parameter types.
     */
    public static <A, B, C, D, E, F, G, H, I, J, K, L, M, N> TupleColumnMapper<Tuple14<A, B, C, D, E, F, G, H, I, J, K, L, M, N>> tuple14ColumnMapper(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f,
            Class<G> g,
            Class<H> h,
            Class<I> i,
            Class<J> j,
            Class<K> k,
            Class<L> l,
            Class<M> m,
            Class<N> n
    ) {
        final TypeTags.TypeTag<Tuple14<A, B, C, D, E, F, G, H, I, J, K, L, M, N>> typeTag =
                typeTag(Tuple14.class,
                        typeTag(a),
                        typeTag(b),
                        typeTag(c),
                        typeTag(d),
                        typeTag(e),
                        typeTag(f),
                        typeTag(g),
                        typeTag(h),
                        typeTag(i),
                        typeTag(j),
                        typeTag(k),
                        typeTag(l),
                        typeTag(m),
                        typeTag(n)
                );
        return new TupleColumnMapper<>(typeTag);
    }

    /**
     * Provides a column mapper for the tuple with given parameter types.
     */
    public static <A, B, C, D, E, F, G, H, I, J, K, L, M, N, O> TupleColumnMapper<Tuple15<A, B, C, D, E, F, G, H, I, J, K, L, M, N, O>> tuple15ColumnMapper(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f,
            Class<G> g,
            Class<H> h,
            Class<I> i,
            Class<J> j,
            Class<K> k,
            Class<L> l,
            Class<M> m,
            Class<N> n,
            Class<O> o
    ) {
        final TypeTags.TypeTag<Tuple15<A, B, C, D, E, F, G, H, I, J, K, L, M, N, O>> typeTag =
                typeTag(Tuple15.class,
                        typeTag(a),
                        typeTag(b),
                        typeTag(c),
                        typeTag(d),
                        typeTag(e),
                        typeTag(f),
                        typeTag(g),
                        typeTag(h),
                        typeTag(i),
                        typeTag(j),
                        typeTag(k),
                        typeTag(l),
                        typeTag(m),
                        typeTag(n),
                        typeTag(o)
                );
        return new TupleColumnMapper<>(typeTag);
    }

    /**
     * Provides a column mapper for the tuple with given parameter types.
     */
    public static <A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P> TupleColumnMapper<Tuple16<A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P>> tuple16ColumnMapper(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f,
            Class<G> g,
            Class<H> h,
            Class<I> i,
            Class<J> j,
            Class<K> k,
            Class<L> l,
            Class<M> m,
            Class<N> n,
            Class<O> o,
            Class<P> p
    ) {
        final TypeTags.TypeTag<Tuple16<A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P>> typeTag =
                typeTag(Tuple16.class,
                        typeTag(a),
                        typeTag(b),
                        typeTag(c),
                        typeTag(d),
                        typeTag(e),
                        typeTag(f),
                        typeTag(g),
                        typeTag(h),
                        typeTag(i),
                        typeTag(j),
                        typeTag(k),
                        typeTag(l),
                        typeTag(m),
                        typeTag(n),
                        typeTag(o),
                        typeTag(p)
                );
        return new TupleColumnMapper<>(typeTag);
    }

    /**
     * Provides a column mapper for the tuple with given parameter types.
     */
    public static <A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q> TupleColumnMapper<Tuple17<A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q>> tuple17ColumnMapper(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f,
            Class<G> g,
            Class<H> h,
            Class<I> i,
            Class<J> j,
            Class<K> k,
            Class<L> l,
            Class<M> m,
            Class<N> n,
            Class<O> o,
            Class<P> p,
            Class<Q> q
    ) {
        final TypeTags.TypeTag<Tuple17<A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q>> typeTag =
                typeTag(Tuple17.class,
                        typeTag(a),
                        typeTag(b),
                        typeTag(c),
                        typeTag(d),
                        typeTag(e),
                        typeTag(f),
                        typeTag(g),
                        typeTag(h),
                        typeTag(i),
                        typeTag(j),
                        typeTag(k),
                        typeTag(l),
                        typeTag(m),
                        typeTag(n),
                        typeTag(o),
                        typeTag(p),
                        typeTag(q)
                );
        return new TupleColumnMapper<>(typeTag);
    }

    /**
     * Provides a column mapper for the tuple with given parameter types.
     */
    public static <A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R> TupleColumnMapper<Tuple18<A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R>> tuple18ColumnMapper(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f,
            Class<G> g,
            Class<H> h,
            Class<I> i,
            Class<J> j,
            Class<K> k,
            Class<L> l,
            Class<M> m,
            Class<N> n,
            Class<O> o,
            Class<P> p,
            Class<Q> q,
            Class<R> r
    ) {
        final TypeTags.TypeTag<Tuple18<A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R>> typeTag =
                typeTag(Tuple18.class,
                        typeTag(a),
                        typeTag(b),
                        typeTag(c),
                        typeTag(d),
                        typeTag(e),
                        typeTag(f),
                        typeTag(g),
                        typeTag(h),
                        typeTag(i),
                        typeTag(j),
                        typeTag(k),
                        typeTag(l),
                        typeTag(m),
                        typeTag(n),
                        typeTag(o),
                        typeTag(p),
                        typeTag(q),
                        typeTag(r)
                );
        return new TupleColumnMapper<>(typeTag);
    }

    /**
     * Provides a column mapper for the tuple with given parameter types.
     */
    public static <A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S> TupleColumnMapper<Tuple19<A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S>> tuple19ColumnMapper(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f,
            Class<G> g,
            Class<H> h,
            Class<I> i,
            Class<J> j,
            Class<K> k,
            Class<L> l,
            Class<M> m,
            Class<N> n,
            Class<O> o,
            Class<P> p,
            Class<Q> q,
            Class<R> r,
            Class<S> s
    ) {
        final TypeTags.TypeTag<Tuple19<A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S>> typeTag =
                typeTag(Tuple19.class,
                        typeTag(a),
                        typeTag(b),
                        typeTag(c),
                        typeTag(d),
                        typeTag(e),
                        typeTag(f),
                        typeTag(g),
                        typeTag(h),
                        typeTag(i),
                        typeTag(j),
                        typeTag(k),
                        typeTag(l),
                        typeTag(m),
                        typeTag(n),
                        typeTag(o),
                        typeTag(p),
                        typeTag(q),
                        typeTag(r),
                        typeTag(s)
                );
        return new TupleColumnMapper<>(typeTag);
    }

    /**
     * Provides a column mapper for the tuple with given parameter types.
     */
    public static <A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T> TupleColumnMapper<Tuple20<A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T>> tuple20ColumnMapper(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f,
            Class<G> g,
            Class<H> h,
            Class<I> i,
            Class<J> j,
            Class<K> k,
            Class<L> l,
            Class<M> m,
            Class<N> n,
            Class<O> o,
            Class<P> p,
            Class<Q> q,
            Class<R> r,
            Class<S> s,
            Class<T> t
    ) {
        final TypeTags.TypeTag<Tuple20<A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T>> typeTag =
                typeTag(Tuple20.class,
                        typeTag(a),
                        typeTag(b),
                        typeTag(c),
                        typeTag(d),
                        typeTag(e),
                        typeTag(f),
                        typeTag(g),
                        typeTag(h),
                        typeTag(i),
                        typeTag(j),
                        typeTag(k),
                        typeTag(l),
                        typeTag(m),
                        typeTag(n),
                        typeTag(o),
                        typeTag(p),
                        typeTag(q),
                        typeTag(r),
                        typeTag(s),
                        typeTag(t)
                );
        return new TupleColumnMapper<>(typeTag);
    }

    /**
     * Provides a column mapper for the tuple with given parameter types.
     */
    public static <A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U> TupleColumnMapper<Tuple21<A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U>> tuple21ColumnMapper(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f,
            Class<G> g,
            Class<H> h,
            Class<I> i,
            Class<J> j,
            Class<K> k,
            Class<L> l,
            Class<M> m,
            Class<N> n,
            Class<O> o,
            Class<P> p,
            Class<Q> q,
            Class<R> r,
            Class<S> s,
            Class<T> t,
            Class<U> u
    ) {
        final TypeTags.TypeTag<Tuple21<A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U>> typeTag =
                typeTag(Tuple21.class,
                        typeTag(a),
                        typeTag(b),
                        typeTag(c),
                        typeTag(d),
                        typeTag(e),
                        typeTag(f),
                        typeTag(g),
                        typeTag(h),
                        typeTag(i),
                        typeTag(j),
                        typeTag(k),
                        typeTag(l),
                        typeTag(m),
                        typeTag(n),
                        typeTag(o),
                        typeTag(p),
                        typeTag(q),
                        typeTag(r),
                        typeTag(s),
                        typeTag(t),
                        typeTag(u)
                );
        return new TupleColumnMapper<>(typeTag);
    }

    /**
     * Provides a column mapper for the tuple with given parameter types.
     */
    public static <A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V> TupleColumnMapper<Tuple22<A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V>> tuple22ColumnMapper(
            Class<A> a,
            Class<B> b,
            Class<C> c,
            Class<D> d,
            Class<E> e,
            Class<F> f,
            Class<G> g,
            Class<H> h,
            Class<I> i,
            Class<J> j,
            Class<K> k,
            Class<L> l,
            Class<M> m,
            Class<N> n,
            Class<O> o,
            Class<P> p,
            Class<Q> q,
            Class<R> r,
            Class<S> s,
            Class<T> t,
            Class<U> u,
            Class<V> v
    ) {
        final TypeTags.TypeTag<Tuple22<A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V>> typeTag =
                typeTag(Tuple22.class,
                        typeTag(a),
                        typeTag(b),
                        typeTag(c),
                        typeTag(d),
                        typeTag(e),
                        typeTag(f),
                        typeTag(g),
                        typeTag(h),
                        typeTag(i),
                        typeTag(j),
                        typeTag(k),
                        typeTag(l),
                        typeTag(m),
                        typeTag(n),
                        typeTag(o),
                        typeTag(p),
                        typeTag(q),
                        typeTag(r),
                        typeTag(s),
                        typeTag(t),
                        typeTag(u),
                        typeTag(v)
                );
        return new TupleColumnMapper<>(typeTag);
    }

}
