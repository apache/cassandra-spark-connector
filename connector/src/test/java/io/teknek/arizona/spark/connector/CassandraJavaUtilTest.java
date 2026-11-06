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

package io.teknek.arizona.spark.connector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import scala.reflect.api.TypeTags;

import static io.teknek.arizona.spark.connector.japi.CassandraJavaUtil.*;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.teknek.arizona.spark.connector.japi.RDDJavaFunctions;
import io.teknek.arizona.spark.connector.japi.SparkContextJavaFunctions;
import io.teknek.arizona.spark.connector.mapper.ColumnMapper;
import io.teknek.arizona.spark.connector.rdd.reader.ClassBasedRowReaderFactory;
import io.teknek.arizona.spark.connector.rdd.reader.RowReaderFactory;
import io.teknek.arizona.spark.connector.rdd.reader.ValueRowReaderFactory;
import io.teknek.arizona.spark.connector.types.TypeConverter;

@SuppressWarnings("unchecked")
public class CassandraJavaUtilTest {

    /**
     * Scala reflection type tags change the string reprsentation of some types, in scala 2.11 java.lang
     * is included, in scala 2.12 it is removed. To remove this conflict we just always remove the java.lang
     * portion
     */
    private String removeJavaLang(String target) {
        return target.replaceAll("java.lang.", "");
    }

    private final String STRING = removeJavaLang(String.class.getName());
    private final String LIST_STRING =
            removeJavaLang(String.format("%s[%s]", List.class.getName(), String.class.getName()));
    private final String MAP_STRING_INT =
            removeJavaLang(String.format("%s[%s,%s]", Map.class.getName(), String.class.getName(), Integer.class.getName()));
    private final String LIST_SET_MAP_STRING_INT =
            removeJavaLang(String.format("%s[%s[%s[%s,%s]]]", List.class.getName(), Set.class.getName(), Map.class.getName(), String.class.getName(), Integer.class.getName()));



    @Test
    public void testTypeTag1() throws Exception {
        TypeTags.TypeTag<String> tt = typeTag(String.class);
        assertThat(removeJavaLang(tt.tpe().toString()), is(STRING));
    }

    @Test
    public void testTypeTag2() throws Exception {
        TypeTags.TypeTag<List> tt1 = typeTag(List.class, String.class);
        assertThat(removeJavaLang(removeJavaLang(tt1.tpe().toString())), is(LIST_STRING));

        TypeTags.TypeTag<Map> tt2 = typeTag(Map.class, String.class, Integer.class);
        assertThat(removeJavaLang(removeJavaLang(tt2.tpe().toString())), is(MAP_STRING_INT));
    }

    @Test
    public void testTypeTag3() throws Exception {
        TypeTags.TypeTag<List> tt = typeTag(List.class, typeTag(Set.class, typeTag(Map.class, typeTag(String.class), typeTag(Integer.class))));
        assertThat(removeJavaLang(tt.tpe().toString()), is(LIST_SET_MAP_STRING_INT));
    }

    @Test
    public void testTypeConverter1() throws Exception {
        TypeConverter<List<String>> tc = typeConverter(String.class);
        assertThat(removeJavaLang(tc.targetTypeName()), is(STRING));
    }

    @Test
    public void testTypeConverter2() throws Exception {
        TypeConverter<List<String>> tc1 = typeConverter(List.class, String.class);
        assertThat(removeJavaLang(tc1.targetTypeName()), is(LIST_STRING));

        TypeConverter<Map<String, Integer>> tc2 = typeConverter(Map.class, String.class, Integer.class);
        assertThat(removeJavaLang(tc2.targetTypeName()), is(MAP_STRING_INT));

    }

    @Test
    public void testTypeConverter3() throws Exception {
        TypeConverter<List> tc = typeConverter(List.class, typeTag(Set.class, typeTag(Map.class, typeTag(String.class), typeTag(Integer.class))));
        assertThat(removeJavaLang(tc.targetTypeName()), is(LIST_SET_MAP_STRING_INT));
    }

    @Test
    public void testTypeConverter4() throws Exception {
        TypeTags.TypeTag<List> tt = typeTag(List.class, typeTag(Set.class, typeTag(Map.class, typeTag(String.class), typeTag(Integer.class))));
        TypeConverter<List> tc = typeConverter(tt);
        assertThat(removeJavaLang(tc.targetTypeName()), is(LIST_SET_MAP_STRING_INT));
    }

    @Test
    public void testMapColumnTo1() throws Exception {
        RowReaderFactory<Integer> rrf = mapColumnTo(Integer.class);
        assertThat(rrf, instanceOf(ValueRowReaderFactory.class));
        assertThat(rrf.targetClass().getName(), is(Integer.class.getName()));
    }

    @Test
    public void testMapColumnTo2() throws Exception {
        RowReaderFactory<Integer> rrf = mapColumnTo(TypeConverter.JavaIntConverter$.MODULE$);
        assertThat(rrf, instanceOf(ValueRowReaderFactory.class));
        assertThat(rrf.targetClass().getName(), is(Integer.class.getName()));
    }

    @Test
    public void testMapColumnTo3() throws Exception {
        RowReaderFactory<List<Integer>> rrf = mapColumnTo(List.class, Integer.class);
        assertThat(rrf, instanceOf(ValueRowReaderFactory.class));
        assertThat(rrf.targetClass().getName(), is(List.class.getName()));
    }

    @Test
    public void testMapColumnToListOf() throws Exception {
        RowReaderFactory<List<Integer>> rrf = mapColumnToListOf(Integer.class);
        assertThat(rrf, instanceOf(ValueRowReaderFactory.class));
        assertThat(rrf.targetClass().getName(), is(List.class.getName()));
    }

    @Test
    public void testMapColumnToSetOf() throws Exception {
        RowReaderFactory<Set<Integer>> rrf = mapColumnToSetOf(Integer.class);
        assertThat(rrf, instanceOf(ValueRowReaderFactory.class));
        assertThat(rrf.targetClass().getName(), is(Set.class.getName()));

    }

    @Test
    public void testMapColumnToMapOf() throws Exception {
        RowReaderFactory<Map<Integer, Double>> rrf = mapColumnToMapOf(Integer.class, Double.class);
        assertThat(rrf, instanceOf(ValueRowReaderFactory.class));
        assertThat(rrf.targetClass().getName(), is(Map.class.getName()));
    }


    @Test
    public void testMapRowTo1() throws Exception {
        RowReaderFactory<SampleJavaBean> rrf = mapRowTo(SampleJavaBean.class);
        assertThat(rrf, instanceOf(ClassBasedRowReaderFactory.class));
        assertThat(rrf.targetClass().getName(), is(SampleJavaBean.class.getName()));
    }

    @Test
    public void testMapRowTo2() throws Exception {
        Map<String, String> mappings = new HashMap<>();
        mappings.put("one", "1");
        mappings.put("two", "2");
        RowReaderFactory<SampleJavaBean> rrf = mapRowTo(SampleJavaBean.class, mappings);
        assertThat(rrf, instanceOf(ClassBasedRowReaderFactory.class));
        assertThat(rrf.targetClass().getName(), is(SampleJavaBean.class.getName()));
    }

    @Test
    public void testMapRowTo3() throws Exception {
        RowReaderFactory<SampleJavaBean> rrf = mapRowTo(SampleJavaBean.class, Pair.of("a", "b"), Pair.of("c", "d"));
        assertThat(rrf, instanceOf(ClassBasedRowReaderFactory.class));
        assertThat(rrf.targetClass().getName(), is(SampleJavaBean.class.getName()));
    }

    @Test
    public void testMapRowTo4() throws Exception {
        //noinspection unchecked
        ColumnMapper<SampleJavaBean> mapper = mock(ColumnMapper.class);
        RowReaderFactory<SampleJavaBean> rrf = mapRowTo(SampleJavaBean.class, mapper);
        assertThat(rrf, instanceOf(ClassBasedRowReaderFactory.class));
        assertThat(rrf.targetClass().getName(), is(SampleJavaBean.class.getName()));
    }

    @Test
    public void testConvertToMap() throws Exception {
        Map<String, String> map1 = convertToMap(new Pair[0]);
        assertThat(map1.size(), is(0));

        Map<String, String> map2 = convertToMap(new Pair[]{Pair.of("one", "1"), Pair.of("two", "2")});
        assertThat(map2.size(), is(2));
        assertThat(map2.get("one"), is("1"));
        assertThat(map2.get("two"), is("2"));
    }

    @Test
    public void testJavaFunctions() throws Exception {
        SparkContext sc = mock(SparkContext.class);
        SparkContextJavaFunctions scjf = javaFunctions(sc);
        assertThat(scjf.sparkContext, is(sc));
    }

    @Test
    public void testJavaFunctions1() throws Exception {
        SparkContext sc = mock(SparkContext.class);
        JavaSparkContext jsc = mock(JavaSparkContext.class);
        when(jsc.sc()).thenReturn(sc);
        SparkContextJavaFunctions scjf = javaFunctions(jsc);
        assertThat(scjf.sparkContext, is(jsc.sc()));
    }

    @Test
    public void testJavaFunctions4() throws Exception {
        RDD rdd = mock(RDD.class);
        RDDJavaFunctions rddjf = javaFunctions(rdd);
        assertThat(rddjf.rdd, is(rdd));
    }

    @Test
    public void testJavaFunctions5() throws Exception {
        RDD rdd = mock(RDD.class);
        JavaRDD jrdd = mock(JavaRDD.class);
        when(jrdd.rdd()).thenReturn(rdd);
        RDDJavaFunctions rddjf = javaFunctions(jrdd);
        assertThat(rddjf.rdd, is(rdd));
    }

}
