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

import static io.teknek.arizona.spark.connector.japi.CassandraJavaUtil.column;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.teknek.arizona.spark.connector.ColumnRef;
import io.teknek.arizona.spark.connector.cql.CassandraConnector;
import io.teknek.arizona.spark.connector.rdd.CassandraRDD;
import io.teknek.arizona.spark.connector.rdd.ReadConf;
import io.teknek.arizona.spark.connector.util.JavaApiHelper;

@SuppressWarnings({"unchecked", "RedundantTypeArguments"})
public class CassandraJavaRDDTest {

    @Test
    public void testSelectColumnNames() {
        CassandraRDD<Integer> rdd = mock(CassandraRDD.class);
        CassandraRDD<Integer> rdd2 = mock(CassandraRDD.class);
        when(rdd.select(JavaApiHelper.<ColumnRef>toScalaSeq(
                new ColumnRef[]{column("a"), column("b")}))).thenReturn(rdd2);
        CassandraJavaRDD<Integer> jrdd = new CassandraJavaRDD<>(rdd, Integer.class);
        assertThat(jrdd.select("a", "b").rdd(), is(rdd2));
    }

    @Test
    public void testSelectColumns() {
        CassandraRDD<Integer> rdd = mock(CassandraRDD.class);
        CassandraRDD<Integer> rdd2 = mock(CassandraRDD.class);
        when(rdd.select(JavaApiHelper.<ColumnRef>toScalaSeq(
                new ColumnRef[]{column("a"), column("b")}))).thenReturn(rdd2);
        CassandraJavaRDD<Integer> jrdd = new CassandraJavaRDD<>(rdd, Integer.class);
        assertThat(jrdd.select(column("a"), column("b")).rdd(), is(rdd2));
    }

    @Test
    public void testWhere() {
        CassandraRDD<Integer> rdd = mock(CassandraRDD.class);
        CassandraRDD<Integer> rdd2 = mock(CassandraRDD.class);
        when(rdd.where("a=?", JavaApiHelper.toScalaSeq(new Object[]{1})))
                .thenReturn(rdd2);
        CassandraJavaRDD<Integer> jrdd = new CassandraJavaRDD<>(rdd, Integer.class);
        assertThat(jrdd.where("a=?", 1).rdd(), is(rdd2));
    }

    @Test
    public void testWithAscOrder() {
        CassandraRDD<Integer> rdd = mock(CassandraRDD.class);
        CassandraRDD<Integer> rdd2 = mock(CassandraRDD.class);
        when(rdd.withAscOrder()).thenReturn(rdd2);
        CassandraJavaRDD<Integer> jrdd = new CassandraJavaRDD<>(rdd, Integer.class);
        assertThat(jrdd.withAscOrder().rdd(), is(rdd2));
    }

    @Test
    public void testWithDescOrder() {
        CassandraRDD<Integer> rdd = mock(CassandraRDD.class);
        CassandraRDD<Integer> rdd2 = mock(CassandraRDD.class);
        when(rdd.withDescOrder()).thenReturn(rdd2);
        CassandraJavaRDD<Integer> jrdd = new CassandraJavaRDD<>(rdd, Integer.class);
        assertThat(jrdd.withDescOrder().rdd(), is(rdd2));
    }

    @Test
    public void testSelectedColumnRefs() {
        CassandraRDD<Integer> rdd = mock(CassandraRDD.class);
        when(rdd.selectedColumnRefs())
                .thenReturn(JavaApiHelper.<ColumnRef>toScalaSeq(new ColumnRef[]{column("a"), column("b")}));
        CassandraJavaRDD<Integer> jrdd = new CassandraJavaRDD<>(rdd, Integer.class);
        assertThat(jrdd.selectedColumnRefs(), is(new ColumnRef[] {column("a"), column("b")}));
    }

    @Test
    public void testSelectedColumnNames() {
        CassandraRDD<Integer> rdd = mock(CassandraRDD.class);
        when(rdd.selectedColumnNames())
                .thenReturn(JavaApiHelper.<String>toScalaSeq(new String[]{"a", "b"}));
        CassandraJavaRDD<Integer> jrdd = new CassandraJavaRDD<>(rdd, Integer.class);
        assertThat(jrdd.selectedColumnNames(), is(new String[] {"a", "b"}));
    }

    @Test
    public void testWithConnector() {
        CassandraRDD<Integer> rdd = mock(CassandraRDD.class);
        CassandraRDD<Integer> rdd2 = mock(CassandraRDD.class);
        CassandraConnector connector = mock(CassandraConnector.class);
        when(rdd.withConnector(connector)).thenReturn(rdd2);
        CassandraJavaRDD<Integer> jrdd = new CassandraJavaRDD<>(rdd, Integer.class);
        assertThat(jrdd.withConnector(connector).rdd(), is(rdd2));
    }

    @Test
    public void testWithReadConf() {
        CassandraRDD<Integer> rdd = mock(CassandraRDD.class);
        CassandraRDD<Integer> rdd2 = mock(CassandraRDD.class);
        ReadConf readConf = mock(ReadConf.class);
        when(rdd.withReadConf(readConf)).thenReturn(rdd2);
        CassandraJavaRDD<Integer> jrdd = new CassandraJavaRDD<>(rdd, Integer.class);
        assertThat(jrdd.withReadConf(readConf).rdd(), is(rdd2));
    }

    @Test
    public void testLimit() {
        CassandraRDD<Integer> rdd = mock(CassandraRDD.class);
        CassandraRDD<Integer> rdd2 = mock(CassandraRDD.class);
        when(rdd.limit(1L)).thenReturn(rdd2);
        CassandraJavaRDD<Integer> jrdd = new CassandraJavaRDD<>(rdd, Integer.class);
        assertThat(jrdd.limit(1L).rdd(), is(rdd2));
    }

    @Test
    public void testPerPartitionLimit() {
        CassandraRDD<Integer> rdd = mock(CassandraRDD.class);
        CassandraRDD<Integer> rdd2 = mock(CassandraRDD.class);
        when(rdd.perPartitionLimit(1L)).thenReturn(rdd2);
        CassandraJavaRDD<Integer> jrdd = new CassandraJavaRDD<>(rdd, Integer.class);
        assertThat(jrdd.perPartitionLimit(1L).rdd(), is(rdd2));
    }
}
