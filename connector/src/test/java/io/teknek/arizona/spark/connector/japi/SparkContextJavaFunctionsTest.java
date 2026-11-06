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

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import io.teknek.arizona.spark.connector.rdd.ReadConf;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

public class SparkContextJavaFunctionsTest
{
    @Test
    public void testReadConfPopulating() {
        SparkConf conf = new SparkConf();
        conf.set("spark.cassandra.input.fetch.sizeInRows", "1234");
        conf.set("spark.cassandra.input.split.sizeInMB", "4321");
        conf.set("spark.cassandra.input.consistency.level", "THREE");

        SparkContext sc = Mockito.mock(SparkContext.class);
        when(sc.getConf()).thenReturn(conf);

        ReadConf readConf = CassandraJavaUtil.javaFunctions(sc).cassandraTable("a", "b").rdd().readConf();

        assertEquals(readConf.fetchSizeInRows(), 1234);
        assertEquals(readConf.splitSizeInMB(), 4321);
        assertEquals(readConf.consistencyLevel(), DefaultConsistencyLevel.THREE);
    }
}
