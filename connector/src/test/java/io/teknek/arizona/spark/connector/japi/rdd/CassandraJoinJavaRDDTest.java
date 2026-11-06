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

import static io.teknek.arizona.spark.connector.japi.CassandraJavaUtil.someColumns;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.teknek.arizona.spark.connector.rdd.CassandraJoinRDD;

@SuppressWarnings({"unchecked", "RedundantTypeArguments"})
public class CassandraJoinJavaRDDTest {

    @Test
    public void testOn() {
        CassandraJoinRDD<String, Integer> rdd = mock(CassandraJoinRDD.class);
        CassandraJoinRDD<String, Integer> rdd2 = mock(CassandraJoinRDD.class);
        when(rdd.on(someColumns("a", "b"))).thenReturn(rdd2);
        CassandraJoinJavaRDD<String, Integer> jrdd = new CassandraJoinJavaRDD<>(rdd, String.class, Integer.class);
        assertThat(jrdd.on(someColumns("a", "b")).rdd(), is(rdd2));
    }

}
