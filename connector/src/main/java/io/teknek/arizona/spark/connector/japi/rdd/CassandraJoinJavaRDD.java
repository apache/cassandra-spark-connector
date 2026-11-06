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

import scala.reflect.ClassTag;

import static io.teknek.arizona.spark.connector.japi.CassandraJavaUtil.safeClassTag;

import io.teknek.arizona.spark.connector.ColumnSelector;
import io.teknek.arizona.spark.connector.rdd.CassandraJoinRDD;

public class CassandraJoinJavaRDD<K, V> extends CassandraJavaPairRDD<K, V> {

    public CassandraJoinJavaRDD(CassandraJoinRDD<K, V> rdd,
                                ClassTag<K> keyClassTag, ClassTag<V> valueClassTag) {

        super(rdd, keyClassTag, valueClassTag);
    }

    public CassandraJoinJavaRDD(CassandraJoinRDD<K, V> rdd,
                                Class<K> keyClass, Class<V> valueClass) {

        super(rdd, safeClassTag(keyClass), safeClassTag(valueClass));
    }

    private CassandraJoinJavaRDD<K, V> wrap(CassandraJoinRDD<K, V> newRDD) {
        return new CassandraJoinJavaRDD<>(newRDD, kClassTag(), vClassTag());
    }


    public CassandraJoinJavaRDD<K, V> on(ColumnSelector joinColumns) {
        CassandraJoinRDD<K, V> newRDD = rdd().on(joinColumns);
        return wrap(newRDD);
    }

    @Override
    public CassandraJoinRDD<K, V> rdd() {
        return (CassandraJoinRDD<K, V>) super.rdd();
    }
}
