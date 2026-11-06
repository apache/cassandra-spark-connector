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

import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;

/**
 * The main entry point to Spark Cassandra Connector Java API for Spark Streaming.
 *
 * <p>There are several helpful static factory methods which build useful wrappers around Streaming
 * Context and DStream.</p>
 */
@SuppressWarnings("UnusedDeclaration")
public class CassandraStreamingJavaUtil
{

    private CassandraStreamingJavaUtil() {
        assert false;
    }

    // -------------------------------------------------------------------------
    //              Java API wrappers factory methods
    // -------------------------------------------------------------------------

    /**
     * A static factory method to create a {@link StreamingContextJavaFunctions} based on an existing
     * {@link StreamingContext} instance.
     */
    public static StreamingContextJavaFunctions javaFunctions(StreamingContext streamingContext) {
        return new StreamingContextJavaFunctions(streamingContext);
    }

    /**
     * A static factory method to create a {@link StreamingContextJavaFunctions} based on an existing
     * {@link JavaStreamingContext} instance.
     */
    public static StreamingContextJavaFunctions javaFunctions(JavaStreamingContext streamingContext) {
        return new StreamingContextJavaFunctions(streamingContext.ssc());
    }

    /**
     * A static factory method to create a {@link DStreamJavaFunctions} based on an existing
     * {@link DStream} instance.
     */
    public static <T> DStreamJavaFunctions<T> javaFunctions(DStream<T> dStream) {
        return new DStreamJavaFunctions<>(dStream);
    }

    /**
     * A static factory method to create a {@link DStreamJavaFunctions} based on an existing
     * {@link JavaDStream} instance.
     */
    public static <T> DStreamJavaFunctions<T> javaFunctions(JavaDStream<T> dStream) {
        return new DStreamJavaFunctions<>(dStream.dstream());
    }

}
