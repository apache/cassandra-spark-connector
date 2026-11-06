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

package io.teknek.arizona.spark.driver.core;

import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.token.Token;

import java.nio.ByteBuffer;

/**
 * Wraps the driver's metadata to add the ability to build tokens from partition keys.
 * <p/>
 * This class has to live in {@code com.datastax.driver.core} in order to access package-private fields.
 */
public class MetadataHook {
    /**
     * Builds a new {@link Token} from a partition key, according to the partitioner reported by the Cassandra nodes.
     *
     * @param metadata               the original driver's metadata.
     * @param routingKey             the routing key of the bound partition key
     * @return the token.
     * @throws IllegalStateException if the token factory was not initialized. This would typically
     *                               happen if metadata was explicitly disabled before startup.
     */
    public static Token newToken(Metadata metadata, ByteBuffer routingKey) {
        return metadata.getTokenMap()
            .map(tokenMap -> tokenMap.newToken(routingKey))
            .orElseThrow(()-> new IllegalStateException("Token map was not found"));
    }

    public static String newTokenAsString(Metadata metadata, ByteBuffer routingKey) {
        return metadata.getTokenMap()
                .map(tokenMap -> tokenMap.format(tokenMap.newToken(routingKey)))
                .orElseThrow(()-> new IllegalStateException("Token map was not found"));
    }
}
