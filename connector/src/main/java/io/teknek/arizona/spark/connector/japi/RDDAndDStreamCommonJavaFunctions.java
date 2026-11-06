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

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import io.teknek.arizona.spark.connector.BatchSize;
import io.teknek.arizona.spark.connector.ColumnSelector;
import io.teknek.arizona.spark.connector.cql.CassandraConnector;
import io.teknek.arizona.spark.connector.mapper.ColumnMapper;
import io.teknek.arizona.spark.connector.writer.*;
import org.apache.spark.SparkConf;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import scala.Some$;

import java.io.Serializable;
import java.util.Date;
import java.util.Objects;

import static io.teknek.arizona.spark.connector.japi.CassandraJavaUtil.allColumns;

/**
 * Java API wrapper over either {@code RDD} or {@code DStream} for the DataStax Connector for Apache Spark to Apache Cassandra.
 *
 * <p>To obtain an instance of the specific wrapper, use one of the factory methods in {@link
 * io.teknek.arizona.spark.connector.japi.CassandraJavaUtil} class.</p>
 */
@SuppressWarnings("UnusedDeclaration")
public abstract class RDDAndDStreamCommonJavaFunctions<T> {

    /**
     * Returns the default Cassandra connector instance for the wrapped RDD or DStream.
     *
     * @return an instance of {@link io.teknek.arizona.spark.connector.cql.CassandraConnector}
     */
    public abstract CassandraConnector defaultConnector();

    protected abstract SparkConf getConf();

    /**
     * Returns the default write configuration instance for the wrapped RDD or DStream.
     *
     * @return an instance of {@link io.teknek.arizona.spark.connector.writer.WriteConf}
     */
    public WriteConf defaultWriteConf() {
        return WriteConf.fromSparkConf(getConf());
    }

    protected abstract void saveToCassandra(String keyspace, String table, RowWriterFactory<T> rowWriterFactory,
                                            ColumnSelector columnNames, WriteConf conf, CassandraConnector connector);

    /**
     * @deprecated this method will be removed in future release, please use {@link #writerBuilder(String, String,
     * io.teknek.arizona.spark.connector.writer.RowWriterFactory)}
     */
    @Deprecated
    public void saveToCassandra(String keyspace, String table, RowWriterFactory<T> rowWriterFactory, ColumnSelector columnNames) {
        new WriterBuilder(keyspace, table, rowWriterFactory, columnNames, defaultConnector(), defaultWriteConf()).saveToCassandra();
    }

    /**
     * Creates a writer builder object with specified parameters.
     *
     * <p>Writer builder is used to configure parameters write operation and eventually save the data to Cassandra. By
     * default the builder is configured to save all the columns with default connector and Spark Cassandra Connector
     * default parameters.</p>
     *
     * <p>To obtain an instance of {@link io.teknek.arizona.spark.connector.writer.RowWriterFactory} use one of utility
     * methods in {@link io.teknek.arizona.spark.connector.japi.CassandraJavaUtil}.</p>
     *
     * @param keyspaceName     the target keyspace name
     * @param tableName        the target table name
     * @param rowWriterFactory a row writer factory to be used to save objects from RDD or DStream
     *
     * @return an instance of {@link io.teknek.arizona.spark.connector.japi.RDDAndDStreamCommonJavaFunctions.WriterBuilder}
     *
     * @see io.teknek.arizona.spark.connector.japi.CassandraJavaUtil#mapToRow(Class, ColumnMapper)
     * @see io.teknek.arizona.spark.connector.japi.CassandraJavaUtil#mapToRow(Class, java.util.Map)
     * @see io.teknek.arizona.spark.connector.japi.CassandraJavaUtil#mapToRow(Class, org.apache.commons.lang3.tuple.Pair[])
     */
    public WriterBuilder writerBuilder(String keyspaceName, String tableName, RowWriterFactory<T> rowWriterFactory) {
        return new WriterBuilder(keyspaceName, tableName, rowWriterFactory, allColumns, defaultConnector(), defaultWriteConf());
    }

    /**
     * Write builder is a container for various parameters which determine how the objects from RDD or DStream are to
     * be saved to Cassandra.
     */
    public class WriterBuilder implements Serializable {
        /** Target keyspace name, where the data are to be saved. */
        public final String keyspaceName;

        /** Target table name, where the data are to be saved. */
        public final String tableName;

        /** A factory for row writer which will be used to map objects from the RDD or DStream into Cassandra rows. */
        public final RowWriterFactory<T> rowWriterFactory;

        /** A column selector which defines which columns are to be saved. */
        public final ColumnSelector columnSelector;

        /** A Cassandra connector which defines a connection to Cassandra server. */
        public final CassandraConnector connector;

        /** A set of write operation parameters which are related mainly to performance and failure tolerance. */
        public final WriteConf writeConf;

        /**
         * Refer to particular fields description for more information.
         */
        public WriterBuilder(String keyspaceName, String tableName, RowWriterFactory<T> rowWriterFactory,
                             ColumnSelector columnSelector, CassandraConnector connector, WriteConf writeConf) {
            this.keyspaceName = keyspaceName;
            this.tableName = tableName;
            this.rowWriterFactory = rowWriterFactory;
            this.columnSelector = columnSelector;
            this.connector = connector;
            this.writeConf = writeConf;
        }

        /**
         * Returns a copy of this builder with {@link #connector} changed to a specified one.
         *
         * <p>If the same instance is passed as the one which is currently set, no copy of this builder is created.</p>
         *
         * @return this instance or copy to allow method invocation chaining
         */
        public WriterBuilder withConnector(CassandraConnector connector) {
            if (connector != this.connector)
                return new WriterBuilder(keyspaceName, tableName, rowWriterFactory, columnSelector, connector, writeConf);
            else
                return this;
        }

        /**
         * Returns a copy of this builder with {@link #writeConf} changed to a specified one.
         *
         * <p>If the same instance is passed as the one which is currently set, no copy of this builder is created.</p>
         *
         * @return this instance or copy to allow method invocation chaining
         */
        public WriterBuilder withWriteConf(WriteConf writeConf) {
            if (writeConf != this.writeConf)
                return new WriterBuilder(keyspaceName, tableName, rowWriterFactory, columnSelector, connector, writeConf);
            else
                return this;
        }

        /**
         * Returns a copy of this builder with {@link #rowWriterFactory} changed to a specified one.
         *
         * <p>If the same instance is passed as the one which is currently set, no copy of this builder is created.</p>
         *
         * @return this instance or copy to allow method invocation chaining
         */
        public WriterBuilder withRowWriterFactory(RowWriterFactory<T> factory) {
            return new WriterBuilder(keyspaceName, tableName, factory, columnSelector, connector, writeConf);
        }

        /**
         * Returns a copy of this builder with {@link #columnSelector} changed to a specified one.
         *
         * <p>If the same instance is passed as the one which is currently set, no copy of this builder is created.</p>
         *
         * @return this instance or copy to allow method invocation chaining
         */
        public WriterBuilder withColumnSelector(ColumnSelector columnSelector) {
            return new WriterBuilder(keyspaceName, tableName, rowWriterFactory, columnSelector, connector, writeConf);
        }

        /**
         * Returns a copy of this builder with the new write configuration which has batch size changed to a specified
         * one.
         *
         * <p>If the same instance is passed as the one which is currently set, no copy of this builder is created.</p>
         *
         * @return this instance or copy to allow method invocation chaining
         */
        public WriterBuilder withBatchSize(BatchSize batchSize) {
            if (!Objects.equals(writeConf.batchSize(), batchSize))
                return withWriteConf(
                    new WriteConf(batchSize, writeConf.batchGroupingBufferSize(), writeConf.batchGroupingKey(),
                        writeConf.consistencyLevel(), writeConf.ifNotExists(), writeConf.ignoreNulls(),
                        writeConf.parallelismLevel(), writeConf.throughputMiBPS(), writeConf.ttl(), writeConf.timestamp(),
                        writeConf.taskMetricsEnabled(), writeConf.executeAs()));
            else
                return this;
        }

        /**
         * Returns a copy of this builder with the new write configuration which has batch buffer size changed to a specified
         * one.
         *
         * <p>If the same instance is passed as the one which is currently set, no copy of this builder is created.</p>
         *
         * @return this instance or copy to allow method invocation chaining
         */
        public WriterBuilder withBatchGroupingBufferSize(int batchGroupingBufferSize) {
            if (writeConf.batchGroupingBufferSize() != batchGroupingBufferSize)
                return withWriteConf(
                    new WriteConf(writeConf.batchSize(), batchGroupingBufferSize, writeConf.batchGroupingKey(),
                        writeConf.consistencyLevel(), writeConf.ifNotExists(), writeConf.ignoreNulls(),
                        writeConf.parallelismLevel(), writeConf.throughputMiBPS(), writeConf.ttl(), writeConf.timestamp(),
                        writeConf.taskMetricsEnabled(), writeConf.executeAs()));
            else
                return this;
        }

        /**
         * Returns a copy of this builder with the new write configuration which has batch level changed to a specified
         * one.
         *
         * <p>If the same instance is passed as the one which is currently set, no copy of this builder is created.</p>
         *
         * @return this instance or copy to allow method invocation chaining
         */
        public WriterBuilder withBatchGroupingKey(BatchGroupingKey batchGroupingKey) {
            if (!Objects.equals(writeConf.batchGroupingKey(), batchGroupingKey))
                return withWriteConf(
                    new WriteConf(writeConf.batchSize(), writeConf.batchGroupingBufferSize(), batchGroupingKey,
                        writeConf.consistencyLevel(), writeConf.ifNotExists(), writeConf.ignoreNulls(),
                        writeConf.parallelismLevel(), writeConf.throughputMiBPS(), writeConf.ttl(), writeConf.timestamp(),
                        writeConf.taskMetricsEnabled(), writeConf.executeAs()));
            else
                return this;
        }

        /**
         * Returns a copy of this builder with the new write configuration which has consistency level changed to a
         * specified one.
         *
         * <p>If the same instance is passed as the one which is currently set, no copy of this builder is created.</p>
         *
         * @return this instance or copy to allow method invocation chaining
         */
        public WriterBuilder withConsistencyLevel(ConsistencyLevel consistencyLevel) {
            if (writeConf.consistencyLevel() != consistencyLevel)
                return withWriteConf(
                    new WriteConf(writeConf.batchSize(), writeConf.batchGroupingBufferSize(), writeConf.batchGroupingKey(),
                        consistencyLevel, writeConf.ifNotExists(), writeConf.ignoreNulls(), writeConf.parallelismLevel(),
                        writeConf.throughputMiBPS(), writeConf.ttl(), writeConf.timestamp(), writeConf.taskMetricsEnabled(),
                        writeConf.executeAs()));
            else
                return this;
        }

        /**
         * Returns a copy of this builder with the new write configuration which has parallelism level changed to a
         * specified one.
         *
         * <p>If the same instance is passed as the one which is currently set, no copy of this builder is created.</p>
         *
         * @return this instance or copy to allow method invocation chaining
         */
        public WriterBuilder withParallelismLevel(int parallelismLevel) {
            if (writeConf.parallelismLevel() != parallelismLevel)
                return withWriteConf(
                    new WriteConf(writeConf.batchSize(), writeConf.batchGroupingBufferSize(), writeConf.batchGroupingKey(),
                        writeConf.consistencyLevel(), writeConf.ifNotExists(), writeConf.ignoreNulls(), parallelismLevel,
                        writeConf.throughputMiBPS(), writeConf.ttl(), writeConf.timestamp(), writeConf.taskMetricsEnabled(),
                        writeConf.executeAs()));
            else
                return this;
        }

        /**
         * Returns a copy of this builder with the new write configuration which has write throughput value changed to a
         * specified one.
         *
         * <p>If the same instance is passed as the one which is currently set, no copy of this builder is created.</p>
         *
         * @return this instance or copy to allow method invocation chaining
         */
        public WriterBuilder withThroughputMBPS(int throughputMBPS) {
            if ((Long) writeConf.throughputMiBPS().get() != throughputMBPS)
                return withWriteConf(
                    new WriteConf(writeConf.batchSize(), writeConf.batchGroupingBufferSize(), writeConf.batchGroupingKey(),
                        writeConf.consistencyLevel(), writeConf.ifNotExists(), writeConf.ignoreNulls(),
                        writeConf.parallelismLevel(), Some$.MODULE$.apply(throughputMBPS), writeConf.ttl(), writeConf.timestamp(),
                        writeConf.taskMetricsEnabled(), writeConf.executeAs()));
            else
              return this;
        }


        /**
         * Return a copy of this builder with the new write configuration which has task metrics set to enabled or disabled as specified.
         *
         * <p>If the same instance is passed as the one which is currently set, no copy of this builder is created.</p>
         *
         * @return this instance or copy to allow method invocation chaining
         */
        public WriterBuilder withTaskMetricsEnabled(boolean taskMetricsEnabled) {
            if (writeConf.taskMetricsEnabled() != taskMetricsEnabled)
                return withWriteConf(
                        new WriteConf(writeConf.batchSize(), writeConf.batchGroupingBufferSize(), writeConf.batchGroupingKey(),
                                writeConf.consistencyLevel(), writeConf.ifNotExists(), writeConf.ignoreNulls(),
                                writeConf.parallelismLevel(), writeConf.throughputMiBPS(), writeConf.ttl(),
                                writeConf.timestamp(), taskMetricsEnabled, writeConf.executeAs()));
            else
                return this;
        }

        /**
         * Return a copy of this builder with the new write configuration which has ifNotExists set to enabled or disabled as specified.
         *
         * <p>If the same instance is passed as the one which is currently set, no copy of this builder is created.</p>
         *
         * @return this instance or copy to allow method invocation chaining
         */
        public WriterBuilder withIfNotExists(boolean ifNotExists) {
            if (writeConf.ifNotExists() != ifNotExists)
                return withWriteConf(
                        new WriteConf(writeConf.batchSize(), writeConf.batchGroupingBufferSize(), writeConf.batchGroupingKey(),
                                writeConf.consistencyLevel(), ifNotExists, writeConf.ignoreNulls(), writeConf.parallelismLevel(),
                                writeConf.throughputMiBPS(), writeConf.ttl(), writeConf.timestamp(), writeConf.taskMetricsEnabled(),
                                writeConf.executeAs()));
            else
                return this;
        }

        /**
         * Return a copy of this builder with the new write configuration which has ignoreNulls set to enabled or disabled as specified.
         *
         * <p>If the same instance is passed as the one which is currently set, no copy of this builder is created.</p>
         *
         * @return this instance or copy to allow method invocation chaining
         */
        public WriterBuilder withIgnoreNulls(boolean ignoreNulls) {
            if (writeConf.ignoreNulls() != ignoreNulls)
                return withWriteConf(
                        new WriteConf(writeConf.batchSize(), writeConf.batchGroupingBufferSize(), writeConf.batchGroupingKey(),
                                writeConf.consistencyLevel(), writeConf.ifNotExists(), ignoreNulls, writeConf.parallelismLevel(),
                                writeConf.throughputMiBPS(), writeConf.ttl(), writeConf.timestamp(), writeConf.taskMetricsEnabled()
                                , writeConf.executeAs()));
            else
                return this;
        }

      private WriterBuilder withTimestamp(TimestampOption timestamp) {
            return Objects.equals(writeConf.timestamp(), timestamp)
                ? this
                : withWriteConf(new WriteConf(
                    writeConf.batchSize(),
                    writeConf.batchGroupingBufferSize(),
                    writeConf.batchGroupingKey(),
                    writeConf.consistencyLevel(),
                    writeConf.ifNotExists(),
                    writeConf.ignoreNulls(),
                    writeConf.parallelismLevel(),
                    writeConf.throughputMiBPS(),
                    writeConf.ttl(),
                    timestamp,
                    writeConf.taskMetricsEnabled(),
                    writeConf.executeAs()));
        }


        /**
         * Returns a copy of this builder with the new write configuration which has custom write timestamp
         * changed to a value specified in microseconds.
         *
         * <p>If the same instance is passed as the one which is currently set, no copy of this builder is created.</p>
         *
         * @return this instance or copy to allow method invocation chaining
         */
        public WriterBuilder withConstantTimestamp(long timeInMicroseconds) {
            return withTimestamp(TimestampOption$.MODULE$.constant(timeInMicroseconds));
        }

        /**
         * Returns a copy of this builder with the new write configuration which has custom write timestamp
         * changed to a specified value.
         *
         * <p>If the same instance is passed as the one which is currently set, no copy of this builder is created.</p>
         *
         * @return this instance or copy to allow method invocation chaining
         */
        public WriterBuilder withConstantTimestamp(Date timestamp) {
            long timeInMicroseconds = timestamp.getTime() * 1000L;
            return withConstantTimestamp(timeInMicroseconds);
        }

        /**
         * Returns a copy of this builder with the new write configuration which has custom write timestamp
         * changed to a specified value.
         *
         * <p>If the same instance is passed as the one which is currently set, no copy of this builder is created.</p>
         *
         * @return this instance or copy to allow method invocation chaining
         */
        public WriterBuilder withConstantTimestamp(DateTime timestamp) {
            long timeInMicroseconds = timestamp.getMillis() * 1000L;
            return withConstantTimestamp(timeInMicroseconds);
        }

        /**
         * Returns a copy of this builder with the new write configuration which has write timestamp set
         * to use automatic value set by Cassandra.
         *
         * <p>If the same instance is passed as the one which is currently set, no copy of this builder is created.</p>
         *
         * @return this instance or copy to allow method invocation chaining
         */
        public WriterBuilder withAutoTimestamp() {
            return withTimestamp(TimestampOption.defaultValue());
        }

        /**
         * Returns a copy of this builder with the new write configuration which has write timestamp set
         * to a placeholder which will be filled-in by mapper.
         *
         * <p>If the same instance is passed as the one which is currently set, no copy of this builder is created.</p>
         *
         * @return this instance or copy to allow method invocation chaining
         */
        public WriterBuilder withPerRowTimestamp(String placeholder) {
            return withTimestamp(TimestampOption$.MODULE$.perRow(placeholder));
        }


        private WriterBuilder withTTL(TTLOption ttl) {
            return Objects.equals(writeConf.ttl(), ttl)
                ? this
                : withWriteConf(new WriteConf(
                    writeConf.batchSize(),
                    writeConf.batchGroupingBufferSize(),
                    writeConf.batchGroupingKey(),
                    writeConf.consistencyLevel(),
                    writeConf.ifNotExists(),
                    writeConf.ignoreNulls(),
                    writeConf.parallelismLevel(),
                    writeConf.throughputMiBPS(),
                    ttl,
                    writeConf.timestamp(),
                    writeConf.taskMetricsEnabled(),
                    writeConf.executeAs()));
        }

        /**
         * Returns a copy of this builder with the new write configuration which has TTL set to a given
         * number of seconds.
         *
         * <p>If the same instance is passed as the one which is currently set, no copy of this builder is created.</p>
         *
         * @return this instance or copy to allow method invocation chaining
         */
        public WriterBuilder withConstantTTL(int ttlInSeconds) {
            return withTTL(TTLOption$.MODULE$.constant(ttlInSeconds));
        }

        /**
         * Returns a copy of this builder with the new write configuration which has TTL set to a given
         * duration.
         *
         * <p>If the same instance is passed as the one which is currently set, no copy of this builder is created.</p>
         *
         * @return this instance or copy to allow method invocation chaining
         */
        public WriterBuilder withConstantTTL(Duration ttl) {
            int secs = (int) ttl.getStandardSeconds();
            return withConstantTTL(secs);
        }

        /**
         * Returns a copy of this builder with the new write configuration which has write TTL set
         * to use automatic value set by Cassandra.
         *
         * <p>If the same instance is passed as the one which is currently set, no copy of this builder is created.</p>
         *
         * @return this instance or copy to allow method invocation chaining
         */
        public WriterBuilder withAutoTTL() {
            return withTTL(TTLOption.defaultValue());
        }

        /**
         * Returns a copy of this builder with the new write configuration which has TTL set to a placeholder
         * which will be filled-in by mapper.
         *
         * <p>If the same instance is passed as the one which is currently set, no copy of this builder is created.</p>
         *
         * @return this instance or copy to allow method invocation chaining
         */
        public WriterBuilder withPerRowTTL(String placeholder) {
            return withTTL(TTLOption$.MODULE$.perRow(placeholder));
        }

        /**
         * Sets the output operator for the RDD or DStream.
         *
         * <p>Calling this method cause the data to be written to the database using all the configuration
         * settings.</p>
         */
        public void saveToCassandra() {
            RDDAndDStreamCommonJavaFunctions.this.saveToCassandra(keyspaceName, tableName, rowWriterFactory,
                    columnSelector, writeConf, connector);
        }

    }
}
