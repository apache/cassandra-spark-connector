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

package com.datastax.spark.connector.japi;

import com.google.common.collect.Sets;
import org.joda.time.DateTime;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

public class CassandraRowTest {

    @Test
    public void testGetBoolean() {
        CassandraRow row = new CassandraRow(new String[]{"value"}, new Object[]{true});
        assertEquals(row.getBoolean("value"), true);
        assertEquals(row.getBoolean(0), true);
    }

    @Test
    public void testGetByte() {
        CassandraRow row = new CassandraRow(new String[]{"value"}, new Object[]{(byte) 1});
        assertEquals((Object) row.getByte("value"), (byte) 1);
        assertEquals((Object) row.getByte(0), (byte) 1);
    }

    @Test
    public void testGetBytes() {
        CassandraRow row = new CassandraRow(new String[]{"value"}, new Object[]{new byte[]{1, 2, 3}});
        assertEquals(row.getBytes("value"), ByteBuffer.wrap(new byte[]{1, 2, 3}));
        assertEquals(row.getBytes(0), ByteBuffer.wrap(new byte[]{1, 2, 3}));
    }

    @Test
    public void testGetDate() {
        long t = System.currentTimeMillis();
        CassandraRow row = new CassandraRow(new String[]{"value"}, new Object[]{new Date(t)});
        assertEquals(row.getDate("value"), new Date(t));
        assertEquals(row.getDate(0), new Date(t));
    }

    @Test
    public void testGetDecimal() {
        CassandraRow row = new CassandraRow(new String[]{"value"}, new Object[]{new BigDecimal(123.321d)});
        assertEquals(row.getDecimal("value"), new BigDecimal(123.321d));
        assertEquals(row.getDecimal(0), new BigDecimal(123.321d));
    }

    @Test
    public void testGetDouble() {
        CassandraRow row = new CassandraRow(new String[]{"value"}, new Object[]{123.321d});
        assertEquals((Object) row.getDouble("value"), 123.321d);
        assertEquals((Object) row.getDouble(0), 123.321d);
    }

    @Test
    public void testGetFloat() {
        CassandraRow row = new CassandraRow(new String[]{"value"}, new Object[]{123.321f});
        assertEquals((Object) row.getFloat("value"), 123.321f);
        assertEquals((Object) row.getFloat(0), 123.321f);
    }

    @Test
    public void testGetInet() throws Exception {
        CassandraRow row = new CassandraRow(new String[]{"value"}, new Object[]{InetAddress.getLocalHost()});
        assertEquals(row.getInet("value"), InetAddress.getLocalHost());
        assertEquals(row.getInet(0), InetAddress.getLocalHost());
    }

    @Test
    public void testGetInt() {
        CassandraRow row = new CassandraRow(new String[]{"value"}, new Object[]{123});
        assertEquals((Object) row.getInt("value"), 123);
        assertEquals((Object) row.getInt(0), 123);
    }

    @Test
    public void testGetLong() {
        CassandraRow row = new CassandraRow(new String[]{"value"}, new Object[]{123L});
        assertEquals((Object) row.getLong("value"), 123L);
        assertEquals((Object) row.getLong(0), 123L);
    }

    @Test
    public void testGetShort() {
        CassandraRow row = new CassandraRow(new String[]{"value"}, new Object[]{(short) 123});
        assertEquals((Object) row.getShort("value"), (short) 123);
        assertEquals((Object) row.getShort(0), (short) 123);
    }

    @Test
    public void testGetString() {
        CassandraRow row = new CassandraRow(new String[]{"value"}, new Object[]{"cassandra"});
        assertEquals(row.getString("value"), "cassandra");
        assertEquals(row.getString(0), "cassandra");
    }

    @Test
    public void testGetUUID() {
        String uuid = UUID.randomUUID().toString();
        CassandraRow row = new CassandraRow(new String[]{"value"}, new Object[]{UUID.fromString(uuid)});
        assertEquals(row.getUUID("value"), UUID.fromString(uuid));
        assertEquals(row.getUUID(0), UUID.fromString(uuid));
    }

    @Test
    public void testGetVarInt() {
        CassandraRow row = new CassandraRow(new String[]{"value"}, new Object[]{new BigInteger("123")});
        assertEquals(row.getVarInt("value"), new BigInteger("123"));
        assertEquals(row.getVarInt(0), new BigInteger("123"));
    }

    @Test
    public void testGetObjectAndApply() {
        class TestClass {
            final int v;

            public TestClass(int v) {
                this.v = v;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                TestClass testClass = (TestClass) o;
                return v == testClass.v;
            }

            @Override
            public int hashCode() {
                return v;
            }
        }
        CassandraRow row = new CassandraRow(new String[]{"value"}, new Object[]{new TestClass(123)});
        assertEquals(row.getObject("value"), new TestClass(123));
        assertEquals(row.getObject(0), new TestClass(123));
        assertEquals(row.apply("value"), new TestClass(123));
        assertEquals(row.apply(0), new TestClass(123));
    }

    @Test
    public void testGet() {
        CassandraRow row = new CassandraRow(new String[]{"value"}, new Object[]{"1"});
        assertEquals(row.get("value", CassandraJavaUtil.<String>typeConverter(String.class)), "1");
        assertEquals((Object) row.get("value", CassandraJavaUtil.<Integer>typeConverter(Integer.class)), 1);
        assertEquals(row.get(0, CassandraJavaUtil.<String>typeConverter(String.class)), "1");
        assertEquals((Object) row.get(0, CassandraJavaUtil.<Integer>typeConverter(Integer.class)), 1);
    }

    @Test
    public void testGetList() {
        CassandraRow row = new CassandraRow(new String[]{"value"}, new Object[]{Arrays.asList(1, 2, 3)});
        assertEquals(row.getList("value"), Arrays.<Object>asList(1, 2, 3));
        assertEquals(row.getList(0), Arrays.<Object>asList(1, 2, 3));
        assertEquals(row.getList("value", CassandraJavaUtil.<Integer>typeConverter(Integer.class)), Arrays.asList(1, 2, 3));
        assertEquals(row.getList(0, CassandraJavaUtil.<Integer>typeConverter(Integer.class)), Arrays.asList(1, 2, 3));
    }

    @Test
    public void testGetSet() {
        CassandraRow row = new CassandraRow(new String[]{"value"}, new Object[]{Sets.newHashSet(1, 2, 3)});
        assertEquals(row.getSet("value"), Sets.<Object>newHashSet(1, 2, 3));
        assertEquals(row.getSet(0), Sets.<Object>newHashSet(1, 2, 3));
        assertEquals(row.getSet("value", CassandraJavaUtil.<Integer>typeConverter(Integer.class)), Sets.newHashSet(1, 2, 3));
        assertEquals(row.getSet(0, CassandraJavaUtil.<Integer>typeConverter(Integer.class)), Sets.newHashSet(1, 2, 3));
    }

    @Test
    public void testGetMap() {
        Map<String, Integer> map = new HashMap<>(2);
        map.put("one", 1);
        map.put("two", 2);
        CassandraRow row = new CassandraRow(new String[]{"value"}, new Object[]{map});
        assertEquals(row.getMap("value"), new HashMap<Object, Object>(map));
        assertEquals(row.getMap(0), new HashMap<Object, Object>(map));
        assertThat(row.getMap("value",
                        CassandraJavaUtil.<String>typeConverter(String.class),
                        CassandraJavaUtil.<Integer>typeConverter(Integer.class)),
                is((Map<String, Integer>) new HashMap<>(map)));
        assertThat(row.getMap(0,
                        CassandraJavaUtil.<String>typeConverter(String.class),
                        CassandraJavaUtil.<Integer>typeConverter(Integer.class)),
                is((Map<String, Integer>) new HashMap<>(map)));
    }

    @Test
    public void testToMap() {
        CassandraRow row = new CassandraRow(new String[]{"col1", "col2"}, new Object[]{"one", 1});
        assertEquals(row.toMap().get("col1"), "one");
        assertEquals(row.toMap().get("col2"), 1);
    }

}
