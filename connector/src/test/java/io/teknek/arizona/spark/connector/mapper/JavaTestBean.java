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

package io.teknek.arizona.spark.connector.mapper;

import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;

import java.io.Serializable;

/**
 * This is a Java Bean style class with Java Driver style annotations built in.
 * This class also contains nested UDTs with its own mappings
 */
@Entity
public class JavaTestBean implements Serializable {

    @CqlName(value = "cassandra_property_1")
    public Integer property1;
    @CqlName(value = "cassandra_camel_case_property")
    public Integer camelCaseProperty;
    public JavaTestUDTBean nested;

    public int getProperty1() {
        return property1;
    }

    public void setProperty1(int property1) {
        this.property1 = property1;
    }

    public int getCamelCaseProperty() {
        return camelCaseProperty;
    }

    public void setCamelCaseProperty(int camelCaseProperty) {
        this.camelCaseProperty = camelCaseProperty;
    }

    public JavaTestUDTBean getNested() {
        return nested;
    }

    public void setNested(JavaTestUDTBean nested) {
        this.nested = nested;
    }

}
