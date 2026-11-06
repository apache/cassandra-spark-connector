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

/**
 * This is a sample JavaBean style class/subclass. In order to test JavaAPI correctly, we cannot
 * implement this in Scala because Scala adds some additional accessors and mutators.
 */
public class SampleJavaBeanSubClass extends SampleJavaBean
{
    private String subClassField;

    public static SampleJavaBeanSubClass newInstance(Integer key, String value, String subClassField) {
        SampleJavaBeanSubClass bean = new SampleJavaBeanSubClass();
        bean.setKey(key);
        bean.setValue(value);
        bean.setSubClassField(subClassField);
        return bean;
    }

    public String getSubClassField()
    {
        return subClassField;
    }

    public void setSubClassField(String subClassField)
    {
        this.subClassField = subClassField;
    }
}
