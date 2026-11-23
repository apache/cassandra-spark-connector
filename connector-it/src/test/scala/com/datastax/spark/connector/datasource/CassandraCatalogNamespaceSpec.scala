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

package com.datastax.spark.connector.datasource

import org.scalatest.concurrent.Eventually._
import org.apache.spark.sql.catalyst.analysis.{NamespaceAlreadyExistsException, NoSuchNamespaceException}
import scala.jdk.CollectionConverters._

class CassandraCatalogNamespaceSpec extends CassandraCatalogSpecBase {

  "A Cassandra Catalog Namespace Support" should "initialize successfully" in {
    spark.sessionState.catalogManager.currentCatalog.name() should be(defaultCatalog)
  }

  it should "list available keyspaces" in {
    val currentKeyspaces = spark.sql("SHOW DATABASES").collect().map(_.getString(0))
    val existingKeyspaces = getMetadata().getKeyspaces().keySet().asScala.map(_.asInternal())
    currentKeyspaces should contain allElementsOf(existingKeyspaces)

  }

  it should "list a keyspaces in a keyspace"  in {
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $defaultKs WITH DBPROPERTIES (class='SimpleStrategy',replication_factor='5')")
    waitForKeyspaceToExist(defaultKs)
    //Using Catalog and DefaultKS triggers listNamespaces(namespace) pathway
    val result = spark.sql(s"SHOW NAMESPACES FROM $defaultCatalog.$defaultKs").collect
    result shouldBe empty
  }

  it should "give no keyspace found when looking up a keyspace which doesn't exist" in {
    intercept[NoSuchNamespaceException]{
      spark.sql(s"SHOW NAMESPACES FROM $defaultCatalog.fakenotreal").collect
    }
  }

  it should "be able to create a new keyspace" in {
    dropKeyspace(defaultKs)
    waitForKeyspaceToExist(defaultKs, false)
    spark.sql(s"CREATE DATABASE $defaultKs WITH DBPROPERTIES (class='SimpleStrategy',replication_factor='5')")
    waitForKeyspaceToExist(defaultKs)
    spark.sql(s"DESCRIBE DATABASE EXTENDED $defaultKs").show
  }

  it should "be able to create a new keyspace with NTS" in {
    dropKeyspace(defaultKs)
    waitForKeyspaceToExist(defaultKs, false)
    val datacenterName = getMetadata().getNodes.values().iterator().next().getDatacenter
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $defaultKs WITH DBPROPERTIES (class='NetworkTopologyStrategy', $datacenterName='1')")
    waitForKeyspaceToExist(defaultKs)
    spark.sql(s"DESCRIBE DATABASE EXTENDED $defaultKs").show
  }

  it should "give a sensible error when trying to create a keyspace without a replication class" in {
    dropKeyspace(defaultKs)
    waitForKeyspaceToExist(defaultKs, false)
    val exception = intercept[CassandraCatalogException]{
      spark.sql(s"CREATE DATABASE $defaultKs WITH DBPROPERTIES (replication_factor='5')")
    }
    exception.getMessage should include("requires a class")
  }

  it should "give a sensible error when trying to create a keyspace with an invalid replication strategy" in {
    dropKeyspace(defaultKs)
    waitForKeyspaceToExist(defaultKs, false)
    val exception = intercept[CassandraCatalogException]{
      spark.sql(s"CREATE DATABASE $defaultKs WITH DBPROPERTIES (class='foobar',replication_factor='5')")
    }
    exception.getMessage should include("Unknown keyspace replication strategy")
  }

  it should "throw a keyspace exists exception when creating a keyspace that already exists" in {
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $defaultKs WITH DBPROPERTIES (class='SimpleStrategy',replication_factor='5')")
    waitForKeyspaceToExist(defaultKs)
    intercept[NamespaceAlreadyExistsException]{
      spark.sql(s"CREATE DATABASE $defaultKs WITH DBPROPERTIES (class='SimpleStrategy',replication_factor='5')")
    }
  }

  it should "explain Cassandra specific properties in the describe statement" in {
    dropKeyspace(defaultKs)
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $defaultKs WITH DBPROPERTIES (class='SimpleStrategy',replication_factor='5')")
    waitForKeyspaceToExist(defaultKs)
    val results = spark.sql(s"DESCRIBE NAMESPACE EXTENDED $defaultKs").collect()
    val properties = results.filter(_.getString(0) == "Properties").head.getString(1)
    properties should include("SimpleStrategy")
    properties should include("replication_factor,5")
    properties should include("durable_writes,true")
  }

  it should "alter a keyspace" in {
    dropKeyspace(defaultKs)
    waitForKeyspaceToExist(defaultKs, false)
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $defaultKs WITH DBPROPERTIES (class='SimpleStrategy',replication_factor='5')")
    waitForKeyspaceToExist(defaultKs)
    spark.sql(s"ALTER NAMESPACE $defaultKs SET DBPROPERTIES (replication_factor='2')")
    eventually {
      getMetadata().getKeyspace(defaultKs).get().getReplication.get("replication_factor") shouldBe ("2")
    }
  }

  it should "refuse to alter a keyspace with a bad replication class" in {
    dropKeyspace(defaultKs)
    waitForKeyspaceToExist(defaultKs, false)
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $defaultKs WITH DBPROPERTIES (class='SimpleStrategy',replication_factor='5')")
    waitForKeyspaceToExist(defaultKs)
    intercept[CassandraCatalogException] {
      spark.sql(s"ALTER NAMESPACE $defaultKs SET DBPROPERTIES (class='2')")
    }
  }

  it should "drop a keyspace" in {
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $defaultKs WITH DBPROPERTIES (class='SimpleStrategy',replication_factor='5')")
    waitForKeyspaceToExist(defaultKs)
    spark.sql(s"DROP DATABASE $defaultKs")
    waitForKeyspaceToExist(defaultKs, false)
  }

  it should "allow to switch a catalog" in {
    val otherCatalog = "itsatrap"
    spark.conf.set(s"spark.sql.catalog.$otherCatalog", classOf[CassandraCatalog].getCanonicalName)
    spark.sql(s"USE $otherCatalog")
  }
}
