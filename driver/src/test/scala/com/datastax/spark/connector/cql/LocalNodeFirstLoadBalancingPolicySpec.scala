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

package com.datastax.spark.connector.cql

import java.net.InetSocketAddress
import java.util.function.Predicate
import java.util.{Optional, UUID}

import com.datastax.oss.driver.api.core.config.DefaultDriverOption.{LOAD_BALANCING_FILTER_CLASS, LOAD_BALANCING_LOCAL_DATACENTER}
import com.datastax.oss.driver.api.core.config.{DriverConfig, DriverExecutionProfile}
import com.datastax.oss.driver.api.core.context.DriverContext
import com.datastax.oss.driver.api.core.loadbalancing.{LoadBalancingPolicy, NodeDistance}
import com.datastax.oss.driver.api.core.metadata.{EndPoint, Node}
import com.datastax.oss.driver.internal.core.context.InternalDriverContext
import com.datastax.oss.driver.internal.core.metadata.MetadataManager
import com.datastax.spark.connector.util.DriverUtil
import org.mockito.Mockito._
import org.mockito.{Matchers => m}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar

import scala.jdk.CollectionConverters._

class NodeFilter(context: DriverContext, profileName: String) extends Predicate[Node] {
  override def test(t: Node): Boolean = DriverUtil.toAddress(t).get.getHostName.equals("192.168.123.2")
}

class LocalNodeFirstLoadBalancingPolicySpec extends FlatSpec with Matchers with MockitoSugar with BeforeAndAfterEach {

  private val dc = "superDC"
  private val localNode0 = nodeMock("127.0.0.1")
  private val remoteNode1 = nodeMock("192.168.123.1")
  private val remoteNode2 = nodeMock("192.168.123.2")
  private val remoteNode3 = nodeMock("192.168.123.3")

  def nodeMock(address: String): Node = {
    val endpoint = mock[EndPoint]
    when(endpoint.resolve).thenReturn(new InetSocketAddress(address, 9042))

    val node = mock[Node]
    when(node.getHostId).thenReturn(UUID.randomUUID())
    when(node.getBroadcastAddress).thenReturn(Optional.of(new InetSocketAddress(address, 9042)))
    when(node.getEndPoint).thenReturn(endpoint)
    when(node.getDatacenter).thenReturn(dc)
    node
  }

  def toJMap(nodes: Node*): java.util.Map[UUID, Node] = {
    nodes.map(node => (node.getHostId, node)).toMap.asJava
  }

  private val profileName = "someProfile"
  private val context = mock[InternalDriverContext]
  private val config = mock[DriverConfig]
  private val profile = mock[DriverExecutionProfile]
  private val metaManager = mock[MetadataManager]

  override def beforeEach() {
    when(profile.getString(m.eq(LOAD_BALANCING_LOCAL_DATACENTER))).thenReturn(dc)
    when(profile.getString(m.eq(LOAD_BALANCING_LOCAL_DATACENTER), m.any())).thenReturn(dc)

    when(config.getProfile(m.eq(profileName))).thenReturn(profile)
    when(context.getConfig).thenReturn(config)

    when(context.getMetadataManager).thenReturn(metaManager)
  }

  it should "set distance to LOCAL for local node in local dc" in {
    val policy = new LocalNodeFirstLoadBalancingPolicy(context, profileName)
    val reporter = mock[LoadBalancingPolicy.DistanceReporter]

    policy.init(toJMap(localNode0), reporter)

    verify(reporter, times(1)).setDistance(localNode0, NodeDistance.LOCAL)
    verifyNoMoreInteractions(reporter)
  }

  it should "set distance to IGNORED for local node in different dc" in {
    val policy = new LocalNodeFirstLoadBalancingPolicy(context, profileName)
    val reporter = mock[LoadBalancingPolicy.DistanceReporter]

    when(localNode0.getDatacenter).thenReturn("some_other_dc")

    policy.init(toJMap(localNode0), reporter)

    verify(reporter, times(1)).setDistance(localNode0, NodeDistance.IGNORED)
    verifyNoMoreInteractions(reporter)
  }

  it should "set distance to REMOTE for remote nodes" in {
    val policy = new LocalNodeFirstLoadBalancingPolicy(context, profileName)
    val reporter = mock[LoadBalancingPolicy.DistanceReporter]

    policy.init(toJMap(remoteNode1, remoteNode2), reporter)

    verify(reporter, times(1)).setDistance(remoteNode1, NodeDistance.REMOTE)
    verify(reporter, times(1)).setDistance(remoteNode2, NodeDistance.REMOTE)
    verifyNoMoreInteractions(reporter)
  }

  it should "apply configured node filter" in {
    when(profile.isDefined(m.eq(LOAD_BALANCING_FILTER_CLASS))).thenReturn(true)
    when(profile.getString(m.eq(LOAD_BALANCING_FILTER_CLASS))).thenReturn(classOf[NodeFilter].getCanonicalName)

    val policy = new LocalNodeFirstLoadBalancingPolicy(context, profileName)
    val reporter = mock[LoadBalancingPolicy.DistanceReporter]

    policy.init(toJMap(remoteNode1, localNode0, remoteNode2, remoteNode3), reporter)

    verify(reporter, times(1)).setDistance(remoteNode1, NodeDistance.IGNORED)
    verify(reporter, times(1)).setDistance(localNode0, NodeDistance.IGNORED)
    verify(reporter, times(1)).setDistance(remoteNode2, NodeDistance.REMOTE)
    verify(reporter, times(1)).setDistance(remoteNode3, NodeDistance.IGNORED)
    verifyNoMoreInteractions(reporter)
  }
}
