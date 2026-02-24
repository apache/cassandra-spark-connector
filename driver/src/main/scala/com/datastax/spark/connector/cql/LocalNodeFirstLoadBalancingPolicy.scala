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

import java.net.{InetSocketAddress, NetworkInterface}
import java.nio.ByteBuffer
import java.util
import java.util.UUID
import java.util.function.Predicate

import com.datastax.oss.driver.api.core.config.{DefaultDriverOption, DriverOption}
import com.datastax.oss.driver.api.core.context.DriverContext
import com.datastax.oss.driver.api.core.loadbalancing.{LoadBalancingPolicy, NodeDistance}
import com.datastax.oss.driver.api.core.metadata.token.Token
import com.datastax.oss.driver.api.core.metadata.{Node, NodeState}
import com.datastax.oss.driver.api.core.session.{Request, Session}
import com.datastax.oss.driver.internal.core.context.InternalDriverContext
import com.datastax.oss.driver.internal.core.metadata.MetadataManager
import com.datastax.oss.driver.internal.core.util.Reflection
import com.datastax.oss.driver.internal.core.util.collection.SimpleQueryPlan
import com.datastax.spark.connector.cql.LocalNodeFirstLoadBalancingPolicy.{LoadBalancingShuffleNodes, _}
import com.datastax.spark.connector.util.DriverUtil.{toAddress, toOption}

import scala.annotation.nowarn
import scala.jdk.CollectionConverters._
import scala.util.Random

/** Selects local node first and then nodes in local DC in random order. Never selects nodes from other DCs.
  * For writes, if a statement has a routing key set, this LBP is token aware - it prefers the nodes which
  * are replicas of the computed token to the other nodes. */
class LocalNodeFirstLoadBalancingPolicy(context: DriverContext, profileName: String)
  extends LoadBalancingPolicy {

  private val profile = context.getConfig.getProfile(profileName)
  private val localDC: Option[String] = Option(profile.getString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, null))
  private val shuffleReplicas: Boolean = profile.getBoolean(LoadBalancingShuffleNodes, true)

  private var nodes = Set.empty[Node]
  private var dcToUse = ""
  private val random = new Random
  private val nodeFilter: Option[Predicate[Node]] = {
    val internalContext = context.asInstanceOf[InternalDriverContext]
    (Option(internalContext.getNodeFilter(profileName)): @nowarn("cat=deprecation"))
      .orElse {
        toOption(Reflection.buildFromConfig(internalContext, profileName, DefaultDriverOption.LOAD_BALANCING_FILTER_CLASS, classOf[Predicate[Node]])): @nowarn("cat=deprecation")
      }
  }

  private var distanceReporter: LoadBalancingPolicy.DistanceReporter = _
  private val metadataManager: MetadataManager = context.asInstanceOf[InternalDriverContext].getMetadataManager

  private def distance(node: Node): NodeDistance =
    if (nodeFilter.forall(_.test(node)) && node.getDatacenter == dcToUse) {
      sameDCNodeDistance(node)
    } else {
      // this insures we keep remote hosts out of our list entirely, even when we get notified of newly joined nodes
      NodeDistance.IGNORED
    }

  override def init(nodes: util.Map[UUID, Node], distanceReporter: LoadBalancingPolicy.DistanceReporter): Unit = {
    this.nodes = nodes.asScala.values.toSet
    // use explicitly set DC if available, otherwise see if all contact points have same DC
    // if so, use that DC; if not, throw an error
    val contactPoints = metadataManager.getContactPoints.asScala.flatMap(toAddress).toSet
    dcToUse = localDC.getOrElse(determineDataCenter(contactPoints, this.nodes))
    this.distanceReporter = distanceReporter

    this.nodes.foreach { node =>
      distanceReporter.setDistance(node, distance(node))
    }
  }

  private def tokenUnawareQueryPlan(statement: Request): Seq[Node] = {
    sortNodesByStatusAndProximity(dcToUse, nodes)
  }

  private def replicaAwareQueryPlan(statement: Request, replicas: Set[Node]): Seq[Node] = {
    val (localReplica, otherReplicas) = replicas.partition(isLocalHost)
    lazy val maybeShuffledOtherReplicas = if (shuffleReplicas) random.shuffle(otherReplicas.toIndexedSeq) else otherReplicas

    lazy val otherNodes = tokenUnawareQueryPlan(statement).iterator
      .filter(node => !replicas.contains(node) && distance(node) != NodeDistance.IGNORED)

    (localReplica.iterator #:: maybeShuffledOtherReplicas.iterator #:: otherNodes #:: LazyList.empty).flatten
  }

  def tokenMap =
    Option(metadataManager.getMetadata.getTokenMap.orElse(null))
      .orElse(throw new IllegalArgumentException("Unable to get Token Metadata"))

  // copied and adjusted from DefaultLoadBalancingPolicy
  private def getReplicas(request: Request, session: Session): Set[Node] = {
    if (request == null || session == null) {
      Set()
    } else {
      Option(request.getKeyspace)
        .orElse(Option(request.getRoutingKeyspace))
        .orElse(toOption(session.getKeyspace))
        .flatMap { keyspace =>

          def replicasForToken(token: Token) = {
            tokenMap.map(_.getReplicas(keyspace, token))
          }

          def replicasForRoutingKey(key: ByteBuffer) = {
            tokenMap.map(_.getReplicas(keyspace, key))
          }

          Option(request.getRoutingToken).flatMap(replicasForToken)
            .orElse(Option(request.getRoutingKey).flatMap(replicasForRoutingKey))
            .map(_.asScala.toSet)
        }.getOrElse(Set())
    }
  }

  override def newQueryPlan(request: Request, session: Session): util.Queue[Node] = {
    val replicas = getReplicas(request, session)
      .filter(node => node.getState == NodeState.UP && distance(node) != NodeDistance.IGNORED)

    val nodes = if (replicas.nonEmpty) {
      replicaAwareQueryPlan(request, replicas)
    } else {
      tokenUnawareQueryPlan(request)
    }
    new SimpleQueryPlan(nodes: _*)
  }

  override def onAdd(node: Node): Unit = {
    // The added host might be a "better" version of a host already in the set.
    // The nodes added in the init call don't have DC and rack set.
    // Therefore we want to really replace the object now, to get full information on DC:
    nodes -= node
    nodes += node
    distanceReporter.setDistance(node, distance(node))
  }

  override def onRemove(node: Node): Unit = {
    nodes -= node
  }

  override def close(): Unit = {}

  override def onUp(node: Node): Unit = {
    distanceReporter.setDistance(node, distance(node))
  }

  override def onDown(node: Node): Unit = {}

  private def sameDCNodeDistance(node: Node): NodeDistance =
    if (isLocalHost(node))
      NodeDistance.LOCAL
    else
      NodeDistance.REMOTE
}

object LocalNodeFirstLoadBalancingPolicy {

  private val random = new Random

  private val localAddresses =
    NetworkInterface.getNetworkInterfaces.asScala.flatMap(_.getInetAddresses.asScala).toSet

  /** Returns true if given host is local host */
  def isLocalHost(node: Node): Boolean = {
    toAddress(node).exists(hostAddress => hostAddress.getAddress.isLoopbackAddress || localAddresses.contains(hostAddress.getAddress))
  }

  /** Sorts nodes in the following order:
    * 1. local node in a given DC
    * 2. live nodes in a given DC
    * 3. the rest of nodes in a given DC
    *
    * Nodes within a group are ordered randomly. Nodes from other DCs are not included. */
  def sortNodesByStatusAndProximity(dc: String, nodesToSort: Set[Node]): Seq[Node] = {
    val grouped = nodesToSort.groupBy {
      case node if node.getDatacenter != dc => None
      case node if node.getState != NodeState.UP => Some(2)
      case node if !isLocalHost(node) => Some(1)
      case _ => Some(0)
    } - None

    grouped.toSeq.sortBy(_._1.get).flatMap {
      case (_, nodes) => random.shuffle(nodes.toIndexedSeq)
    }
  }

  /** Returns a common data center name of the given contact points.
    *
    * For each contact point there must be a [[Node]] in `allNodes` collection in order to determine its data center
    * name. If contact points belong to more than a single data center, an [[IllegalArgumentException]] is thrown.
    */
  def determineDataCenter(contactPoints: Set[InetSocketAddress], allNodes: Set[Node]): String = {
    val maybeResolved = contactPoints.map(x =>
      if (x.isUnresolved) new InetSocketAddress(x.getHostString, x.getPort) else x)
    val dcs = allNodes
      .filter(node => toAddress(node).exists(maybeResolved.contains))
      .flatMap(node => Option(node.getDatacenter))
    assert(dcs.nonEmpty, "There are no contact points in the given set of hosts")
    require(dcs.size == 1, s"Contact points contain multiple data centers: ${dcs.mkString(", ")}")
    dcs.head
  }

  val LoadBalancingShuffleNodes: DriverOption = new DriverOption {
    override def getPath: String = "basic.load-balancing-policy.shuffle-replicas"
  }

}
