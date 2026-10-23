package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.network.HostEndpointChanged

import scala.concurrent.ExecutionContext

class HostEndpointSuite extends IntegrationTestSuite with HostTestHarness:

  atest("unchanged endpoint writes nothing and gossips nothing"):
    given ExecutionContext = executionContext

    val host = newHost(newHostDir())

    for
      hs <- client.getHostState(host.hostId)
      changed <- host.reconcileEndpoint(hs.address, hs.dataPort, hs.httpPort, hs.storeTransferPort)
      after <- client.getHostState(host.hostId)
    yield
      changed should be(false)
      after should be(hs)
      net.takeCapturedGossip() should be(Nil)

  atest("changed endpoint updates the host object, then gossips"):
    given ExecutionContext = executionContext

    val host = newHost(newHostDir())

    for
      hs <- client.getHostState(host.hostId)
      _ = net.takeCapturedGossip()
      changed <- host.reconcileEndpoint("10.9.8.7", hs.dataPort + 1, hs.httpPort, hs.storeTransferPort)
      after <- client.getHostState(host.hostId)
    yield
      changed should be(true)
      after.address should be("10.9.8.7")
      after.dataPort should be(hs.dataPort + 1)
      after.httpPort should be(hs.httpPort)
      net.takeCapturedGossip() should be(List(HostEndpointChanged(host.hostId)))
      // Saved so the retransmits survive a restart
      host.gossipRetransmitter.pendingMessages.keySet should be(Set(HostEndpointChanged(host.hostId)))
      os.exists(GossipRetransmitter.stateFile(host.rootDir)) should be(true)

  atest("a failed first attempt is retried until the endpoint is updated and gossiped"):
    given ExecutionContext = executionContext

    val host = newHost(newHostDir())
    host.failEndpointReads.set(2)

    for
      hs <- client.getHostState(host.hostId)
      _ = net.takeCapturedGossip()
      first = host.reconcileEndpoint("10.9.8.7", hs.dataPort, hs.httpPort, hs.storeTransferPort)
      // The returned Future reflects only the first attempt, which failed
      firstFailed <- first.failed
      _ <- yieldUntil(net.capturedGossip.nonEmpty)
      _ <- waitForTransactionsToComplete()
      _ <- yieldUntil(net.capturedGossip.nonEmpty)
      after <- client.getHostState(host.hostId)
    yield
      firstFailed.getMessage should include("Simulated")
      host.endpointRetriesScheduled.get should be(2)
      after.address should be("10.9.8.7")
      net.takeCapturedGossip() should be(List(HostEndpointChanged(host.hostId)))
      // Saved so the retransmits survive a restart
      host.gossipRetransmitter.pendingMessages.keySet should be(Set(HostEndpointChanged(host.hostId)))
      os.exists(GossipRetransmitter.stateFile(host.rootDir)) should be(true)

  atest("a retry that finds the endpoint already matching stops without gossiping"):
    given ExecutionContext = executionContext

    val host = newHost(newHostDir())
    host.failEndpointReads.set(1)

    for
      hs <- client.getHostState(host.hostId)
      first = host.reconcileEndpoint(hs.address, hs.dataPort, hs.httpPort, hs.storeTransferPort)
      _ <- first.failed
      _ <- yieldUntil(false, 50)
      _ <- waitForTransactionsToComplete()
    yield
      host.endpointRetriesScheduled.get should be(1)
      net.takeCapturedGossip() should be(Nil)
