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
