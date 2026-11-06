package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.network.HostEndpointChanged
import org.aspen_ddp.aspen.server.host.ControlFixture

import scala.concurrent.duration.{Duration, SECONDS}
import scala.concurrent.{ExecutionContext, Future}

class HostEndpointSuite extends IntegrationTestSuite with ControlFixture:
  /** The delay reconcileEndpoint schedules a failed attempt's retry after */
  private val RetryDelay = Duration(5, SECONDS)

  /** Like yieldUntil, but each yield also runs the retries the host has scheduled, as their delay elapsing would:
   *  they are posted to the host's control events and handled at once. */
  private def runRetriesUntil(host: Host)(condition: => Boolean, maxYields: Int = 100): Future[Unit] =
    if condition || maxYields == 0 then
      Future.unit
    else
      scheduler(host).runAll()
      host.testingOnlyHandleEvents()
      Future(()).flatMap(_ => runRetriesUntil(host)(condition, maxYields - 1))


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
      (after, timestamp) <- client.getHostStateAndTimestamp(host.hostId)
    yield
      changed should be(true)
      after.address should be("10.9.8.7")
      after.dataPort should be(hs.dataPort + 1)
      after.httpPort should be(hs.httpPort)
      // Carries the new endpoint and the timestamp of the HostState value holding it
      val expected = HostEndpointChanged(host.hostId, "10.9.8.7", hs.dataPort + 1, hs.httpPort,
                                         hs.storeTransferPort, timestamp)
      net.takeCapturedGossip() should be(List(expected))
      // Saved so the retransmits survive a restart
      host.gossipRetransmitter.pendingMessages.keySet should be(Set(expected))
      os.exists(GossipRetransmitter.stateFile(host.rootDir)) should be(true)

  atest("a failed first attempt is retried until the endpoint is updated and gossiped"):
    given ExecutionContext = executionContext

    val host = newHost(newHostDir())
    recorded(host).failEndpointReads.set(2)

    for
      hs <- client.getHostState(host.hostId)
      _ = net.takeCapturedGossip()
      first = host.reconcileEndpoint("10.9.8.7", hs.dataPort, hs.httpPort, hs.storeTransferPort)
      // The returned Future reflects only the first attempt, which failed
      firstFailed <- first.failed
      _ <- runRetriesUntil(host)(net.capturedGossip.nonEmpty)
      _ <- waitForTransactionsToComplete()
      _ <- runRetriesUntil(host)(net.capturedGossip.nonEmpty)
      (after, timestamp) <- client.getHostStateAndTimestamp(host.hostId)
    yield
      firstFailed.getMessage should include("Simulated")
      scheduler(host).scheduled should be(List.fill(2)(RetryDelay))
      after.address should be("10.9.8.7")
      val expected = HostEndpointChanged(host.hostId, "10.9.8.7", hs.dataPort, hs.httpPort,
                                         hs.storeTransferPort, timestamp)
      net.takeCapturedGossip() should be(List(expected))
      // Saved so the retransmits survive a restart
      host.gossipRetransmitter.pendingMessages.keySet should be(Set(expected))
      os.exists(GossipRetransmitter.stateFile(host.rootDir)) should be(true)

  atest("a retry that finds the endpoint already matching stops without gossiping"):
    given ExecutionContext = executionContext

    val host = newHost(newHostDir())
    recorded(host).failEndpointReads.set(1)

    for
      hs <- client.getHostState(host.hostId)
      first = host.reconcileEndpoint(hs.address, hs.dataPort, hs.httpPort, hs.storeTransferPort)
      _ <- first.failed
      _ <- runRetriesUntil(host)(false, 50)
      _ <- waitForTransactionsToComplete()
    yield
      scheduler(host).scheduled should be(List(RetryDelay))
      net.takeCapturedGossip() should be(Nil)
