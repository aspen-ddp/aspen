package org.aspen_ddp.aspen.common.metadata

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.objects.KeyAlreadyExists

import scala.concurrent.ExecutionContext

class HostCreationSuite extends IntegrationTestSuite:

  atest("createHost registers a host with the supplied identity and no devices"):
    given ExecutionContext = executionContext
    for
      hostId <- client.createHost("node_b", "10.0.0.7", 5750, 5751, 5752)
      _      <- waitForTransactionsToComplete()
      hs     <- client.getHostState(hostId)
    yield
      hs.hostId should be(hostId)
      hs.name should be("node_b")
      hs.address should be("10.0.0.7")
      hs.dataPort should be(5750)
      hs.cncPort should be(5751)
      hs.storeTransferPort should be(5752)
      hs.storageDevices should be(Set.empty)

  atest("createHost registers the name so it resolves back to the new id"):
    given ExecutionContext = executionContext
    for
      hostId   <- client.createHost("node_b", "10.0.0.7", 5750, 5751, 5752)
      _        <- waitForTransactionsToComplete()
      byName   <- client.getHostId("node_b")
      allHosts <- client.listHosts()
    yield
      byName should be(hostId)
      allHosts should contain("node_b" -> hostId)
      // The bootstrap host is still there: creating one must not displace another.
      allHosts.map(_._2) should contain(HostId.BootstrapHostId)

  atest("two calls produce distinct hosts, both registered"):
    given ExecutionContext = executionContext
    for
      first    <- client.createHost("node_b", "10.0.0.7", 5750, 5751, 5752)
      _        <- waitForTransactionsToComplete()
      second   <- client.createHost("node_c", "10.0.0.8", 5750, 5751, 5752)
      _        <- waitForTransactionsToComplete()
      allHosts <- client.listHosts()
    yield
      first should not be second
      allHosts should contain("node_b" -> first)
      allHosts should contain("node_c" -> second)

  atest("createHost fails with KeyAlreadyExists when the name is taken"):
    given ExecutionContext = executionContext
    for
      _      <- client.createHost("node_b", "10.0.0.7", 5750, 5751, 5752)
      _      <- waitForTransactionsToComplete()

      before <- client.listHosts()
      err    <- client.createHost("node_b", "10.0.0.9", 6750, 6751, 6752).failed
      _      <- waitForTransactionsToComplete()
      after  <- client.listHosts()
    yield
      // Unwrapped rather than a StopRetrying: the retry strategy fails the promise with the
      // reason, which is what lets add-host report a plain "name already in use". The type is
      // KeyAlreadyExists because the registration goes through Registry.prepareRegister --
      // Registry.DuplicateRegistration belongs to the non-transactional register() path.
      err shouldBe a[KeyAlreadyExists]
      // The allocation and the hosts-tree insert are already staged when the registration
      // rejects, so this holds because the failed transaction is invalidated wholesale.
      after should be(before)

  atest("createHost fails for the bootstrap host's name"):
    given ExecutionContext = executionContext
    recoverToSucceededIf[KeyAlreadyExists](
      client.createHost("testhost", "10.0.0.7", 5750, 5751, 5752))
