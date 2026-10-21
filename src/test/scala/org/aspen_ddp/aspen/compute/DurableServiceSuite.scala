package org.aspen_ddp.aspen.compute

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState, RegisteredTypeFactory, Transaction}
import org.aspen_ddp.aspen.common.{HLCTimestamp, Radicle}
import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.network.ServiceMessage
import org.aspen_ddp.aspen.common.objects.{Key, KeyValueObjectPointer, Value}
import org.aspen_ddp.aspen.compute.impl.SimpleDurableServiceExecutor
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.{Duration, MILLISECONDS}

class ServiceEntrySpec extends AnyFunSuite with Matchers:

  test("ServiceEntry round-trip encode/decode"):
    val typeUUID = UUID.randomUUID()
    val hostId = UUID.randomUUID()
    val leaseExpiry = HLCTimestamp(12345678L)
    val statePointer = KeyValueObjectPointer(Radicle.objectId, Radicle.poolId)
    val entry = ServiceEntry(typeUUID, hostId, leaseExpiry, statePointer)
    val decoded = ServiceEntry.decode(entry.encode())
    decoded.typeUUID shouldBe typeUUID
    decoded.hostId shouldBe hostId
    decoded.leaseExpiry shouldBe leaseExpiry
    decoded.statePointer.id shouldBe statePointer.id
    decoded.statePointer.poolId shouldBe statePointer.poolId
    decoded.isClaimed shouldBe true

  test("ServiceEntry zero hostId represents unclaimed"):
    val entry = ServiceEntry(UUID.randomUUID(), new UUID(0, 0), HLCTimestamp.Zero,
      KeyValueObjectPointer(Radicle.objectId, Radicle.poolId))
    val decoded = ServiceEntry.decode(entry.encode())
    decoded.hostId shouldBe new UUID(0, 0)
    decoded.leaseExpiry shouldBe HLCTimestamp.Zero
    decoded.isClaimed shouldBe false


class DurableServiceSuite extends IntegrationTestSuite:

  val testHostId: HostId = HostId(UUID.fromString("00000000-0000-0000-0000-000000000001"))

  // Fixed typeUUID so the same factory object is always registered
  val fixedTypeUUID: UUID = UUID.fromString("aaaaaaaa-0000-0000-0000-000000000001")

  // Per-test promises; reset in subFixtureSetup
  private var claimedPromise: Promise[Unit]  = scala.compiletime.uninitialized
  private var shutdownPromise: Promise[Unit] = scala.compiletime.uninitialized
  private var receivedPromise: Promise[ServiceMessage] = scala.compiletime.uninitialized

  // Stable factory object — captures promises by reference (reads them at call time)
  private val stableFactory: DurableServiceFactory = new DurableServiceFactory:
    val typeUUID: UUID = fixedTypeUUID
    def createService(c: AspenClient, ptr: KeyValueObjectPointer, state: KeyValueObjectState): DurableService =
      claimedPromise.trySuccess(())
      new DurableService:
        def shutdown(): Unit = shutdownPromise.trySuccess(())
        override def receiveMessage(msg: ServiceMessage): Unit = receivedPromise.trySuccess(msg)

  override def userTypeFactories: List[RegisteredTypeFactory] = List(stableFactory)

  override def subFixtureSetup(): Unit =
    claimedPromise  = Promise[Unit]()
    shutdownPromise = Promise[Unit]()
    receivedPromise = Promise[ServiceMessage]()

  // Executors made by the running test. Tests shut theirs down on success, but a failed or timed
  // out test never reaches that, and its executor would keep scanning and renewing for the life
  // of the process, loading every test after it. Teardown runs on every outcome.
  private val executors = new java.util.concurrent.ConcurrentLinkedQueue[SimpleDurableServiceExecutor]()

  override def subFixtureTeardown(): Unit =
    var exec = executors.poll()
    while exec != null do
      exec.shutdown()
      exec = executors.poll()

  def makeExecutor(): SimpleDurableServiceExecutor =
    val exec = new SimpleDurableServiceExecutor(
      client, testHostId, client.backgroundTaskManager,
      // No test here depends on a lease expiring, so keep it far longer than any scheduling
      // stall: a lease lost to a starved process shuts the service down mid-test.
      leaseDuration        = Duration(30000, MILLISECONDS),
      renewalInterval      = Duration(50, MILLISECONDS),
      minScanInterval      = Duration(50, MILLISECONDS),
      maxScanInterval      = Duration(100, MILLISECONDS),
      claimDelayPerService = Duration(0, MILLISECONDS)
    )
    executors.add(exec)
    exec

  atest("registerService creates a claimable TKVL entry"):
    given ExecutionContext = executionContext
    val exec = makeExecutor()
    val typeUUID = UUID.randomUUID()
    val svcUUID  = UUID.randomUUID()
    for
      _ <- exec.registerService(typeUUID, svcUUID, Map.empty)
      vs <- exec.servicesTkvl.get(Key(svcUUID))
    yield
      exec.shutdown()
      vs shouldBe defined
      val entry = ServiceEntry.decode(vs.get.value.bytes)
      entry.typeUUID shouldBe typeUUID
      entry.hostId shouldBe ServiceEntry.UnclaimedHostId
      entry.leaseExpiry shouldBe HLCTimestamp.Zero

  atest("registerService is idempotent"):
    given ExecutionContext = executionContext
    val exec = makeExecutor()
    val typeUUID = UUID.randomUUID()
    val svcUUID  = UUID.randomUUID()
    for
      _ <- exec.registerService(typeUUID, svcUUID, Map.empty)
      _ <- exec.registerService(typeUUID, svcUUID, Map.empty)
      vs <- exec.servicesTkvl.get(Key(svcUUID))
    yield
      exec.shutdown()
      vs shouldBe defined

  atest("executor claims an unclaimed service on scan"):
    given ExecutionContext = executionContext
    val svcUUID = UUID.randomUUID()
    val exec = makeExecutor()
    for
      _  <- exec.registerService(fixedTypeUUID, svcUUID, Map.empty)
      _  <- claimedPromise.future
      vs <- exec.servicesTkvl.get(Key(svcUUID))
    yield
      exec.shutdown()
      vs shouldBe defined
      val entry = ServiceEntry.decode(vs.get.value.bytes)
      entry.hostId shouldBe testHostId.uuid
      entry.leaseExpiry > HLCTimestamp.Zero shouldBe true

  atest("executor renews lease before expiry"):
    given ExecutionContext = executionContext
    val svcUUID = UUID.randomUUID()
    val exec = makeExecutor()
    val deadline = System.nanoTime() + Duration(120000, MILLISECONDS).toNanos

    def leaseExpiry(): Future[HLCTimestamp] =
      exec.servicesTkvl.get(Key(svcUUID)).map(v => ServiceEntry.decode(v.get.value.bytes).leaseExpiry)

    // Poll until the expiry moves past `first`. The renewal interval is 50ms but a starved
    // process can run it arbitrarily late, so wait on the condition rather than a fixed delay.
    def awaitRenewal(first: HLCTimestamp): Future[HLCTimestamp] =
      leaseExpiry().flatMap: current =>
        if current > first then
          Future.successful(current)
        else if System.nanoTime() > deadline then
          Future.failed(new AssertionError(s"lease was not renewed; expiry stayed at $first"))
        else
          val p = Promise[Unit]()
          client.backgroundTaskManager.schedule(Duration(50, MILLISECONDS))(p.trySuccess(()))
          p.future.flatMap(_ => awaitRenewal(first))

    val test =
      for
        _      <- exec.registerService(fixedTypeUUID, svcUUID, Map.empty)
        _      <- claimedPromise.future                // wait for initial claim
        first  <- leaseExpiry()
        second <- awaitRenewal(first)
      yield second > first shouldBe true

    test.andThen(_ => exec.shutdown())

  atest("executor calls shutdown when another host overwrites the TKVL entry"):
    given ExecutionContext = executionContext
    val svcUUID = UUID.randomUUID()
    val exec = makeExecutor()
    for
      _  <- exec.registerService(fixedTypeUUID, svcUUID, Map.empty)
      _  <- claimedPromise.future                        // wait for initial claim
      vs <- exec.servicesTkvl.get(Key(svcUUID))
      _  <- {
              // Overwrite the TKVL entry with a different hostId, simulating another host taking over
              val fakeHostId  = UUID.randomUUID()
              val fakeEntry   = ServiceEntry.decode(vs.get.value.bytes).copy(hostId = fakeHostId)
              client.transact: tx =>
                given Transaction = tx
                exec.servicesTkvl.set(Key(svcUUID), Value(fakeEntry.encode()))(using tx)
            }
      _  <- shutdownPromise.future                       // wait for shutdown() to be called
    yield
      exec.shutdown()
      succeed

  atest("executor calls shutdown when service is removed from the TKVL"):
    given ExecutionContext = executionContext
    val svcUUID = UUID.randomUUID()
    val exec = makeExecutor()
    for
      _  <- exec.registerService(fixedTypeUUID, svcUUID, Map.empty)
      _  <- claimedPromise.future                        // wait for initial claim
      _  <- {
              // Delete the TKVL entry directly, bypassing unregisterService (not yet implemented)
              client.transact: tx =>
                given Transaction = tx
                exec.servicesTkvl.delete(Key(svcUUID))(using tx)
            }
      _  <- shutdownPromise.future                       // wait for shutdown() to be called by scan
    yield
      exec.shutdown()
      succeed

  atest("unregisterService removes TKVL entry and shuts down owned service"):
    given ExecutionContext = executionContext
    val svcUUID = UUID.randomUUID()
    val exec    = makeExecutor()
    for
      _  <- exec.registerService(fixedTypeUUID, svcUUID, Map.empty)
      _  <- claimedPromise.future         // wait for service to be claimed
      _  <- exec.unregisterService(svcUUID)
      vs <- exec.servicesTkvl.get(Key(svcUUID))
      _  <- shutdownPromise.future        // verify shutdown() was called
    yield
      exec.shutdown()
      vs shouldBe None

  atest("unregisterService on unowned service just removes TKVL entry"):
    given ExecutionContext = executionContext
    val svcUUID = UUID.randomUUID()
    val exec    = makeExecutor()
    // Use a typeUUID that has no registered factory — entry stays unclaimed
    val unknownTypeUUID = UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff")
    val delayPromise = Promise[Unit]()
    for
      _ <- exec.registerService(unknownTypeUUID, svcUUID, Map.empty)
      _ <- {
        // Wait long enough for a scan to run and fail to find a factory
        exec.backgroundTasks.schedule(Duration(200, MILLISECONDS)):
          delayPromise.trySuccess(())
        delayPromise.future
      }
      _ <- exec.unregisterService(svcUUID)
      vs <- exec.servicesTkvl.get(Key(svcUUID))
    yield
      exec.shutdown()
      vs shouldBe None

  atest("deliverMessage routes to the owning service's receiveMessage"):
    given ExecutionContext = executionContext
    val svcUUID = UUID.randomUUID()
    val content = Array[Byte](1, 2, 3)
    val exec = makeExecutor()
    for
      _   <- exec.registerService(fixedTypeUUID, svcUUID, Map.empty)
      _   <- claimedPromise.future
      _    = exec.deliverMessage(ServiceMessage(testHostId, client.clientId, svcUUID, content))
      msg <- receivedPromise.future
    yield
      exec.shutdown()
      msg.serviceUUID shouldBe svcUUID
      msg.encodedContent.toList shouldBe content.toList

  atest("deliverMessage is a no-op for a service this host does not own"):
    given ExecutionContext = executionContext
    val exec = makeExecutor()
    noException should be thrownBy exec.deliverMessage(
      ServiceMessage(testHostId, client.clientId, UUID.randomUUID(), Array.emptyByteArray))
    exec.shutdown()
    Future.successful(succeed)

  atest("getServiceHost returns the claiming host"):
    given ExecutionContext = executionContext
    val svcUUID = UUID.randomUUID()
    val exec = makeExecutor()
    for
      _  <- exec.registerService(fixedTypeUUID, svcUUID, Map.empty)
      _  <- claimedPromise.future
      oh <- client.getServiceHost(svcUUID)
    yield
      exec.shutdown()
      oh shouldBe Some(testHostId)

  atest("getServiceHost returns None for an unknown service"):
    given ExecutionContext = executionContext
    for
      oh <- client.getServiceHost(UUID.randomUUID())
    yield
      oh shouldBe None

  atest("sendServiceMessage sends a ServiceMessage to the owning host"):
    given ExecutionContext = executionContext
    val svcUUID = UUID.randomUUID()
    val content = Array[Byte](9, 8, 7)
    val exec = makeExecutor()
    for
      _ <- exec.registerService(fixedTypeUUID, svcUUID, Map.empty)
      _ <- claimedPromise.future
      _ <- client.sendServiceMessage(svcUUID, content)
    yield
      exec.shutdown()
      val sent = net.capturedHostMessages.collect:
        case m: ServiceMessage if m.serviceUUID == svcUUID => m
      sent.size shouldBe 1
      sent.head.toHost shouldBe testHostId
      sent.head.fromClient shouldBe client.clientId
      sent.head.encodedContent.toList shouldBe content.toList
