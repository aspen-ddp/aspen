package org.aspen_ddp.aspen.compute

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState, RegisteredTypeFactory}
import org.aspen_ddp.aspen.common.{HLCTimestamp, Radicle}
import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.objects.{Key, KeyValueObjectPointer, Value}
import org.aspen_ddp.aspen.compute.impl.SimpleDurableServiceExecutor
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import scala.concurrent.{ExecutionContext, Promise}
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

  // Per-test promise; reset in subFixtureSetup
  private var claimedPromise: Promise[Unit] = scala.compiletime.uninitialized

  // Stable factory object — captures claimedPromise by reference (reads it at call time)
  private val stableFactory: DurableServiceFactory = new DurableServiceFactory:
    val typeUUID: UUID = fixedTypeUUID
    def createService(c: AspenClient, ptr: KeyValueObjectPointer, state: KeyValueObjectState): DurableService =
      claimedPromise.trySuccess(())
      new DurableService { def shutdown(): Unit = () }

  override def userTypeFactories: List[RegisteredTypeFactory] = List(stableFactory)

  override def subFixtureSetup(): Unit =
    claimedPromise = Promise[Unit]()

  def makeExecutor(): SimpleDurableServiceExecutor =
    new SimpleDurableServiceExecutor(
      client, testHostId, client.backgroundTaskManager,
      leaseDuration        = Duration(200, MILLISECONDS),
      renewalInterval      = Duration(50, MILLISECONDS),
      minScanInterval      = Duration(50, MILLISECONDS),
      maxScanInterval      = Duration(100, MILLISECONDS),
      claimDelayPerService = Duration(0, MILLISECONDS)
    )

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
