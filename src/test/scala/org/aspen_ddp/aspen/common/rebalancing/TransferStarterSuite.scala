package org.aspen_ddp.aspen.common.rebalancing

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.ida.{IDA, Replication}
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId, StorageDeviceSetId, StorageDeviceSetState,
                                            StorageDeviceState}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{Duration, MILLISECONDS}

/** TransferStarter tests shared by every pool shape. Both concrete suites have three stores on the
 *  bootstrap device and a floor of 2 (`min(writeThreshold + 2, width - 1)`), so exactly one
 *  transfer is admissible at a time. */
abstract class TransferStarterSuiteBase extends IntegrationTestSuite:

  protected val poolId: PoolId = PoolId.BootstrapPoolId
  protected val sourceId: StorageDeviceId = StorageDeviceId.BootstrapStorageDeviceId
  protected val store0: StoreId = StoreId(poolId, 0)
  protected val store1: StoreId = StoreId(poolId, 1)

  protected def setup(): Future[Unit] =
    given ExecutionContext = executionContext
    for
      _ <- net.createSecondDevice()
      _ <- net.createThirdDevice()
      _ <- waitForTransactionsToComplete()
    yield ()

  protected def statusOf(deviceId: StorageDeviceId, storeId: StoreId): Future[Option[StorageDeviceState.StoreStatus]] =
    given ExecutionContext = executionContext
    client.getStorageDeviceState(deviceId).map(_.stores.get(storeId).map(_.status))

  atest("start returns Started and leaves the source TransferringOut"):
    given ExecutionContext = executionContext
    for
      _ <- setup()
      result <- TransferStarter.start(client, store0, net.secondDeviceId)
      _ <- waitForTransactionsToComplete()
      src <- statusOf(sourceId, store0)
      dst <- statusOf(net.secondDeviceId, store0)
    yield
      result should be(StartResult.Started)
      src should be(Some(StorageDeviceState.StoreStatus.TransferringOut))
      dst should be(Some(StorageDeviceState.StoreStatus.TransferringIn))

  atest("offlineHosts is the registered hosts the network reports offline"):
    given ExecutionContext = executionContext
    val host = HostId.BootstrapHostId
    for
      _ <- client.hostLivenessReady
      before = client.offlineHosts()
      _ = net.offlineHostIds = Set(host)
      after = client.offlineHosts()
      // Unaffected by a host the registry has never heard of
      _ = net.offlineHostIds = Set(host, HostId(java.util.UUID.randomUUID()))
      withUnknown = client.offlineHosts()
    yield
      before should be(Set.empty)
      after should be(Set(host))
      withUnknown should be(Set(host))

  atest("start is Rejected, and starts nothing, when the destination's host is offline"):
    given ExecutionContext = executionContext
    for
      _ <- setup()
      _ <- client.hostLivenessReady
      _ = net.offlineHostIds = Set(HostId.BootstrapHostId)
      result <- TransferStarter.start(client, store0, net.secondDeviceId)
      _ <- waitForTransactionsToComplete()
      src <- statusOf(sourceId, store0)
    yield
      net.offlineHostIds = Set.empty
      result should be(StartResult.Rejected)
      src should be(Some(StorageDeviceState.StoreStatus.Active))

  atest("a started transfer stays started"):
    given ExecutionContext = executionContext
    for
      _ <- setup()
      result <- TransferStarter.start(client, store0, net.secondDeviceId)
      _ <- waitForTransactionsToComplete()
      // A later, unrelated look must still find it: verification did not roll it back.
      planning <- State.getStateForPoolMigration(client, poolId, net.secondDeviceId)
      src <- statusOf(sourceId, store0)
    yield
      result should be(StartResult.Started)
      src should be(Some(StorageDeviceState.StoreStatus.TransferringOut))
      TransferSafety.isBelowFloor(planning, Set.empty, poolId) should be(false)

  atest("start returns Rejected when the rule fails on fresh state and starts nothing"):
    given ExecutionContext = executionContext
    for
      _ <- setup()
      first <- TransferStarter.start(client, store0, net.secondDeviceId)
      _ <- waitForTransactionsToComplete()
      second <- TransferStarter.start(client, store1, net.thirdDeviceId)
      _ <- waitForTransactionsToComplete()
      src <- statusOf(sourceId, store1)
      dst <- statusOf(net.thirdDeviceId, store1)
    yield
      first should be(StartResult.Started)
      second should be(StartResult.Rejected)
      src should be(Some(StorageDeviceState.StoreStatus.Active))
      dst should be(None)

  atest("start rolls back when the pool is below the floor after its start commits"):
    given ExecutionContext = executionContext
    // The race: another initiator starts a transfer in the same pool after this start's
    // admission check passed but before its transferStore commits.
    def racingStart(): Future[Unit] = client.transferStore(store1, net.thirdDeviceId)
    for
      _ <- setup()
      result <- TransferStarter.start(client, store0, net.secondDeviceId, racingStart)
      _ <- waitForTransactionsToComplete()
      src0 <- statusOf(sourceId, store0)
      dst0 <- statusOf(net.secondDeviceId, store0)
      src1 <- statusOf(sourceId, store1)
    yield
      result should be(StartResult.RolledBack)
      src0 should be(Some(StorageDeviceState.StoreStatus.Active))
      dst0 should be(None)
      // The racing transfer is not ours to roll back.
      src1 should be(Some(StorageDeviceState.StoreStatus.TransferringOut))

  atest("start fails when transferStore fails"):
    given ExecutionContext = executionContext
    // The racing initiator moves the very store being started, so transferStore sees it is no
    // longer Active and stops retrying with StoreNotActive.
    def racingStart(): Future[Unit] = client.transferStore(store0, net.thirdDeviceId)
    for
      _ <- setup()
      result <- TransferStarter.start(client, store0, net.secondDeviceId, racingStart)
                  .map(Right(_)).recover { case e => Left(e) }
    yield
      result.isLeft should be(true)

  /** A store of the bootstrap pool at an index no host serves, so neither a host's reconcile nor
   *  its usage tracker acts on the metadata written for it. */
  private val phantom = StoreId(poolId, 9.toByte)

  atest("start returns NoRoom, and starts nothing, when the destination is already receiving too much"):
    given ExecutionContext = executionContext
    for
      _ <- setup()
      // More than the second device's 1,000,000 bytes are already on their way to it.
      _ <- setStoreSize(phantom, 2_000_000L)
      _ <- setDeviceState(net.secondDeviceId)(_.setStoreEntry(phantom,
             StorageDeviceState.StoreStatus.TransferringIn, Some(sourceId)))
      result <- TransferStarter.start(client, store0, net.secondDeviceId)
      _ <- waitForTransactionsToComplete()
      src <- statusOf(sourceId, store0)
      dst <- statusOf(net.secondDeviceId, store0)
    yield
      result should be(StartResult.NoRoom)
      src should be(Some(StorageDeviceState.StoreStatus.Active))
      dst should be(None)

  atest("start does not count a planned transfer against its own destination"):
    given ExecutionContext = executionContext
    for
      _ <- setup()
      // 600,000 bytes fit the second device's 1,000,000 once, not twice. A host's usage tracker
      // may lower the recorded size mid-test, which still fits.
      _ <- setStoreSize(store0, 600_000L)
      setPtr <- client.getStorageDeviceSetPointer(StorageDeviceSetId.BootstrapStorageDeviceSetId)
      setDos <- client.read(setPtr)
      _ <- client.transactUntilSuccessful: tx =>
             tx.overwrite(setPtr, setDos.revision, DataBuffer(StorageDeviceSetState(setDos).copy(
               pendingTransfers = List((store0, sourceId, net.secondDeviceId))).toBytes))
             Future.unit
      _ <- waitForTransactionsToComplete()
      result <- TransferStarter.start(client, store0, net.secondDeviceId)
    yield
      result should be(StartResult.Started)

class TransferStarterSuite extends TransferStarterSuiteBase:

  private class FakeClock(var t: Long):
    def now(): Long = t

  private val base = Duration(100, MILLISECONDS)
  private val max = Duration(1000, MILLISECONDS)

  test("TransferBackoff: an unknown store is ready"):
    val b = new TransferBackoff(() => 0L, base, max)
    b.ready(StoreId(poolId, 0)) should be(true)

  test("TransferBackoff: not ready after failed, ready after base"):
    val clock = FakeClock(1000L)
    val b = new TransferBackoff(() => clock.now(), base, max)
    b.failed(store0)
    b.ready(store0) should be(false)
    clock.t = 1099L
    b.ready(store0) should be(false)
    clock.t = 1100L
    b.ready(store0) should be(true)
    // Other stores are unaffected.
    b.ready(store1) should be(true)

  test("TransferBackoff: the delay doubles per failure up to max"):
    val clock = FakeClock(0L)
    val b = new TransferBackoff(() => clock.now(), base, max)
    val expected = List(100L, 200L, 400L, 800L, 1000L, 1000L)
    expected.foreach: delay =>
      val failedAt = clock.t
      b.failed(store0)
      clock.t = failedAt + delay - 1
      b.ready(store0) should be(false)
      clock.t = failedAt + delay
      b.ready(store0) should be(true)
    succeed

  test("TransferBackoff: jitter shortens the delay within its bound and never lengthens it"):
    val clock = FakeClock(0L)
    val low = new TransferBackoff(() => clock.now(), base, max, new scala.util.Random(1) { override def nextDouble() = 1.0 }, 0.5)
    low.failed(store0)
    clock.t = 49L
    low.ready(store0) should be(false)
    clock.t = 50L
    low.ready(store0) should be(true)

    val none = new TransferBackoff(() => clock.now(), base, max, new scala.util.Random(1) { override def nextDouble() = 0.0 }, 0.5)
    clock.t = 0L
    none.failed(store0)
    clock.t = 99L
    none.ready(store0) should be(false)
    clock.t = 100L
    none.ready(store0) should be(true)

  test("TransferBackoff: succeeded resets"):
    val clock = FakeClock(0L)
    val b = new TransferBackoff(() => clock.now(), base, max)
    b.failed(store0)
    b.failed(store0)
    b.failed(store0)
    b.succeeded(store0)
    b.ready(store0) should be(true)
    // The next failure starts again from base.
    clock.t = 10L
    b.failed(store0)
    clock.t = 109L
    b.ready(store0) should be(false)
    clock.t = 110L
    b.ready(store0) should be(true)

/** `writeThreshold == width`: the floor clamps to `width - 1`, so one transfer must start and stay
 *  started (verification uses the same clamped floor), while a racing second one is rolled back. */
class FullWriteThresholdTransferStarterSuite extends TransferStarterSuiteBase:
  override def testIda: IDA = Replication(3, 3)

  test("the pool's writeThreshold equals its width"):
    testIda.writeThreshold should be(testIda.width)
