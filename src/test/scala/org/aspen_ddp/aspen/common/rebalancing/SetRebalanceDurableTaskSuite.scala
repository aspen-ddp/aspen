package org.aspen_ddp.aspen.common.rebalancing

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.common.{DataBuffer, Radicle}
import org.aspen_ddp.aspen.common.ida.{IDA, Replication}
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceSetId, StorageDeviceSetState,
                                            StorageDeviceState, fixed_ids}
import org.aspen_ddp.aspen.common.metadata.management.FailedStorageDeviceTestHarness
import org.aspen_ddp.aspen.common.objects.Insert
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import org.aspen_ddp.aspen.compute.DurableTaskPointer

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.{Duration, MILLISECONDS}
import scala.util.Random

/** Drives SetRebalanceDurableTask directly over a hand-written `pendingTransfers` list, so each
 *  test controls exactly which transfers are planned and to where. */
abstract class SetRebalanceDurableTaskSuiteBase extends IntegrationTestSuite:

  protected val setId: StorageDeviceSetId = StorageDeviceSetId.BootstrapStorageDeviceSetId
  protected val boot: StorageDeviceId = StorageDeviceId.BootstrapStorageDeviceId
  protected val pollPeriod: Duration = Duration(50, MILLISECONDS)

  protected type Transfer = (StoreId, StorageDeviceId, StorageDeviceId)

  private var tasks: List[SetRebalanceDurableTask] = Nil

  // Runs on failure and timeout too, so a failed test cannot leave a task polling.
  override def subFixtureSetup(): Unit = tasks = Nil

  override def subFixtureTeardown(): Unit =
    tasks.foreach(_.stop())
    tasks = Nil

  protected def storeId(i: Int): StoreId = StoreId(Radicle.poolId, i.toByte)

  /** The task is constructed and driven directly, as RebalancingDurableService does. Its task
   *  pointer is only an identity, so any KV pointer serves. */
  protected def startTask(rng: Random = new Random()): SetRebalanceDurableTask =
    val task = new SetRebalanceDurableTask(DurableTaskPointer(Radicle.pointer), client, setId,
                                           pollPeriod, rng)
    tasks ::= task
    task

  protected def writePending(transfers: List[Transfer]): Future[Unit] =
    given ExecutionContext = executionContext
    client.getStorageDeviceSetPointer(setId).flatMap: setPtr =>
      client.transactUntilSuccessful: tx =>
        given Transaction = tx
        client.read(setPtr).map: dos =>
          val updated = StorageDeviceSetState(dos).copy(pendingTransfers = transfers)
          tx.overwrite(setPtr, dos.revision, DataBuffer(updated.toBytes))

  protected def pendingTransfers(): Future[List[Transfer]] =
    given ExecutionContext = executionContext
    client.getStorageDeviceSetState(setId).map(_.pendingTransfers)

  protected def statusOf(deviceId: StorageDeviceId,
                         sid: StoreId): Future[Option[StorageDeviceState.StoreStatus]] =
    given ExecutionContext = executionContext
    client.getStorageDeviceState(deviceId).map(_.stores.get(sid).map(_.status))

  protected def countTransferringOut(deviceId: StorageDeviceId): Future[Int] =
    given ExecutionContext = executionContext
    client.getStorageDeviceState(deviceId).map: ds =>
      ds.stores.values.count(_.status == StorageDeviceState.StoreStatus.TransferringOut)

  protected def sleep(d: Duration): Future[Unit] =
    val p = Promise[Unit]()
    client.backgroundTaskManager.schedule(d)(p.success(()))
    p.future

  /** Poll `cond` until it holds or `timeout` elapses. Fails the test on timeout. */
  protected def awaitUntil(what: String, timeout: Duration = Duration(60000, MILLISECONDS))
                          (cond: => Future[Boolean]): Future[Unit] =
    given ExecutionContext = executionContext
    val deadline = System.nanoTime() + timeout.toNanos
    def loop(): Future[Unit] =
      cond.flatMap: ok =>
        if ok then Future.unit
        else if System.nanoTime() > deadline then
          Future.failed(new AssertionError(s"timed out waiting for: $what"))
        else
          sleep(Duration(50, MILLISECONDS)).flatMap(_ => loop())
    loop()

  /** Complete in-flight transfers until the task finishes (pendingTransfers drained). */
  protected def driveToCompletion(task: SetRebalanceDurableTask): Future[Unit] =
    given ExecutionContext = executionContext
    awaitUntil("the task to drain pendingTransfers and complete"):
      net.completeInFlightTransfers(setId).map(_ => task.completed.isCompleted)
    .flatMap(_ => task.completed.map(_ => ()))

/** Default Replication(3, 2): floor 2, so one transfer of the pool may be in flight at a time. */
class SetRebalanceDurableTaskSuite extends SetRebalanceDurableTaskSuiteBase
                                     with FailedStorageDeviceTestHarness:

  /** Rewrite one device's state object in place. Metadata only: no Host reacts to it here. */
  private def updateDevice(deviceId: StorageDeviceId)
                          (f: StorageDeviceState => StorageDeviceState): Future[Unit] =
    given ExecutionContext = executionContext
    client.transactUntilSuccessful: tx =>
      given Transaction = tx
      for
        ptr <- client.getStorageDevicePointer(deviceId)
        kvos <- client.read(ptr)
      yield
        val updated = f(StorageDeviceState(kvos))
        tx.update(ptr, None, None,
          List(KeyRevision(StorageDeviceState.StateKey, kvos.contents(StorageDeviceState.StateKey).revision)),
          List(Insert(StorageDeviceState.StateKey, updated.encode())))

  /** Tombstone `deviceId` (which holds no stores) through the real failure path. */
  private def tombstone(deviceId: StorageDeviceId): Future[Unit] =
    given ExecutionContext = executionContext
    for
      _ <- client.failStorageDevice(deviceId)
      _ <- waitForTransactionsToComplete()
      task <- taskForEnrolled(deviceId)
      _ <- withTimeout(task.completed.map(_ => ()), Duration(60000, MILLISECONDS), "tombstone")
      state <- client.getStorageDeviceState(deviceId)
    yield
      state.isFailed should be(true)

  atest("a transfer aborted with abortStoreTransfer is started again and drains"):
    given ExecutionContext = executionContext
    val s0 = storeId(0)
    for
      _ <- net.createSecondDevice()
      _ <- writePending(List((s0, boot, net.secondDeviceId)))
      task = startTask()
      _ <- awaitUntil("the transfer to start")(
             statusOf(boot, s0).map(_.contains(StorageDeviceState.StoreStatus.TransferringOut)))
      _ <- client.abortStoreTransfer(s0)
      // Only a restarted transfer can be completed: completeInFlightTransfers finds nothing
      // while the store sits Active on its source.
      _ <- driveToCompletion(task)
      remaining <- pendingTransfers()
      dst <- statusOf(net.secondDeviceId, s0)
      src <- statusOf(boot, s0)
    yield
      remaining shouldBe empty
      dst should be(Some(StorageDeviceState.StoreStatus.Active))
      src should be(None)

  atest("a pending transfer to a tombstoned device gets a new destination and completes"):
    given ExecutionContext = executionContext
    val s0 = storeId(0)
    for
      _ <- net.createSecondDevice()
      _ <- net.createThirdDevice()
      _ <- tombstone(net.secondDeviceId)
      _ <- writePending(List((s0, boot, net.secondDeviceId)))
      task = startTask()
      // The only usable non-pool device left in the set is the third one.
      _ <- awaitUntil("the destination to be replaced")(
             pendingTransfers().map(_ == List((s0, boot, net.thirdDeviceId))))
      _ <- driveToCompletion(task)
      remaining <- pendingTransfers()
      dst <- statusOf(net.thirdDeviceId, s0)
    yield
      remaining shouldBe empty
      dst should be(Some(StorageDeviceState.StoreStatus.Active))

  atest("a replacement destination avoids another pending destination of the same pool"):
    given ExecutionContext = executionContext
    val s0 = storeId(0)
    val s1 = storeId(1)
    val fourthDeviceId = StorageDeviceId(new java.util.UUID(0, 102))
    // shuffle never reorders when nextInt(n) always returns n - 1, so selection tries the set's
    // members in list order: boot, second, third, fourth. Without s1's planned destination in
    // the exclude set the replacement for s0 would therefore land on third.
    val noShuffle = new Random(new java.util.Random:
      override def nextInt(bound: Int): Int = bound - 1)
    for
      _ <- net.createSecondDevice()
      _ <- net.createThirdDevice()
      _ <- net.createDevice(fourthDeviceId)
      _ <- tombstone(net.secondDeviceId)
      _ <- writePending(List((s0, boot, net.secondDeviceId), (s1, boot, net.thirdDeviceId)))
      task = startTask(noShuffle)
      _ <- awaitUntil("the destination to be replaced")(
             pendingTransfers().map(!_.contains((s0, boot, net.secondDeviceId))))
      remaining <- pendingTransfers()
    yield
      remaining should contain((s0, boot, fourthDeviceId))
      remaining should not contain((s0, boot, net.thirdDeviceId))

  atest("a pending transfer is kept, not dropped, when no new destination is selectable"):
    given ExecutionContext = executionContext
    val s0 = storeId(0)
    val planned = (s0, boot, net.secondDeviceId)
    for
      _ <- net.createSecondDevice()
      // With the second device gone the set holds only the source device, which selection can
      // return only as a soft-excluded fallback and which is not a legal destination.
      _ <- tombstone(net.secondDeviceId)
      _ <- writePending(List(planned))
      task = startTask()
      _ <- sleep(Duration(1000, MILLISECONDS))
      remaining <- pendingTransfers()
      src <- statusOf(boot, s0)
    yield
      remaining should be(List(planned))
      src should be(Some(StorageDeviceState.StoreStatus.Active))
      task.completed.isCompleted should be(false)

  atest("classify treats a source-death rebuild onto the planned destination as InFlight"):
    given ExecutionContext = executionContext
    val s0 = storeId(0)
    for
      _ <- net.createSecondDevice()
      _ <- net.createThirdDevice()
      // The state FailedStorageDeviceDurableTask leaves after rebuilding a store whose transfer
      // source died: the source is a tombstone that no longer lists the store, and the planned
      // destination holds a Rebuilding entry for it.
      _ <- updateDevice(net.secondDeviceId)(_.copy(hostId = fixed_ids.FailedHostId))
      _ <- updateDevice(net.thirdDeviceId)(
             _.setStoreEntry(s0, StorageDeviceState.StoreStatus.Rebuilding, None))
      phase <- SetRebalanceDurableTask.classify(client, (s0, net.secondDeviceId, net.thirdDeviceId))
    yield
      phase should be(SetRebalanceDurableTask.Phase.InFlight)

  // ---- Device death mid-transfer, through the real FailedStorageDeviceDurableTask ------

  /** Declare `deviceId` failed and run the real drain until its store map is empty. */
  private def failAndDrain(deviceId: StorageDeviceId): Future[Unit] =
    given ExecutionContext = executionContext
    for
      _ <- client.failStorageDevice(deviceId)
      _ <- waitForTransactionsToComplete()
      drain <- taskForEnrolled(deviceId)
      _ <- withTimeout(drain.completed.map(_ => ()), Duration(60000, MILLISECONDS), "the drain")
    yield ()

  /** The TransferringIn and TransferringOut entries on `deviceId`: the halves of transfers. */
  private def transferEntries(deviceId: StorageDeviceId): Future[Map[StoreId, StorageDeviceState.StoreEntry]] =
    given ExecutionContext = executionContext
    client.getStorageDeviceState(deviceId).map: ds =>
      ds.stores.filter: (_, e) =>
        e.status == StorageDeviceState.StoreStatus.TransferringIn ||
        e.status == StorageDeviceState.StoreStatus.TransferringOut

  atest("a destination dying mid-transfer restores the source and the transfer is retried elsewhere"):
    given ExecutionContext = executionContext
    val s0 = storeId(0)
    for
      _ <- net.createSecondDevice()
      _ <- net.createThirdDevice()
      _ <- writePending(List((s0, boot, net.secondDeviceId)))
      task = startTask()
      _ <- awaitUntil("the transfer to start")(
             statusOf(net.secondDeviceId, s0).map(_.contains(StorageDeviceState.StoreStatus.TransferringIn)))

      // The drain disowns the destination's TransferringIn entry and puts the source back.
      _ <- failAndDrain(net.secondDeviceId)
      srcAfterDrain <- client.getStorageDeviceState(boot)

      // The only usable non-pool device left in the set is the third one.
      _ <- awaitUntil("the destination to be replaced")(
             pendingTransfers().map(_ == List((s0, boot, net.thirdDeviceId))))
      _ <- driveToCompletion(task)
      remaining <- pendingTransfers()
      dst <- statusOf(net.thirdDeviceId, s0)
      src <- statusOf(boot, s0)
      dead <- client.getStorageDeviceState(net.secondDeviceId)
    yield
      // The task polls every 50 ms, so by the time this is read it may already have restarted
      // the transfer toward the third device. What must not survive is the half naming the
      // dead device.
      srcAfterDrain.stores(s0) should not be
        StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.TransferringOut, Some(net.secondDeviceId))
      remaining shouldBe empty
      dst should be(Some(StorageDeviceState.StoreStatus.Active))
      src should be(None)
      dead.stores.get(s0) should be(None)

  atest("a source dying mid-transfer is waited out while it rebuilds, then the task finishes"):
    given ExecutionContext = executionContext
    val s0 = storeId(0)
    val planned = (s0, net.secondDeviceId, net.thirdDeviceId)
    for
      _ <- net.createSecondDevice()
      _ <- net.createThirdDevice()
      // Put store 0 on the second device so that failing it costs one replica, not three.
      _ <- net.simulateTransferComplete(s0, boot, net.secondDeviceId)
      _ <- writePending(List(planned))
      task = startTask()
      _ <- awaitUntil("the transfer to start")(
             statusOf(net.thirdDeviceId, s0).map(_.contains(StorageDeviceState.StoreStatus.TransferringIn)))

      // The drain owns store 0 (TransferringOut, pool names the second device) and rebuilds it.
      // The set now holds the bootstrap device, soft-excluded as a pool device, and the third
      // device, so the rebuild lands on the planned destination.
      _ <- failAndDrain(net.secondDeviceId)
      rebuilding <- statusOf(net.thirdDeviceId, s0)

      // Many passes while the store rebuilds. Nothing may start, replace, or drop anything.
      _ <- sleep(Duration(1000, MILLISECONDS))
      duringRebuild <- pendingTransfers()
      bootTransfers <- transferEntries(boot)
      thirdTransfers <- transferEntries(net.thirdDeviceId)
      stillRebuilding <- statusOf(net.thirdDeviceId, s0)
      doneEarly = task.completed.isCompleted

      // The destination host finishes the rebuild, as Host.markRebuiltStoreActive does.
      _ <- updateDevice(net.thirdDeviceId)(_.setStoreEntry(s0, StorageDeviceState.StoreStatus.Active, None))
      _ <- driveToCompletion(task)
      remaining <- pendingTransfers()
      dst <- statusOf(net.thirdDeviceId, s0)
    yield
      rebuilding should be(Some(StorageDeviceState.StoreStatus.Rebuilding))
      duringRebuild should be(List(planned))
      bootTransfers shouldBe empty
      thirdTransfers shouldBe empty
      stillRebuilding should be(Some(StorageDeviceState.StoreStatus.Rebuilding))
      doneEarly should be(false)
      remaining shouldBe empty
      dst should be(Some(StorageDeviceState.StoreStatus.Active))

/** Replication(10, 6): floor min(6 + 2, 10 - 1) = 8, so two of the ten stores may be in flight at
 *  once. (The harness needs writeThreshold > width / 2, which rules out narrower pools that admit
 *  two transfers.) */
class ParallelSetRebalanceDurableTaskSuite extends SetRebalanceDurableTaskSuiteBase:
  override def testIda: IDA = Replication(10, 6)

  atest("several admissible transfers are in flight at once, bounded by the floor"):
    given ExecutionContext = executionContext
    for
      _ <- net.createSecondDevice()
      _ <- net.createThirdDevice()
      _ <- writePending(List(
             (storeId(0), boot, net.secondDeviceId),
             (storeId(1), boot, net.thirdDeviceId),
             (storeId(2), boot, net.secondDeviceId)))
      task = startTask()
      _ <- awaitUntil("two transfers to be in flight together")(countTransferringOut(boot).map(_ >= 2))
      // Several more passes: the third must still be held back by the floor.
      _ <- sleep(Duration(500, MILLISECONDS))
      inFlight <- countTransferringOut(boot)
      _ <- driveToCompletion(task)
      remaining <- pendingTransfers()
    yield
      inFlight should be(2)
      remaining shouldBe empty
