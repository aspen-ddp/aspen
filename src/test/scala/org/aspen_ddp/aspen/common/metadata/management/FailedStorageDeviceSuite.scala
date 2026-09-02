package org.aspen_ddp.aspen.common.metadata.management

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.{Radicle, TypeFactories}
import org.aspen_ddp.aspen.common.metadata.{BootstrapConfig, HostId, StorageDeviceId, StorageDeviceSetId, StorageDeviceState, StoragePoolState, fixed_ids}
import org.aspen_ddp.aspen.common.objects.{Insert, Key, KeyValueObjectPointer, ObjectRevision}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import org.aspen_ddp.aspen.compute.{DurableTaskFactory, DurableTaskPointer, ServiceEntry}
import org.aspen_ddp.aspen.compute.systemtask.{SystemTaskExecutorService, SystemTaskServiceState}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.{Duration, MILLISECONDS}

class FailedStorageDeviceSuite extends IntegrationTestSuite:

  atest("FailedStorageDeviceDurableTask is registered in the client type registry"):
    given ExecutionContext = executionContext
    val factory = client.typeRegistry.getType[DurableTaskFactory](
      FailedStorageDeviceDurableTask.typeUUID)
    factory should be(Some(FailedStorageDeviceDurableTask))
    TypeFactories.factories should contain(FailedStorageDeviceDurableTask)

  /** The task state objects currently enrolled with the system task executor. */
  protected def enrolledTasks(): Future[List[(java.util.UUID, KeyValueObjectPointer)]] =
    given ExecutionContext = executionContext
    val tkvl = TieredKeyValueList(client,
      KVObjectRootManager(client, Radicle.ServicesTreeKey, Radicle.pointer))
    for
      vs <- tkvl.get(Key(SystemTaskExecutorService.ServiceUUID))
      statePtr = ServiceEntry.decode(vs.get.value.bytes).statePointer
      enrolled <- SystemTaskServiceState.scan(client, statePtr)
    yield enrolled

  atest("failStorageDevice enrolls exactly one task"):
    given ExecutionContext = executionContext
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.failStorageDevice(net.secondDeviceId)
      _ <- waitForTransactionsToComplete()
      enrolled <- enrolledTasks()
    yield
      enrolled.size should be(1)

  atest("failStorageDevice fails with NoSuchElementException for an unknown device"):
    given ExecutionContext = executionContext
    recoverToSucceededIf[NoSuchElementException](
      client.failStorageDevice(StorageDeviceId(java.util.UUID.randomUUID())))

  // ---- Driving the task -----------------------------------------------------------

  private val fastPoll = Duration(50, MILLISECONDS)

  /** Build a task instance over the state object failStorageDevice enrolled. */
  protected def taskForEnrolled(deviceId: StorageDeviceId): Future[FailedStorageDeviceDurableTask] =
    taskForEnrolled(deviceId, fastPoll)

  protected def taskForEnrolled(deviceId: StorageDeviceId,
                                pollPeriod: Duration): Future[FailedStorageDeviceDurableTask] =
    given ExecutionContext = executionContext
    enrolledTasks().map: enrolled =>
      enrolled.size should be(1)
      new FailedStorageDeviceDurableTask(
        DurableTaskPointer(enrolled.head._2), client, deviceId, pollPeriod)

  /** Poll until `deviceId` reads as tombstoned, or fail on timeout. A bare wait on the task's
   *  completion would hang the suite rather than fail if the loop stopped making progress. */
  protected def awaitTombstone(deviceId: StorageDeviceId, timeout: Duration): Future[Unit] =
    given ExecutionContext = executionContext
    val deadline = System.nanoTime() + timeout.toNanos
    def loop(): Future[Unit] =
      client.getStorageDeviceState(deviceId).flatMap: state =>
        if state.isFailed then
          Future.unit
        else if System.nanoTime() > deadline then
          Future.failed(new AssertionError(s"device ${deviceId.uuid} was not tombstoned"))
        else
          val p = Promise[Unit]()
          client.backgroundTaskManager.schedule(fastPoll)(p.success(()))
          p.future.flatMap(_ => loop())
    loop()

  /** Race a future against a timeout, failing with an explicit assertion if the timeout wins. */
  protected def withTimeout[T](fut: Future[T], timeout: Duration, what: String): Future[T] =
    given ExecutionContext = executionContext
    val p = Promise[T]()
    client.backgroundTaskManager.schedule(timeout):
      if !p.isCompleted then
        p.failure(new AssertionError(s"$what did not complete within ${timeout.toMillis} ms"))
    fut.onComplete(p.tryComplete)
    p.future

  /** Wait without occupying a pool thread. Thread.sleep here would hold a thread the task under
   *  test needs to make the very progress the test is watching for. */
  protected def delay(d: Duration): Future[Unit] =
    val p = Promise[Unit]()
    client.backgroundTaskManager.schedule(d)(p.success(()))
    p.future

  atest("step 1 zeroes both ids and removes the device from its set and host"):
    given ExecutionContext = executionContext
    val setId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.failStorageDevice(net.secondDeviceId)
      _ <- waitForTransactionsToComplete()

      _ <- taskForEnrolled(net.secondDeviceId)
      _ <- awaitTombstone(net.secondDeviceId, Duration(30000, MILLISECONDS))

      state <- client.getStorageDeviceState(net.secondDeviceId)
      set <- client.getStorageDeviceSetState(setId)
      host <- client.getHostState(HostId.BootstrapHostId)
    yield
      state.hostId should be(fixed_ids.FailedHostId)
      state.storageDeviceId should be(fixed_ids.FailedStorageDeviceId)
      state.isFailed should be(true)
      set.memberDevices should not contain net.secondDeviceId
      host.storageDevices should not contain net.secondDeviceId

  atest("step 1 removes the device from the set it is in now, not the one it was in at enrollment"):
    given ExecutionContext = executionContext
    val bootstrapSet = StorageDeviceSetId.BootstrapStorageDeviceSetId
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.failStorageDevice(net.secondDeviceId)
      _ <- waitForTransactionsToComplete()

      // An operator moves the device between enrollment and the first pass. That window is a
      // whole poll period wide, and arbitrarily wide across a restart.
      otherSet <- client.createStorageDeviceSet("other", 0, None)
      _ <- client.moveDeviceToSet(net.secondDeviceId, otherSet)
      _ <- waitForTransactionsToComplete()

      _ <- taskForEnrolled(net.secondDeviceId)
      _ <- awaitTombstone(net.secondDeviceId, Duration(30000, MILLISECONDS))
      _ <- waitForTransactionsToComplete()

      other <- client.getStorageDeviceSetState(otherSet)
      bootstrap <- client.getStorageDeviceSetState(bootstrapSet)
    yield
      // A set id captured at enrollment would filter the device out of the set it had already
      // left and leave it in the new one with both ids zeroed, permanently.
      other.memberDevices should not contain net.secondDeviceId
      bootstrap.memberDevices should not contain net.secondDeviceId

  atest("step 1 against an already-tombstoned device changes nothing"):
    given ExecutionContext = executionContext
    val setId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    val timeout = Duration(30000, MILLISECONDS)
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.failStorageDevice(net.secondDeviceId)
      _ <- waitForTransactionsToComplete()

      _ <- taskForEnrolled(net.secondDeviceId)
      _ <- awaitTombstone(net.secondDeviceId, timeout)
      _ <- waitForTransactionsToComplete()
      before <- client.getStorageDeviceState(net.secondDeviceId)

      // Re-running tombstone() against already-tombstoned state must complete and change nothing.
      task <- taskForEnrolled(net.secondDeviceId)
      completed <- withTimeout(task.tombstone(), timeout, "tombstone() on already-tombstoned device")
      _ <- waitForTransactionsToComplete()
      after <- client.getStorageDeviceState(net.secondDeviceId)
      set <- client.getStorageDeviceSetState(setId)
    yield
      after should be(before)
      set.memberDevices should not contain net.secondDeviceId

  atest("failStorageDevice refuses a device that is already tombstoned"):
    given ExecutionContext = executionContext
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.failStorageDevice(net.secondDeviceId)
      _ <- waitForTransactionsToComplete()

      _ <- taskForEnrolled(net.secondDeviceId)
      _ <- awaitTombstone(net.secondDeviceId, Duration(30000, MILLISECONDS))
      _ <- waitForTransactionsToComplete()

      // A second enrollment attempt sees the tombstoned device and rejects it.
      result <- recoverToExceptionIf[AspenClient.DeviceAlreadyFailed](
        client.failStorageDevice(net.secondDeviceId))
    yield
      result.getMessage should include("already been declared failed")

  atest("a failing pass does not wedge the task"):
    given ExecutionContext = executionContext
    val bogusId = StorageDeviceId(java.util.UUID.randomUUID())
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.failStorageDevice(net.secondDeviceId)
      _ <- waitForTransactionsToComplete()

      // Build a task instance over the enrolled state but with a bogus device id, so
      // drive()'s getStorageDeviceState fails.
      task <- taskForEnrolled(bogusId)
      _ <- waitForTransactionsToComplete()
      before = task.testDriveCount

      // Wait several poll periods. The task should survive: neither completed nor failed,
      // still rescheduling.
      _ <- delay(fastPoll * 5)
    yield
      task.completed.isCompleted should be(false)
      // The completion assertion alone is theatre: with a bogus device id that promise can only
      // be completed by stop() or an empty store map, neither of which can happen here, so it
      // holds identically whether the task is polling healthily or wedged solid. The pass count
      // is what tells the two apart -- a wedged single-flight flag never re-enters drive().
      (task.testDriveCount - before) should be >= 2

  /** The revision of the radicle's bootstrap-config key.
   *
   *  The generated YAML names hosts and stores but never devices, so a move between two devices
   *  on the same host rewrites it to byte-identical text. The revision is the only evidence the
   *  regeneration happened at all.
   */
  private def bootstrapConfigRevision(): Future[ObjectRevision] =
    given ExecutionContext = executionContext
    client.read(client.radicle).map(_.contents(Radicle.BootstrapConfigKey).revision)

  /** The revision of the bootstrap pool's config key. */
  private def bootstrapPoolConfigRevision(): Future[ObjectRevision] =
    given ExecutionContext = executionContext
    for
      poolPtr <- client.getStoragePoolPointer(PoolId.BootstrapPoolId)
      poolKvos <- client.read(poolPtr)
    yield poolKvos.contents(StoragePoolState.ConfigKey).revision

  /** Poll until the tombstone holds at most `atMost` stores, or fail on timeout. */
  private def awaitStoresRemaining(deviceId: StorageDeviceId,
                                   atMost: Int,
                                   timeout: Duration): Future[Unit] =
    given ExecutionContext = executionContext
    val deadline = System.nanoTime() + timeout.toNanos
    def loop(): Future[Unit] =
      client.getStorageDeviceState(deviceId).flatMap: state =>
        if state.stores.size <= atMost then
          Future.unit
        else if System.nanoTime() > deadline then
          Future.failed(new AssertionError(
            s"device ${deviceId.uuid} still holds ${state.stores.size} stores"))
        else
          val p = Promise[Unit]()
          client.backgroundTaskManager.schedule(fastPoll)(p.success(()))
          p.future.flatMap(_ => loop())
    loop()

  atest("the drain moves every store off the tombstone and completes"):
    given ExecutionContext = executionContext
    val failedId = StorageDeviceId.BootstrapStorageDeviceId
    val result = for
      // A live destination in the same set. The bootstrap device carries all three stores.
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()

      _ <- client.failStorageDevice(failedId)
      _ <- waitForTransactionsToComplete()

      task <- taskForEnrolled(failedId)
      _ <- withTimeout(task.completed.map(_ => ()), Duration(30000, MILLISECONDS),
                       "task completion")
      _ <- waitForTransactionsToComplete()

      tombstone <- client.getStorageDeviceState(failedId)
      destination <- client.getStorageDeviceState(net.secondDeviceId)
      poolState <- client.getStoragePoolState(PoolId.BootstrapPoolId)
    yield (tombstone, destination, poolState)

    result.map: (tombstone, destination, poolState) =>
      tombstone.isFailed should be(true)
      tombstone.stores shouldBe empty

      destination.stores.size should be(3)
      val dstStatuses = destination.stores.values.map(_.status).toSet
      dstStatuses should be(Set(StorageDeviceState.StoreStatus.Rebuilding))
      val dstTransfers = destination.stores.values.map(_.transferDevice).toSet
      dstTransfers should be(Set(None))

      // The pool is repointed at the start of the rebuild, not at its end.
      poolState.stores.size should be(3)
      val poolDevices = poolState.stores.map(_.storageDeviceId).toSet
      poolDevices should be(Set(net.secondDeviceId))

  atest("the drain regenerates the bootstrap config for a bootstrap-pool store"):
    given ExecutionContext = executionContext
    val failedId = StorageDeviceId.BootstrapStorageDeviceId
    val result = for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()

      _ <- client.failStorageDevice(failedId)
      _ <- waitForTransactionsToComplete()
      // Snapshot once enrollment has settled: from here on the drain is the only writer of the
      // bootstrap-config key, so a revision change can only have come from it.
      beforeRev <- bootstrapConfigRevision()

      task <- taskForEnrolled(failedId)
      _ <- withTimeout(task.completed.map(_ => ()), Duration(30000, MILLISECONDS),
                       "task completion")
      _ <- waitForTransactionsToComplete()

      afterRev <- bootstrapConfigRevision()
      poolRev <- bootstrapPoolConfigRevision()
      after <- client.getBootstrapConfig()
    yield (beforeRev, afterRev, poolRev, after)

    result.map: (beforeRev, afterRev, poolRev, after) =>
      // Both devices are on the bootstrap host and the YAML carries no device ids, so the
      // regenerated text is byte-identical and comparing it proves nothing. What matters is
      // that the write happened: a bootstrap-pool store changing device without it leaves every
      // host bootstrapping against stale state. The revision is the only witness to that.
      afterRev should not be beforeRev
      // The drain stages the radicle update and the pool repoint into the same transaction, so
      // the final pass's radicle BootstrapConfigKey revision must equal the pool's ConfigKey
      // revision. This proves same-transaction atomicity in one line, and unlike an inequality
      // cannot be satisfied by an unrelated writer.
      afterRev should be(poolRev)
      val cfg = BootstrapConfig.parseBootstrapConfig(after)
      cfg.hosts.map(_.stores.length).sum should be(3)

  /** Repoint one pool store entry at `deviceId`, as a completed transfer or a rebalance would. */
  private def repointPoolStore(storeId: StoreId, deviceId: StorageDeviceId): Future[Unit] =
    given ExecutionContext = executionContext
    client.transactUntilSuccessful: tx =>
      for
        devState <- client.getStorageDeviceState(deviceId)
        poolPtr <- client.getStoragePoolPointer(storeId.poolId)
        poolKvos <- client.read(poolPtr)
      yield
        val poolCfg = StoragePoolState(poolKvos)
        poolCfg.stores(storeId.poolIndex) =
          StoragePoolState.StoreEntry(devState.hostId, deviceId)
        tx.update(poolPtr, None, None,
          List(KeyRevision(StoragePoolState.ConfigKey,
            poolKvos.contents(StoragePoolState.ConfigKey).revision)),
          List(Insert(StoragePoolState.ConfigKey, poolCfg.encode())))

  atest("the drain disowns a TransferringIn entry rather than repointing the pool"):
    given ExecutionContext = executionContext
    val liveId = StorageDeviceId.BootstrapStorageDeviceId
    val storeId = StoreId(PoolId.BootstrapPoolId, 1)
    val result = for
      // A destination for the transfer, and a third device so the drain has somewhere to place
      // the store if it wrongly decides to rebuild it. Without the third device the pass would
      // fail on AllocationError and the test would pass for the wrong reason.
      _ <- net.createSecondDevice()
      _ <- net.createThirdDevice()
      _ <- waitForTransactionsToComplete()

      // An operator starts a transfer of store 1 onto the second device. transferStore marks the
      // source TransferringOut and the destination TransferringIn, and deliberately leaves the
      // pool naming the source: the source owns the store until the transfer completes.
      _ <- client.transferStore(storeId, net.secondDeviceId)
      _ <- waitForTransactionsToComplete()

      // The transfer stalls, so the operator fails the destination. Its store map now holds a
      // store it has no data for and does not own.
      _ <- client.failStorageDevice(net.secondDeviceId)
      _ <- waitForTransactionsToComplete()

      task <- taskForEnrolled(net.secondDeviceId)
      _ <- withTimeout(task.completed.map(_ => ()), Duration(30000, MILLISECONDS),
                       "task completion")
      _ <- waitForTransactionsToComplete()

      tombstone <- client.getStorageDeviceState(net.secondDeviceId)
      live <- client.getStorageDeviceState(liveId)
      third <- client.getStorageDeviceState(net.thirdDeviceId)
      poolState <- client.getStoragePoolState(PoolId.BootstrapPoolId)
    yield (tombstone, live, third, poolState)

    result.map: (tombstone, live, third, poolState) =>
      // The entry is dropped, not filtered: the store map is also the completion condition, so
      // an entry left in place would strand the task.
      tombstone.stores.get(storeId) should be(None)

      // Nothing else moves. The pool still names the device that actually holds the slice, and
      // that device's transfer entry is untouched.
      poolState.stores(storeId.poolIndex).storageDeviceId should be(liveId)
      live.stores(storeId).status should be(StorageDeviceState.StoreStatus.TransferringOut)
      live.stores(storeId).transferDevice should be(Some(net.secondDeviceId))
      third.stores.get(storeId) should be(None)

  atest("the drain disowns a store the pool no longer names"):
    given ExecutionContext = executionContext
    val failedId = StorageDeviceId.BootstrapStorageDeviceId
    val storeId = StoreId(PoolId.BootstrapPoolId, 0)
    val result = for
      _ <- net.createSecondDevice()
      _ <- net.createThirdDevice()
      _ <- waitForTransactionsToComplete()

      _ <- client.failStorageDevice(failedId)
      _ <- waitForTransactionsToComplete()

      // Something else repointed store 0 at the third device while its entry stayed on the
      // failed device. Whatever did it, the failed device no longer owns store 0, and rebuilding
      // it would route the pool away from the copy that exists.
      _ <- repointPoolStore(storeId, net.thirdDeviceId)
      _ <- waitForTransactionsToComplete()

      task <- taskForEnrolled(failedId)
      _ <- withTimeout(task.completed.map(_ => ()), Duration(30000, MILLISECONDS),
                       "task completion")
      _ <- waitForTransactionsToComplete()

      tombstone <- client.getStorageDeviceState(failedId)
      third <- client.getStorageDeviceState(net.thirdDeviceId)
      poolState <- client.getStoragePoolState(PoolId.BootstrapPoolId)
    yield (tombstone, third, poolState)

    result.map: (tombstone, third, poolState) =>
      // Store 0 is dropped and the other two drain normally, so the task still completes.
      tombstone.stores shouldBe empty
      poolState.stores(storeId.poolIndex).storageDeviceId should be(net.thirdDeviceId)
      third.stores.get(storeId) should be(None)

  atest("a resumed task picks up mid-drain"):
    given ExecutionContext = executionContext
    val failedId = StorageDeviceId.BootstrapStorageDeviceId
    val slowerPoll = Duration(200, MILLISECONDS)
    val result = for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.failStorageDevice(failedId)
      _ <- waitForTransactionsToComplete()

      // Stop the first instance once it has moved at least one store, simulating a crash. Give
      // it a slower poll period than the watcher to widen the observation windows.
      first <- taskForEnrolled(failedId, slowerPoll)
      _ <- awaitStoresRemaining(failedId, atMost = 2, Duration(30000, MILLISECONDS))
      _ = first.stop()
      midDrain <- client.getStorageDeviceState(failedId)

      // A fresh instance over the same enrolled state finishes the job.
      second <- taskForEnrolled(failedId)
      _ <- withTimeout(second.completed.map(_ => ()), Duration(30000, MILLISECONDS),
                       "task completion")
      _ <- waitForTransactionsToComplete()

      tombstone <- client.getStorageDeviceState(failedId)
      destination <- client.getStorageDeviceState(net.secondDeviceId)
    yield (midDrain, tombstone, destination)

    result.map: (midDrain, tombstone, destination) =>
      // If the watcher stalls past the 2-store and 1-store windows, the first instance finishes
      // the entire drain, stop() is a no-op, and the second instance reads an empty tombstone
      // and finishes immediately -- degenerating into a re-run of "moves every store". The
      // mid-drain snapshot must be strictly between 0 and 3 to prove the test actually caught a
      // resume.
      midDrain.stores.size should be > 0
      midDrain.stores.size should be < 3
      tombstone.stores shouldBe empty
      destination.stores.size should be(3)
