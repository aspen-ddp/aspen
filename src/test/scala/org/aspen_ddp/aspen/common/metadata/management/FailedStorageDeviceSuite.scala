package org.aspen_ddp.aspen.common.metadata.management

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.{Radicle, TypeFactories}
import org.aspen_ddp.aspen.common.metadata.{BootstrapConfig, HostId, StorageDeviceId, StorageDeviceSetId, StorageDeviceState, StoragePoolState, fixed_ids}
import org.aspen_ddp.aspen.common.network.CheckStorageDevice
import org.aspen_ddp.aspen.common.objects.{Insert, Key, ObjectRevision}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import org.aspen_ddp.aspen.compute.DurableTaskFactory

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.{Duration, MILLISECONDS}

class FailedStorageDeviceSuite extends IntegrationTestSuite
                                 with FailedStorageDeviceTestHarness:

  atest("FailedStorageDeviceDurableTask is registered in the client type registry"):
    given ExecutionContext = executionContext
    val factory = client.typeRegistry.getType[DurableTaskFactory](
      FailedStorageDeviceDurableTask.typeUUID)
    factory should be(Some(FailedStorageDeviceDurableTask))
    TypeFactories.factories should contain(FailedStorageDeviceDurableTask)


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

      // Everything captured from here on belongs to the drain.
      _ = net.takeCapturedHostMessages()

      task <- taskForEnrolled(net.secondDeviceId)
      _ <- withTimeout(task.completed.map(_ => ()), Duration(30000, MILLISECONDS),
                       "task completion")
      _ <- waitForTransactionsToComplete()

      nudges = net.takeCapturedHostMessages().collect:
        case m: CheckStorageDevice => m

      tombstone <- client.getStorageDeviceState(net.secondDeviceId)
      live <- client.getStorageDeviceState(liveId)
      third <- client.getStorageDeviceState(net.thirdDeviceId)
      poolState <- client.getStoragePoolState(PoolId.BootstrapPoolId)
    yield (tombstone, live, third, poolState, nudges)

    result.map: (tombstone, live, third, poolState, nudges) =>
      // The entry is dropped, not filtered: the store map is also the completion condition, so
      // an entry left in place would strand the task.
      tombstone.stores.get(storeId) should be(None)

      // The pool still names the device that actually holds the slice.
      poolState.stores(storeId.poolIndex).storageDeviceId should be(liveId)
      third.stores.get(storeId) should be(None)

      // And that device's half of the transfer is put back rather than left in place. The
      // destination is dead, so nothing will ever send it a StartStoreTransfer or complete the
      // transfer; a TransferringOut entry left here would never be cleared by anything, and it
      // holds MigratePoolToSetDurableTask's inFlight and SetRebalanceDurableTask's Phase.InFlight
      // true forever.
      live.stores(storeId).status should be(StorageDeviceState.StoreStatus.Active)
      live.stores(storeId).transferDevice should be(None)

      // Restoring the entry is only half of it: the source's local copy is still offline behind
      // its transfer-out marker, and the pool names that device, so the slice answers nothing
      // until the source host runs its own poll. Without this nudge that is up to
      // Main.CheckStorageDevicesPeriod -- an hour -- of unavailability for a repair that has
      // already been decided.
      nudges should contain(
        CheckStorageDevice(HostId.BootstrapHostId, client.clientId, liveId))

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

  atest("an unplaceable disowned entry does not block the stores behind it"):
    given ExecutionContext = executionContext
    val liveId = StorageDeviceId.BootstrapStorageDeviceId
    val stalled = StoreId(PoolId.BootstrapPoolId, 1)
    val behind = StoreId(PoolId.BootstrapPoolId, 2)
    val result = for
      // Deliberately two devices only. That is what makes the disowned entry unplaceable: the
      // pool names the bootstrap device for store 1, selectDeviceForRebuild hard-excludes it as
      // the holder of the live data, and step 1 has already removed the failed device from the
      // set -- so there is no candidate at all and selection fails with AllocationError.
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()

      // Store 1: a transfer onto the second device that never completes. The second device gets
      // a TransferringIn entry it does not own; the pool still names the bootstrap device.
      _ <- client.transferStore(stalled, net.secondDeviceId)
      _ <- waitForTransactionsToComplete()

      // Store 2: a transfer onto the second device that did complete, so the second device
      // genuinely owns it and the pool names it. Ordered after store 1 by nextStore's sort.
      _ <- net.simulateTransferComplete(behind, liveId, net.secondDeviceId)
      _ <- waitForTransactionsToComplete()

      _ <- client.failStorageDevice(net.secondDeviceId)
      _ <- waitForTransactionsToComplete()

      task <- taskForEnrolled(net.secondDeviceId)
      _ <- withTimeout(task.completed.map(_ => ()), Duration(30000, MILLISECONDS),
                       "task completion")
      _ <- waitForTransactionsToComplete()

      tombstone <- client.getStorageDeviceState(net.secondDeviceId)
      live <- client.getStorageDeviceState(liveId)
      poolState <- client.getStoragePoolState(PoolId.BootstrapPoolId)
    yield (tombstone, live, poolState)

    result.map: (tombstone, live, poolState) =>
      // Deciding ownership before spending a selection is what makes this pass. With the check
      // only inside moveStore, store 1 has to be placed before it can be disowned, selection
      // fails every pass, and because nextStore returns the head of a sorted list store 1 is
      // chosen forever and store 2 is never reached.
      tombstone.stores shouldBe empty

      // Store 1 is dropped, not rebuilt: the pool still names the device holding the slice, and
      // that device's abandoned transfer entry is put back to Active.
      poolState.stores(stalled.poolIndex).storageDeviceId should be(liveId)
      live.stores(stalled).status should be(StorageDeviceState.StoreStatus.Active)
      live.stores(stalled).transferDevice should be(None)

      // Store 2 -- the one behind the blockage -- is rebuilt onto the only live device.
      poolState.stores(behind.poolIndex).storageDeviceId should be(liveId)
      live.stores(behind).status should be(StorageDeviceState.StoreStatus.Rebuilding)

  /** Zero both of `deviceId`'s ids, exactly as the task's step 1 does, without running the task.
   *
   *  These tests need a tombstone in place while the drain is NOT running, so that the other
   *  half of the transfer machinery can be exercised against it in isolation.
   */
  private def tombstoneDevice(deviceId: StorageDeviceId): Future[Unit] =
    given ExecutionContext = executionContext
    client.transactUntilSuccessful: tx =>
      for
        ptr <- client.getStorageDevicePointer(deviceId)
        kvos <- client.read(ptr)
      yield
        val tombstoned = StorageDeviceState(kvos).copy(
          hostId = fixed_ids.FailedHostId,
          storageDeviceId = fixed_ids.FailedStorageDeviceId)
        tx.update(ptr, None, None,
          List(KeyRevision(StorageDeviceState.StateKey,
            kvos.contents(StorageDeviceState.StateKey).revision)),
          List(Insert(StorageDeviceState.StateKey, tombstoned.encode())))

  atest("a transfer completing onto a tombstoned destination restores the source"):
    given ExecutionContext = executionContext
    val sourceId = StorageDeviceId.BootstrapStorageDeviceId
    val storeId = StoreId(PoolId.BootstrapPoolId, 1)
    val result = for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()

      // Source goes TransferringOut, destination TransferringIn, pool still names the source.
      _ <- client.transferStore(storeId, net.secondDeviceId)
      _ <- waitForTransactionsToComplete()

      // The operator declares the destination dead while the bytes are still moving. The drain
      // has not reached the entry yet -- and in the real trace it never will, because it disowns
      // that entry on its very first pass, long before a real transfer finishes.
      _ <- tombstoneDevice(net.secondDeviceId)
      _ <- waitForTransactionsToComplete()

      // The transfer completes anyway: the source is alive and sending, and the destination's
      // StoreTransferIn was created before the tombstone. withTimeout because the pre-fix
      // failure mode is not a wrong answer but a never-completing future -- prepRadicleUpdate
      // looks up HostId(0,0), which is absent from the host tree, and the bare retry loop
      // re-runs it every 60 s forever.
      _ <- withTimeout(net.smgr.testingOnlyUpdateStateForTransferredStore(
                         storeId, sourceId, net.secondDeviceId),
                       Duration(30000, MILLISECONDS),
                       "post-transfer update against a tombstoned destination")
      _ <- waitForTransactionsToComplete()

      poolState <- client.getStoragePoolState(PoolId.BootstrapPoolId)
      source <- client.getStorageDeviceState(sourceId)
      tombstone <- client.getStorageDeviceState(net.secondDeviceId)
    yield (poolState, source, tombstone)

    result.map: (poolState, source, tombstone) =>
      // Repointing the pool here loses the slice outright: reconcileDeviceState ignores a failed
      // device, so nothing on the destination ever serves the store, and the drain has already
      // disowned the entry rather than rebuilding it. The pool entry would look perfectly
      // healthy while naming a device that answers nothing.
      poolState.stores(storeId.poolIndex).storageDeviceId should be(sourceId)
      poolState.stores(storeId.poolIndex).hostId should be(HostId.BootstrapHostId)

      // The source keeps the store and goes back to Active: it never gave up its copy.
      source.stores(storeId).status should be(StorageDeviceState.StoreStatus.Active)
      source.stores(storeId).transferDevice should be(None)

      // The tombstone's store map is the drain's work list, not ours to edit.
      tombstone.stores(storeId).status should be(StorageDeviceState.StoreStatus.TransferringIn)

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
