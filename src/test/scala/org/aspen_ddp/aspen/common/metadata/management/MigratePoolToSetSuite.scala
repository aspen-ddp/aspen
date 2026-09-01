package org.aspen_ddp.aspen.common.metadata.management

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.TypeFactories
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceSetId, StorageDeviceSetState, StoragePoolState}
import org.aspen_ddp.aspen.common.objects.{Key, KeyValueObjectPointer}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.compute.DurableTaskFactory
import org.aspen_ddp.aspen.compute.DurableTaskPointer
import org.aspen_ddp.aspen.compute.ServiceEntry
import org.aspen_ddp.aspen.compute.systemtask.{SystemTaskExecutorService, SystemTaskServiceState}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.{Duration, MILLISECONDS}

class MigratePoolToSetSuite extends IntegrationTestSuite:

  atest("MigratePoolToSetDurableTask is registered in the client type registry"):
    given ExecutionContext = executionContext
    // SystemTaskExecutorService reconstructs a task by looking its type UUID up here; an
    // unregistered factory makes prepareSystemDurableTask StopRetrying.
    val factory = client.typeRegistry.getType[DurableTaskFactory](
      MigratePoolToSetDurableTask.typeUUID)
    factory should be(Some(MigratePoolToSetDurableTask))
    TypeFactories.factories should contain(MigratePoolToSetDurableTask)

  /** The task state objects currently enrolled with the system task executor. */
  private def enrolledTasks(): Future[List[(java.util.UUID, KeyValueObjectPointer)]] =
    given ExecutionContext = executionContext
    val tkvl = TieredKeyValueList(client,
      KVObjectRootManager(client, Radicle.ServicesTreeKey, Radicle.pointer))
    for
      vs <- tkvl.get(Key(SystemTaskExecutorService.ServiceUUID))
      statePtr = ServiceEntry.decode(vs.get.value.bytes).statePointer
      enrolled <- SystemTaskServiceState.scan(client, statePtr)
    yield enrolled

  atest("migratePoolToSet flips the pointer, swaps assignedPools, and enrolls one task"):
    given ExecutionContext = executionContext
    val poolId = PoolId.BootstrapPoolId
    val oldSetId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    for
      targetSetId <- client.createStorageDeviceSet("migration-target", level = 0, parent = None)
      _ <- waitForTransactionsToComplete()

      _ <- client.migratePoolToSet(poolId, targetSetId)
      _ <- waitForTransactionsToComplete()

      poolState <- client.getStoragePoolState(poolId)
      target <- client.getStorageDeviceSetState(targetSetId)
      old <- client.getStorageDeviceSetState(oldSetId)
      enrolled <- enrolledTasks()
    yield
      poolState.storageDeviceSet should be(targetSetId)
      poolState.migration should be(Some(StoragePoolState.Migration(
        targetSetId, StoragePoolState.MigrationStatus.InProgress)))
      target.assignedPools should contain(poolId)
      old.assignedPools should not contain poolId
      enrolled.size should be(1)

  atest("migratePoolToSet to the pool's current set is a no-op"):
    given ExecutionContext = executionContext
    val poolId = PoolId.BootstrapPoolId
    val setId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    for
      _ <- client.migratePoolToSet(poolId, setId)
      _ <- waitForTransactionsToComplete()

      poolState <- client.getStoragePoolState(poolId)
      set <- client.getStorageDeviceSetState(setId)
      enrolled <- enrolledTasks()
    yield
      poolState.migration should be(None)
      poolState.storageDeviceSet should be(setId)
      set.assignedPools.count(_ == poolId) should be(1)
      enrolled shouldBe empty

  atest("migratePoolToSet fails with NoSuchElementException for an unknown target set"):
    given ExecutionContext = executionContext
    val unknown = StorageDeviceSetId(java.util.UUID.randomUUID())
    recoverToSucceededIf[NoSuchElementException](
      client.migratePoolToSet(PoolId.BootstrapPoolId, unknown))

  atest("retargeting swaps assignedPools to the new target and enrolls no second task"):
    given ExecutionContext = executionContext
    val poolId = PoolId.BootstrapPoolId
    for
      firstId <- client.createStorageDeviceSet("first-target", level = 0, parent = None)
      _ <- waitForTransactionsToComplete()
      secondId <- client.createStorageDeviceSet("second-target", level = 0, parent = None)
      _ <- waitForTransactionsToComplete()

      _ <- client.migratePoolToSet(poolId, firstId)
      _ <- waitForTransactionsToComplete()
      _ <- client.migratePoolToSet(poolId, secondId)
      _ <- waitForTransactionsToComplete()

      poolState <- client.getStoragePoolState(poolId)
      first <- client.getStorageDeviceSetState(firstId)
      second <- client.getStorageDeviceSetState(secondId)
      enrolled <- enrolledTasks()
    yield
      poolState.storageDeviceSet should be(secondId)
      poolState.migration.get.targetSet should be(secondId)
      poolState.migration.get.status should be(StoragePoolState.MigrationStatus.InProgress)
      second.assignedPools should contain(poolId)
      first.assignedPools should not contain poolId
      // The running task holds only the pool id and re-reads its target, so it is reused.
      enrolled.size should be(1)

  // ---- End-to-end migration -------------------------------------------------------

  private val fastPoll = Duration(50, MILLISECONDS)

  /** Build a task instance over the state object migratePoolToSet enrolled. */
  private def taskForEnrolled(poolId: PoolId): Future[MigratePoolToSetDurableTask] =
    given ExecutionContext = executionContext
    enrolledTasks().map: enrolled =>
      enrolled.size should be(1)
      new MigratePoolToSetDurableTask(
        DurableTaskPointer(enrolled.head._2), client, poolId, fastPoll)

  /** Await the task's completion under a deadline. A bare `task.completed` would hang the whole
   *  suite rather than fail if the loop ever stopped making progress. */
  private def awaitCompletion(task: MigratePoolToSetDurableTask, timeout: Duration): Future[Unit] =
    given ExecutionContext = executionContext
    val expired = Promise[Unit]()
    client.backgroundTaskManager.schedule(timeout):
      expired.tryFailure(new AssertionError(s"task did not complete within $timeout"))
    Future.firstCompletedOf(List(task.completed.map(_ => ()), expired.future))

  /** Repeatedly complete in-flight transfers landing on `destinationSetId`'s member devices
   *  until the task completes. Fails on timeout. */
  private def driveUntilComplete(task: MigratePoolToSetDurableTask,
                                 destinationSetId: StorageDeviceSetId,
                                 timeout: Duration): Future[Unit] =
    given ExecutionContext = executionContext
    val deadline = System.nanoTime() + timeout.toNanos
    def loop(): Future[Unit] =
      if task.completed.isCompleted then
        task.completed.map(_ => ())
      else if System.nanoTime() > deadline then
        client.getStoragePoolState(PoolId.BootstrapPoolId).flatMap: ps =>
          Future.failed(new AssertionError(
            s"migration did not complete; migration=${ps.migration} " +
              s"stores=${ps.stores.map(_.storageDeviceId.uuid).mkString(",")}"))
      else
        net.completeInFlightTransfers(destinationSetId).flatMap: _ =>
          val p = Promise[Unit]()
          client.backgroundTaskManager.schedule(fastPoll)(p.success(()))
          p.future.flatMap(_ => loop())
    loop()

  /** Complete in-flight transfers until `poolId` has at least `moved` stores sitting on a
   *  device of `destinationSetId`, then stop pumping. Used to leave a migration partially
   *  done: the task only ever has one transfer in flight and a transfer completes only when
   *  this helper completes it, so ceasing to pump freezes progress deterministically. A
   *  wall-clock window would not -- it races the whole migration. Fails on timeout. */
  private def pumpUntilStoresMoved(poolId: PoolId,
                                   destinationSetId: StorageDeviceSetId,
                                   moved: Int,
                                   timeout: Duration): Future[Unit] =
    given ExecutionContext = executionContext
    val deadline = System.nanoTime() + timeout.toNanos
    def loop(): Future[Unit] =
      for
        targetSet <- client.getStorageDeviceSetState(destinationSetId)
        members <- targetSet.collectMemberDevices(client)
        poolState <- client.getStoragePoolState(poolId)
        onTarget = poolState.stores.count(e => members.contains(e.storageDeviceId))
        result <-
          if onTarget >= moved then
            Future.unit
          else if System.nanoTime() > deadline then
            Future.failed(new AssertionError(
              s"only $onTarget of $moved stores reached the target set; " +
                s"migration=${poolState.migration}"))
          else
            net.completeInFlightTransfers(destinationSetId).flatMap: _ =>
              val p = Promise[Unit]()
              client.backgroundTaskManager.schedule(fastPoll)(p.success(()))
              p.future.flatMap(_ => loop())
      yield result
    loop()

  atest("level-0 migration moves every store and marks the migration Complete"):
    given ExecutionContext = executionContext
    val poolId = PoolId.BootstrapPoolId
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      targetSetId <- client.createStorageDeviceSet("nvme", level = 0, parent = None)
      _ <- waitForTransactionsToComplete()
      _ <- client.moveDeviceToSet(net.secondDeviceId, targetSetId)
      _ <- waitForTransactionsToComplete()

      _ <- client.migratePoolToSet(poolId, targetSetId)
      _ <- waitForTransactionsToComplete()

      task <- taskForEnrolled(poolId)
      _ <- driveUntilComplete(task, targetSetId, Duration(30000, MILLISECONDS))

      poolState <- client.getStoragePoolState(poolId)
    yield
      poolState.stores.foreach: entry =>
        entry.storageDeviceId should be(net.secondDeviceId)
      poolState.migration should be(Some(StoragePoolState.Migration(
        targetSetId, StoragePoolState.MigrationStatus.Complete)))

  atest("a level-1 target set works by the same path"):
    given ExecutionContext = executionContext
    val poolId = PoolId.BootstrapPoolId
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      parentId <- client.createStorageDeviceSet("tier-1", level = 1, parent = None)
      _ <- waitForTransactionsToComplete()
      // createStorageDeviceSet links the child into the parent's memberSets.
      childId <- client.createStorageDeviceSet("tier-1-leaf", level = 0, parent = Some(parentId))
      _ <- waitForTransactionsToComplete()
      _ <- client.moveDeviceToSet(net.secondDeviceId, childId)
      _ <- waitForTransactionsToComplete()

      _ <- client.migratePoolToSet(poolId, parentId)
      _ <- waitForTransactionsToComplete()

      task <- taskForEnrolled(poolId)
      // Destinations land on the level-0 child's member devices.
      _ <- driveUntilComplete(task, childId, Duration(30000, MILLISECONDS))

      poolState <- client.getStoragePoolState(poolId)
    yield
      poolState.stores.foreach: entry =>
        entry.storageDeviceId should be(net.secondDeviceId)
      poolState.migration.get.status should be(StoragePoolState.MigrationStatus.Complete)

  atest("migration completes with zero transfers when the old set is already inside the target"):
    given ExecutionContext = executionContext
    val poolId = PoolId.BootstrapPoolId
    val bootstrapSetId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    for
      parentId <- client.createStorageDeviceSet("umbrella", level = 1, parent = None)
      _ <- waitForTransactionsToComplete()

      // Adopt the bootstrap set as a member of the level-1 set. There is no client API for
      // reparenting an existing set, so write memberSets directly.
      _ <- client.transactUntilSuccessful: tx =>
             given Transaction = tx
             for
               parentPtr <- client.getStorageDeviceSetPointer(parentId)
               dos <- client.read(parentPtr)
             yield
               val updated = StorageDeviceSetState(dos).copy(memberSets = List(bootstrapSetId))
               tx.overwrite(parentPtr, dos.revision, DataBuffer(updated.toBytes))
      _ <- waitForTransactionsToComplete()

      before <- client.getStoragePoolState(poolId)
      _ <- client.migratePoolToSet(poolId, parentId)
      _ <- waitForTransactionsToComplete()

      task <- taskForEnrolled(poolId)
      _ <- awaitCompletion(task, Duration(30000, MILLISECONDS))

      poolState <- client.getStoragePoolState(poolId)
    yield
      // Every store was already inside the target, so nothing moved.
      poolState.stores.map(_.storageDeviceId).toList should be(
        before.stores.map(_.storageDeviceId).toList)
      poolState.migration should be(Some(StoragePoolState.Migration(
        parentId, StoragePoolState.MigrationStatus.Complete)))

  atest("a fresh task instance resumes a migration after the first is stopped"):
    given ExecutionContext = executionContext
    val poolId = PoolId.BootstrapPoolId
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      targetSetId <- client.createStorageDeviceSet("resume-target", level = 0, parent = None)
      _ <- waitForTransactionsToComplete()
      _ <- client.moveDeviceToSet(net.secondDeviceId, targetSetId)
      _ <- waitForTransactionsToComplete()

      _ <- client.migratePoolToSet(poolId, targetSetId)
      _ <- waitForTransactionsToComplete()

      first <- taskForEnrolled(poolId)
      // Let exactly one store land, then stop the task mid-migration.
      _ <- pumpUntilStoresMoved(poolId, targetSetId, 1, Duration(30000, MILLISECONDS))
      _ = first.stop()

      midPool <- client.getStoragePoolState(poolId)

      // Nothing was written to say where the migration got to -- the pool's own stores array
      // is the progress record, so a fresh instance simply picks up.
      second <- taskForEnrolled(poolId)
      _ <- driveUntilComplete(second, targetSetId, Duration(30000, MILLISECONDS))

      poolState <- client.getStoragePoolState(poolId)
    yield
      midPool.migration.get.status should be(StoragePoolState.MigrationStatus.InProgress)
      // Genuinely partial: some stores moved, some did not, so the second instance has work.
      midPool.stores.count(_.storageDeviceId == net.secondDeviceId) should be >= 1
      midPool.stores.count(_.storageDeviceId != net.secondDeviceId) should be >= 1
      poolState.stores.foreach: entry =>
        entry.storageDeviceId should be(net.secondDeviceId)
      poolState.migration.get.status should be(StoragePoolState.MigrationStatus.Complete)

  atest("a completed migration re-dispatched as a task finishes immediately without changes"):
    given ExecutionContext = executionContext
    val poolId = PoolId.BootstrapPoolId
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      targetSetId <- client.createStorageDeviceSet("idempotent-target", level = 0, parent = None)
      _ <- waitForTransactionsToComplete()
      _ <- client.moveDeviceToSet(net.secondDeviceId, targetSetId)
      _ <- waitForTransactionsToComplete()

      _ <- client.migratePoolToSet(poolId, targetSetId)
      _ <- waitForTransactionsToComplete()

      task <- taskForEnrolled(poolId)
      _ <- driveUntilComplete(task, targetSetId, Duration(30000, MILLISECONDS))
      after <- client.getStoragePoolState(poolId)

      // Re-dispatch: SystemTaskExecutorService may restart a task whose state object is still
      // enrolled.
      again <- taskForEnrolled(poolId)
      _ <- awaitCompletion(again, Duration(30000, MILLISECONDS))
      afterAgain <- client.getStoragePoolState(poolId)
    yield
      afterAgain.migration should be(after.migration)
      afterAgain.stores.map(_.storageDeviceId).toList should be(
        after.stores.map(_.storageDeviceId).toList)
