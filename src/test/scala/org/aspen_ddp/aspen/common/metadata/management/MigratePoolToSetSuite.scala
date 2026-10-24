package org.aspen_ddp.aspen.common.metadata.management

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.TypeFactories
import org.aspen_ddp.aspen.common.ida.{IDA, Replication}
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceSetId, StorageDeviceSetState,
                                            StorageDeviceState, StoragePoolState}
import org.aspen_ddp.aspen.common.objects.{Key, KeyValueObjectPointer}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.compute.DurableTaskFactory
import org.aspen_ddp.aspen.compute.DurableTaskPointer
import org.aspen_ddp.aspen.compute.ServiceEntry
import org.aspen_ddp.aspen.compute.systemtask.{SystemTaskExecutorService, SystemTaskServiceState}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.{Duration, MILLISECONDS}

/** Shared harness for the migration suites; subclasses pick the bootstrap pool's IDA. */
abstract class MigratePoolToSetSuiteBase extends IntegrationTestSuite:

  private var tasks: List[MigratePoolToSetDurableTask] = Nil

  // Runs on failure and timeout too, so a failed test cannot leave a task polling.
  override def subFixtureSetup(): Unit = tasks = Nil

  override def subFixtureTeardown(): Unit = synchronized:
    tasks.foreach(_.stop())
    tasks = Nil

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

  protected val fastPoll = Duration(50, MILLISECONDS)

  /** Build a task instance over the state object migratePoolToSet enrolled. */
  protected def taskForEnrolled(poolId: PoolId): Future[MigratePoolToSetDurableTask] =
    given ExecutionContext = executionContext
    enrolledTasks().map: enrolled =>
      enrolled.size should be(1)
      val task = new MigratePoolToSetDurableTask(
        DurableTaskPointer(enrolled.head._2), client, poolId, fastPoll)
      synchronized { tasks ::= task }
      task

  /** Await the task's completion under a deadline. A bare `task.completed` would hang the whole
   *  suite rather than fail if the loop ever stopped making progress. */
  protected def awaitCompletion(task: MigratePoolToSetDurableTask, timeout: Duration): Future[Unit] =
    given ExecutionContext = executionContext
    val expired = Promise[Unit]()
    client.backgroundTaskManager.schedule(timeout):
      expired.tryFailure(new AssertionError(s"task did not complete within $timeout"))
    Future.firstCompletedOf(List(task.completed.map(_ => ()), expired.future))

  /** Repeatedly complete in-flight transfers landing on `destinationSetId`'s member devices
   *  until the task completes. Fails on timeout. */
  protected def driveUntilComplete(task: MigratePoolToSetDurableTask,
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

  /** Like `driveUntilComplete`, but pumps several sets on every pass.
   *
   *  A retarget can leave a transfer in flight toward the set just abandoned, and in a pool whose
   *  floor admits one transfer at a time the task can start nothing new until it clears -- so
   *  pumping only the new target would deadlock. Sequential rather than parallel: each pump transacts against the same
   *  pool object, and concurrent passes would collide. Fails on timeout. */
  protected def driveUntilCompleteAcross(task: MigratePoolToSetDurableTask,
                                         destinationSetIds: List[StorageDeviceSetId],
                                         timeout: Duration): Future[Unit] =
    given ExecutionContext = executionContext
    val deadline = System.nanoTime() + timeout.toNanos

    def pumpAll(remaining: List[StorageDeviceSetId]): Future[Unit] = remaining match
      case Nil => Future.unit
      case head :: tail => net.completeInFlightTransfers(head).flatMap(_ => pumpAll(tail))

    def loop(): Future[Unit] =
      if task.completed.isCompleted then
        task.completed.map(_ => ())
      else if System.nanoTime() > deadline then
        client.getStoragePoolState(PoolId.BootstrapPoolId).flatMap: ps =>
          Future.failed(new AssertionError(
            s"migration did not complete; migration=${ps.migration} " +
              s"stores=${ps.stores.map(_.storageDeviceId.uuid).mkString(",")}"))
      else
        pumpAll(destinationSetIds).flatMap: _ =>
          val p = Promise[Unit]()
          client.backgroundTaskManager.schedule(fastPoll)(p.success(()))
          p.future.flatMap(_ => loop())
    loop()

  /** Complete in-flight transfers until `poolId` has at least `moved` stores sitting on a
   *  device of `destinationSetId`, then stop pumping. Used to leave a migration partially
   *  done: the floor admits one transfer of the default pool at a time and a transfer completes
   *  only when this helper completes it, so ceasing to pump freezes progress deterministically.
   *  A wall-clock window would not -- it races the whole migration. Fails on timeout. */
  protected def pumpUntilStoresMoved(poolId: PoolId,
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
          sleep(fastPoll).flatMap(_ => loop())
    loop()

  protected def statusOf(deviceId: StorageDeviceId,
                         sid: StoreId): Future[Option[StorageDeviceState.StoreStatus]] =
    given ExecutionContext = executionContext
    client.getStorageDeviceState(deviceId).map(_.stores.get(sid).map(_.status))

  protected def countTransferringOut(deviceId: StorageDeviceId): Future[Int] =
    given ExecutionContext = executionContext
    client.getStorageDeviceState(deviceId).map: ds =>
      ds.stores.values.count(_.status == StorageDeviceState.StoreStatus.TransferringOut)

  /** A new level-0 set holding only the second device, ready to be a migration target. */
  protected def targetWithSecondDevice(name: String): Future[StorageDeviceSetId] =
    given ExecutionContext = executionContext
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      targetSetId <- client.createStorageDeviceSet(name, level = 0, parent = None)
      _ <- waitForTransactionsToComplete()
      _ <- client.moveDeviceToSet(net.secondDeviceId, targetSetId)
      _ <- waitForTransactionsToComplete()
    yield targetSetId

/** Default Replication(3, 2): floor min(2 + 2, 3 - 1) = 2, so one store of the pool may be in
 *  flight at a time. */
class MigratePoolToSetSuite extends MigratePoolToSetSuiteBase:


  atest("MigratePoolToSetDurableTask is registered in the client type registry"):
    given ExecutionContext = executionContext
    // SystemTaskExecutorService reconstructs a task by looking its type UUID up here; an
    // unregistered factory makes prepareSystemDurableTask StopRetrying.
    val factory = client.typeRegistry.getType[DurableTaskFactory](
      MigratePoolToSetDurableTask.typeUUID)
    factory should be(Some(MigratePoolToSetDurableTask))
    TypeFactories.factories should contain(MigratePoolToSetDurableTask)


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
      _ <- driveUntilComplete(task, targetSetId, Duration(120000, MILLISECONDS))

      poolState <- client.getStoragePoolState(poolId)
    yield
      // Size checked first: a foreach over an empty array would assert nothing.
      poolState.stores.size should be(3)
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
      _ <- driveUntilComplete(task, childId, Duration(120000, MILLISECONDS))

      poolState <- client.getStoragePoolState(poolId)
    yield
      // Size checked first: a foreach over an empty array would assert nothing.
      poolState.stores.size should be(3)
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
      _ <- awaitCompletion(task, Duration(120000, MILLISECONDS))

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
      _ <- pumpUntilStoresMoved(poolId, targetSetId, 1, Duration(120000, MILLISECONDS))
      _ = first.stop()

      midPool <- client.getStoragePoolState(poolId)

      // Nothing was written to say where the migration got to -- the pool's own stores array
      // is the progress record, so a fresh instance simply picks up.
      second <- taskForEnrolled(poolId)
      _ <- driveUntilComplete(second, targetSetId, Duration(120000, MILLISECONDS))

      poolState <- client.getStoragePoolState(poolId)
    yield
      midPool.migration.get.status should be(StoragePoolState.MigrationStatus.InProgress)
      // Genuinely partial: some stores moved, some did not, so the second instance has work.
      midPool.stores.count(_.storageDeviceId == net.secondDeviceId) should be >= 1
      midPool.stores.count(_.storageDeviceId != net.secondDeviceId) should be >= 1
      // Size checked first: a foreach over an empty array would assert nothing. The counts
      // above constrain midPool, not poolState.
      poolState.stores.size should be(3)
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
      _ <- driveUntilComplete(task, targetSetId, Duration(120000, MILLISECONDS))
      after <- client.getStoragePoolState(poolId)

      // Re-dispatch: SystemTaskExecutorService may restart a task whose state object is still
      // enrolled.
      again <- taskForEnrolled(poolId)
      _ <- awaitCompletion(again, Duration(120000, MILLISECONDS))
      afterAgain <- client.getStoragePoolState(poolId)
    yield
      afterAgain.migration should be(after.migration)
      afterAgain.stores.map(_.storageDeviceId).toList should be(
        after.stores.map(_.storageDeviceId).toList)

  atest("retargeting mid-flight redirects the running task to the new set"):
    given ExecutionContext = executionContext
    val poolId = PoolId.BootstrapPoolId
    val bootstrapSetId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      setA <- client.createStorageDeviceSet("retarget-first", level = 0, parent = None)
      _ <- waitForTransactionsToComplete()
      // Moving the second device out leaves the bootstrap set holding device 0 alone, so it
      // serves as the second target -- no third device needed.
      _ <- client.moveDeviceToSet(net.secondDeviceId, setA)
      _ <- waitForTransactionsToComplete()

      _ <- client.migratePoolToSet(poolId, setA)
      _ <- waitForTransactionsToComplete()

      task <- taskForEnrolled(poolId)
      _ <- pumpUntilStoresMoved(poolId, setA, 1, Duration(120000, MILLISECONDS))
      midPool <- client.getStoragePoolState(poolId)

      // Retarget the same running task. It holds only the pool id and re-reads the target
      // every pass, so no new task is enrolled and none needs to be.
      _ <- client.migratePoolToSet(poolId, bootstrapSetId)
      _ <- waitForTransactionsToComplete()
      enrolled <- enrolledTasks()

      // Both sets get pumped: a transfer toward setA may still be in flight at the moment of
      // the retarget, and the task starts nothing new until it clears.
      _ <- driveUntilCompleteAcross(task, List(setA, bootstrapSetId),
             Duration(120000, MILLISECONDS))

      poolState <- client.getStoragePoolState(poolId)
    yield
      // The retarget landed mid-flight: some stores had reached setA, some had not.
      midPool.stores.count(_.storageDeviceId == net.secondDeviceId) should be >= 1
      midPool.stores.count(_.storageDeviceId != net.secondDeviceId) should be >= 1
      enrolled.size should be(1)
      poolState.stores.size should be(3)
      poolState.stores.foreach: entry =>
        entry.storageDeviceId should be(net.storageDeviceId)
      poolState.migration should be(Some(StoragePoolState.Migration(
        bootstrapSetId, StoragePoolState.MigrationStatus.Complete)))

  atest("an aborted migration transfer is restarted toward a freshly selected destination"):
    given ExecutionContext = executionContext
    val poolId = PoolId.BootstrapPoolId
    val s0 = StoreId(poolId, 0)
    for
      targetSetId <- targetWithSecondDevice("abort-target")
      _ <- net.createThirdDevice()
      _ <- waitForTransactionsToComplete()

      _ <- client.migratePoolToSet(poolId, targetSetId)
      _ <- waitForTransactionsToComplete()

      task <- taskForEnrolled(poolId)
      _ <- awaitUntil("the first transfer to start")(
             statusOf(net.secondDeviceId, s0).map(_.contains(StorageDeviceState.StoreStatus.TransferringIn)))

      // Swap the target set's only member while the transfer is in flight, so the only
      // destination selection can offer after the abort is the third device.
      _ <- client.moveDeviceToSet(net.thirdDeviceId, targetSetId)
      _ <- waitForTransactionsToComplete()
      _ <- client.moveDeviceToSet(net.secondDeviceId, StorageDeviceSetId.BootstrapStorageDeviceSetId)
      _ <- waitForTransactionsToComplete()

      _ <- client.abortStoreTransfer(s0)
      _ <- awaitUntil("the store to be restarted toward the third device")(
             statusOf(net.thirdDeviceId, s0).map(_.contains(StorageDeviceState.StoreStatus.TransferringIn)))
      onSecond <- statusOf(net.secondDeviceId, s0)

      _ <- driveUntilComplete(task, targetSetId, Duration(120000, MILLISECONDS))
      poolState <- client.getStoragePoolState(poolId)
    yield
      onSecond should be(None)
      poolState.stores.size should be(3)
      poolState.stores.foreach: entry =>
        entry.storageDeviceId should be(net.thirdDeviceId)
      poolState.migration.get.status should be(StoragePoolState.MigrationStatus.Complete)

/** Replication(10, 6): floor min(6 + 2, 10 - 1) = 8, so two of the ten stores may be in flight at
 *  once. (The harness needs writeThreshold > width / 2, which rules out narrower pools that admit
 *  two transfers.) */
class ParallelMigratePoolToSetSuite extends MigratePoolToSetSuiteBase:
  override def testIda: IDA = Replication(10, 6)

  atest("one pass starts as many transfers as the floor admits"):
    given ExecutionContext = executionContext
    val poolId = PoolId.BootstrapPoolId
    val boot = net.storageDeviceId
    for
      targetSetId <- targetWithSecondDevice("parallel-target")
      _ <- client.migratePoolToSet(poolId, targetSetId)
      _ <- waitForTransactionsToComplete()

      task <- taskForEnrolled(poolId)
      _ <- awaitUntil("two transfers to be in flight together", Duration(20000, MILLISECONDS))(
             countTransferringOut(boot).map(_ >= 2))
      // Several more passes: a third must still be held back by the floor.
      _ <- sleep(Duration(500, MILLISECONDS))
      inFlight <- countTransferringOut(boot)

      _ <- driveUntilComplete(task, targetSetId, Duration(120000, MILLISECONDS))
      poolState <- client.getStoragePoolState(poolId)
    yield
      inFlight should be(2)
      poolState.stores.size should be(10)
      poolState.stores.foreach: entry =>
        entry.storageDeviceId should be(net.secondDeviceId)
      poolState.migration.get.status should be(StoragePoolState.MigrationStatus.Complete)

/** Replication(3, 3): writeThreshold == width, so the floor clamps to width - 1 = 2 and exactly
 *  one store may be in flight at a time. */
class FullWidthMigratePoolToSetSuite extends MigratePoolToSetSuiteBase:
  override def testIda: IDA = Replication(3, 3)

  atest("a writeThreshold == width pool still migrates, one store at a time"):
    given ExecutionContext = executionContext
    val poolId = PoolId.BootstrapPoolId
    val boot = net.storageDeviceId
    @volatile var maxInFlight = 0

    // Sample the source device until the task completes, recording the most stores ever
    // TransferringOut together.
    def sample(task: MigratePoolToSetDurableTask): Future[Unit] =
      if task.completed.isCompleted then
        Future.unit
      else
        countTransferringOut(boot).flatMap: n =>
          maxInFlight = math.max(maxInFlight, n)
          sleep(Duration(10, MILLISECONDS)).flatMap(_ => sample(task))

    for
      targetSetId <- targetWithSecondDevice("full-width-target")
      _ <- client.migratePoolToSet(poolId, targetSetId)
      _ <- waitForTransactionsToComplete()

      task <- taskForEnrolled(poolId)
      sampling = sample(task)
      _ <- driveUntilComplete(task, targetSetId, Duration(120000, MILLISECONDS))
      _ <- sampling
      poolState <- client.getStoragePoolState(poolId)
    yield
      maxInFlight should be(1)
      poolState.stores.size should be(3)
      poolState.stores.foreach: entry =>
        entry.storageDeviceId should be(net.secondDeviceId)
      poolState.migration.get.status should be(StoragePoolState.MigrationStatus.Complete)
