package org.aspen_ddp.aspen.common.metadata.management

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.common.objects.{Insert, Key, KeyValueObjectPointer}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import org.aspen_ddp.aspen.compute.{DurableTaskPointer, ServiceEntry}
import org.aspen_ddp.aspen.compute.systemtask.{SystemTaskExecutorService, SystemTaskServiceState}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.{Duration, MILLISECONDS}

/** Shared test harness for driving FailedStorageDeviceDurableTask instances.
 *
 *  Provides helpers for enrolling tasks and racing futures against timeouts. Factored out so
 *  both FailedStorageDeviceSuite and RebuildEndToEndSuite can drive the producer half without
 *  duplication.
 */
trait FailedStorageDeviceTestHarness:
  this: IntegrationTestSuite =>

  protected val fastPoll = Duration(50, MILLISECONDS)

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

  /** Race a future against a timeout, failing with an explicit assertion if the timeout wins. */
  protected def withTimeout[T](fut: Future[T], timeout: Duration, what: String): Future[T] =
    given ExecutionContext = executionContext
    val p = Promise[T]()
    client.backgroundTaskManager.schedule(timeout):
      if !p.isCompleted then
        p.failure(new AssertionError(s"$what did not complete within ${timeout.toMillis} ms"))
    fut.onComplete(p.tryComplete)
    p.future

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

  /** Wait until `passes` has advanced by `n`. A pass already running when this is called may be
   *  the first to finish, so the assertion that follows is made after at least n - 1 passes that
   *  began afterwards. Deterministic where a fixed sleep only hopes passes ran. */
  protected def awaitPasses(passes: => Int, n: Int): Future[Unit] =
    val target = passes + n
    awaitUntil(s"$n more polling passes", Duration(20000, MILLISECONDS))(
      Future.successful(passes >= target))

  protected def statusOf(deviceId: StorageDeviceId,
                         sid: StoreId): Future[Option[StorageDeviceState.StoreStatus]] =
    given ExecutionContext = executionContext
    client.getStorageDeviceState(deviceId).map(_.stores.get(sid).map(_.status))

  protected def countTransferringOut(deviceId: StorageDeviceId): Future[Int] =
    given ExecutionContext = executionContext
    client.getStorageDeviceState(deviceId).map: ds =>
      ds.stores.values.count(_.status == StorageDeviceState.StoreStatus.TransferringOut)

  /** Declare `deviceId` failed and run the real FailedStorageDeviceDurableTask until its store
   *  map is empty. Other tasks may be enrolled too (a migration task, say), so the drain's state
   *  object is found as the one failStorageDevice added. The drain is stopped on the way out. */
  protected def failAndDrain(deviceId: StorageDeviceId): Future[Unit] =
    given ExecutionContext = executionContext
    for
      before <- enrolledTasks()
      _ <- client.failStorageDevice(deviceId)
      _ <- waitForTransactionsToComplete()
      after <- enrolledTasks()
      added = after.filterNot(e => before.exists(_._1 == e._1))
      _ = added.size should be(1)
      drain = new FailedStorageDeviceDurableTask(
                DurableTaskPointer(added.head._2), client, deviceId, fastPoll)
      _ <- withTimeout(drain.completed.map(_ => ()), Duration(60000, MILLISECONDS),
                       s"the drain of ${deviceId.uuid}").recoverWith:
             case e: AssertionError =>
               // Say where the drain stopped: the failed device's remaining stores and the tasks still enrolled
               describeStuckDrain(deviceId).map(d => throw new AssertionError(s"${e.getMessage}; $d", e))
           .transform: result =>
             drain.stop()
             result
    yield ()

  private def describeStuckDrain(deviceId: StorageDeviceId): Future[String] =
    given ExecutionContext = executionContext
    val device = client.getStorageDevicePointer(deviceId).flatMap(client.read).map: kvos =>
      s"device stores ${StorageDeviceState(kvos).stores}"
    val tasks = enrolledTasks().map(t => s"${t.size} tasks enrolled")
    Future.sequence(List(device, tasks).map(_.recover { case e => s"unreadable: $e" })).map(_.mkString(", "))

  /** Flip `sid`'s Rebuilding entry on `deviceId` to Active, as Host.markRebuiltStoreActive does
   *  once the destination host has reconstructed the store. */
  protected def completeRebuild(deviceId: StorageDeviceId, sid: StoreId): Future[Unit] =
    given ExecutionContext = executionContext
    client.transactUntilSuccessful: tx =>
      for
        ptr <- client.getStorageDevicePointer(deviceId)
        kvos <- client.read(ptr)
      yield
        val state = StorageDeviceState(kvos)
        state.stores.get(sid).map(_.status) should be(Some(StorageDeviceState.StoreStatus.Rebuilding))
        tx.update(ptr, None, None,
          List(KeyRevision(StorageDeviceState.StateKey, kvos.contents(StorageDeviceState.StateKey).revision)),
          List(Insert(StorageDeviceState.StateKey,
            state.setStoreEntry(sid, StorageDeviceState.StoreStatus.Active, None).encode())))
