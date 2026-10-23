package org.aspen_ddp.aspen.common.network.snapshot

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.{RegisteredTypeFactory, Transaction}
import org.aspen_ddp.aspen.client.registries.NamespacedUUIDRegistry
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.{HLCTimestamp, Radicle}
import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.objects.{Insert, Key, KeyValueObjectPointer}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate
import org.aspen_ddp.aspen.compute.ServiceEntry
import org.aspen_ddp.aspen.compute.impl.SimpleDurableServiceExecutor

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.{Duration, HOURS, MILLISECONDS}

class NetworkCachingServiceSuite extends IntegrationTestSuite:

  override def userTypeFactories: List[RegisteredTypeFactory] = List(NetworkCachingService)

  private var executors: List[SimpleDurableServiceExecutor] = Nil

  override def subFixtureSetup(): Unit =
    executors = Nil
    NetworkCachingService.setTestPeriods(client, Duration(100, MILLISECONDS), Duration(1, HOURS))

  override def subFixtureTeardown(): Unit =
    executors.foreach(_.shutdown())
    executors = Nil

  private def makeExecutor(): SimpleDurableServiceExecutor =
    val exec = new SimpleDurableServiceExecutor(
      client, HostId.BootstrapHostId, client.backgroundTaskManager,
      leaseDuration        = Duration(30000, MILLISECONDS),
      renewalInterval      = Duration(1000, MILLISECONDS),
      minScanInterval      = Duration(40, MILLISECONDS),
      maxScanInterval      = Duration(80, MILLISECONDS),
      claimDelayPerService = Duration(0, MILLISECONDS))
    executors ::= exec
    exec

  private def servicesTkvl() =
    TieredKeyValueList(client, KVObjectRootManager(client, Radicle.ServicesTreeKey, Radicle.pointer))

  private def readStatePointer(): Future[KeyValueObjectPointer] =
    given ExecutionContext = executionContext
    servicesTkvl().get(Key(NetworkCachingService.ServiceUUID)).map:
      case Some(vs) => ServiceEntry.decode(vs.value.bytes).statePointer
      case None => throw new IllegalStateException("NetworkCachingService is not registered")

  private def readLastRun(): Future[HLCTimestamp] =
    given ExecutionContext = executionContext
    for
      ptr <- readStatePointer()
      kvos <- client.read(ptr)
    yield NetworkCachingService.decodeLastRun(kvos.contents(NetworkCachingService.LastRunKey).value.bytes)

  private def writeLastRun(ts: HLCTimestamp): Future[Unit] =
    given ExecutionContext = executionContext
    readStatePointer().flatMap: ptr =>
      client.transactUntilSuccessful: tx =>
        given Transaction = tx
        client.read(ptr).map: kvos =>
          val reqs = KeyValueUpdate.KeyRevision(
                       NetworkCachingService.LastRunKey,
                       kvos.contents(NetworkCachingService.LastRunKey).revision) :: Nil
          val ops = Insert(NetworkCachingService.LastRunKey, NetworkCachingService.encodeLastRun(ts)) :: Nil
          tx.update(ptr, None, None, reqs, ops)

  private def snapshots(): Future[(HostSnapshot, StoreSnapshot)] =
    given ExecutionContext = executionContext
    NetworkCachingService.readSnapshots(client).map(_.get)

  private def awaitUntil(what: String, timeout: Duration = Duration(60000, MILLISECONDS))
                        (cond: => Future[Boolean]): Future[Unit] =
    given ExecutionContext = executionContext
    val deadline = System.nanoTime() + timeout.toNanos
    def loop(): Future[Unit] =
      cond.flatMap: ok =>
        if ok then Future.unit
        else if System.nanoTime() > deadline then
          Future.failed(new AssertionError(s"timed out waiting for: $what"))
        else
          val p = Promise[Unit]()
          client.backgroundTaskManager.schedule(Duration(100, MILLISECONDS)):
            p.success(())
          p.future.flatMap(_ => loop())
    loop()

  private def pause(ms: Int): Future[Unit] =
    val p = Promise[Unit]()
    client.backgroundTaskManager.schedule(Duration(ms, MILLISECONDS)):
      p.success(())
    p.future

  atest("bootstrap registers the service with empty snapshots"):
    given ExecutionContext = executionContext
    snapshots().map: (hosts, stores) =>
      hosts shouldBe HostSnapshot(HLCTimestamp.Zero, Nil)
      stores shouldBe StoreSnapshot(HLCTimestamp.Zero, Nil)

  atest("a due run writes every host and every store"):
    given ExecutionContext = executionContext
    makeExecutor()
    for
      _ <- awaitUntil("hosts snapshot to be written")(
             snapshots().map(_._1.scanStart > HLCTimestamp.Zero))
      (hosts, stores) <- snapshots()
      (hostState, _) <- client.getHostStateAndTimestamp(HostId.BootstrapHostId)
      (poolState, _) <- client.getStoragePoolStateAndTimestamp(Radicle.poolId)
    yield
      val h = hosts.hosts.find(_.hostId == HostId.BootstrapHostId).get
      (h.address, h.dataPort, h.httpPort, h.storeTransferPort) shouldBe
        (hostState.address, hostState.dataPort, hostState.httpPort, hostState.storeTransferPort)
      val expected = poolState.stores.indices.map: i =>
        StoreId(Radicle.poolId, i.toByte) -> poolState.stores(i).hostId
      val actual = stores.stores.filter(_.storeId.poolId == Radicle.poolId).map(s => s.storeId -> s.hostId)
      actual.toSet shouldBe expected.toSet

  atest("the snapshot objects are overwritten, not reallocated"):
    given ExecutionContext = executionContext
    def pointers() =
      for
        ptr <- readStatePointer()
        kvos <- client.read(ptr)
      yield (kvos.contents(NetworkCachingService.HostSnapshotKey).value.bytes.toList,
             kvos.contents(NetworkCachingService.StoreSnapshotKey).value.bytes.toList)
    makeExecutor()
    for
      before <- pointers()
      _ <- awaitUntil("hosts snapshot to be written")(
             snapshots().map(_._1.scanStart > HLCTimestamp.Zero))
      after <- pointers()
    yield
      after shouldBe before

  atest("a recent run defers the next one"):
    given ExecutionContext = executionContext
    for
      _ <- writeLastRun(HLCTimestamp.now)
      _ <- Future.unit.map(_ => makeExecutor())
      _ <- pause(2000)
      (hosts, stores) <- snapshots()
      // Positive control: making the run due proves the service was alive and deferral was the cause
      _ <- writeLastRun(HLCTimestamp.Zero)
      _ <- awaitUntil("hosts snapshot to be written once due")(
             snapshots().map(_._1.scanStart > HLCTimestamp.Zero))
    yield
      hosts.scanStart shouldBe HLCTimestamp.Zero
      stores.scanStart shouldBe HLCTimestamp.Zero

  atest("a failed host read aborts the run"):
    given ExecutionContext = executionContext
    for
      _ <- client.registerId("host", "bogus-host", UUID.randomUUID())
      _ <- Future.unit.map(_ => makeExecutor())
      _ <- pause(2000)
      (hosts, stores) <- snapshots()
      lastRun <- readLastRun()
      // Positive control: removing the bogus entry lets a run succeed, so the abort was the cause
      _ <- client.transactUntilSuccessful: tx =>
             given Transaction = tx
             TieredKeyValueList(client, KVObjectRootManager(client, Radicle.NamespacedRegistryKey, Radicle.pointer))
               .delete(NamespacedUUIDRegistry.makeKey("host", "bogus-host"))
      _ <- awaitUntil("hosts snapshot to be written after removing the bogus host")(
             snapshots().map(_._1.scanStart > HLCTimestamp.Zero))
    yield
      hosts.hosts shouldBe empty
      stores.stores shouldBe empty
      hosts.scanStart shouldBe HLCTimestamp.Zero
      lastRun shouldBe HLCTimestamp.Zero

  atest("readSnapshots is None without a service entry"):
    given ExecutionContext = executionContext
    for
      _ <- client.transactUntilSuccessful: tx =>
             given Transaction = tx
             servicesTkvl().delete(Key(NetworkCachingService.ServiceUUID))
      r <- NetworkCachingService.readSnapshots(client)
    yield
      r shouldBe None
