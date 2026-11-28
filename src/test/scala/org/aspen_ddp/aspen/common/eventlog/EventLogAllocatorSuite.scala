package org.aspen_ddp.aspen.common.eventlog

import org.aspen_ddp.aspen.client.{InvalidObject, PoolObjectAllocatorId}
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, Root}
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.metadata.StorageDeviceSetId
import org.aspen_ddp.aspen.common.network.{TxFinalized, TxMessage, TxResolved}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.store.backend.{MapBackend, RocksDBConfig}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{Duration, MILLISECONDS}
import scala.util.Random

class EventLogAllocatorSuite extends EventLogSuiteBase:

  private val poolIda = Replication(3, 2)

  /** A second pool, its stores loaded straight into the harness's host, which never picks a new pool's up */
  private def createPool(): Future[PoolId] =
    given ExecutionContext = executionContext
    for
      poolId <- client.createNewStoragePool(s"event-log-${UUID.randomUUID()}", poolIda, None,
                  RocksDBConfig(), StorageDeviceSetId.BootstrapStorageDeviceSetId, 0L)
      loads = (0 until poolIda.width).toList.map: i =>
        net.smgr.loadStore(net.storageDeviceId, new MapBackend(StoreId(poolId, i.toByte)), None)
      _ = handleEvents()
      _ <- Future.sequence(loads)
      _ <- waitForTransactionsToComplete()
    yield poolId

  private def loggedRoot(): Future[Root] =
    given ExecutionContext = executionContext
    SystemEventLog.statePointer(client).flatMap: ptr =>
      KVObjectRootManager(client, EventLogState.LoggedKey, ptr).readStoredRoot()

  private def allocatorKey(): Future[org.aspen_ddp.aspen.client.ObjectAllocatorId] =
    given ExecutionContext = executionContext
    for
      ptr <- SystemEventLog.statePointer(client)
      kvos <- client.read(ptr)
    yield EventLogState.decodeAllocator(kvos.contents(EventLogState.AllocatorKey).value.bytes)

  /** Incompressible, so a known number of entries fills the tails and forces compactions */
  private def entries(n: Int, size: Int, seed: Int): Vector[EventLogEntry] =
    val rnd = new Random(seed)
    val base = HLCTimestamp.now.asLong
    Vector.tabulate(n): i =>
      val data = new Array[Byte](size)
      rnd.nextBytes(data)
      EventLogEntry(HLCTimestamp(base + i), if i % 2 == 0 then typeA else typeB, data)

  atest("setSystemLogAllocator directs new full objects and tails to the new pool"):
    given ExecutionContext = executionContext
    // About twice the full-object target, so the tails compact at least once after the change
    val es = entries(130, 1000, 3)
    for
      before <- loggedRoot()
      oldState <- readState()
      poolId <- createPool()
      _ <- client.setSystemLogAllocator(PoolObjectAllocatorId(poolId))
      after <- loggedRoot()
      allocator <- allocatorKey()
      changed <- readState()
      ptr <- SystemEventLog.statePointer(client)
      _ = makeService(ptr)
      _ <- logInGroups(es)
      _ <- awaitIncomingEmpty()
      st <- readState()
      objs <- fullObjects()
      oldReads <- Future.traverse(oldState.tails.pointers)(p => client.read(p).transform(t => scala.util.Success(t)))
      logged <- tailEntries()
      full <- fullEntries()
      read <- client.readSystemEvents()
    yield
      allocator shouldBe PoolObjectAllocatorId(poolId)
      // Only the Root's NodeAllocator changed: the tree keeps its root node, tier, ordering and kind
      after.copy(nodeAllocator = before.nodeAllocator) shouldBe before
      after.nodeAllocator.tiers.toList shouldBe List(PoolObjectAllocatorId(poolId) -> before.nodeAllocator.maxNodeSize(0))
      // The tails move at a compaction, not at the change
      changed.tails shouldBe oldState.tails

      objs should not be empty
      objs.foreach(o => o.state.pointer.poolId shouldBe poolId)
      st.tails.allocator shouldBe PoolObjectAllocatorId(poolId)
      st.tails.pointers.size shouldBe EventLogState.TailCount
      st.tails.pointers.foreach(p => p.poolId shouldBe poolId)
      st.tails.pointers.toSet.intersect(oldState.tails.pointers.toSet) shouldBe empty
      // Every old tail's refcount reached 0, so it no longer reads
      oldReads.foreach: r =>
        r.isFailure shouldBe true
        r.failed.get shouldBe an[InvalidObject]
      multiset(logged ++ full) shouldBe multiset(es)
      multiset(read) shouldBe multiset(es)

  atest("setSystemLogAllocator fails for an unknown allocator"):
    given ExecutionContext = executionContext
    for
      before <- loggedRoot()
      beforeAllocator <- allocatorKey()
      err <- client.setSystemLogAllocator(PoolObjectAllocatorId(PoolId(UUID.randomUUID()))).failed
      after <- loggedRoot()
      afterAllocator <- allocatorKey()
    yield
      err should not be null
      afterAllocator shouldBe beforeAllocator
      after.nodeAllocator.tiers.toList shouldBe before.nodeAllocator.tiers.toList

  /** Logs `es` with the tails in a new pool, then logs one more entry whose move commits but whose outcome the new
    * pool's stores do not hear: they vote commit for it and keep its lock on the tail, while the bootstrap pool
    * applies it. Returns the entry and a release that delivers the held outcome. This client learns commits only
    * from votes and keeps no status cache, so nothing it reads tells it the move committed except the move's
    * applied effects */
  private def holdMoveOutcome(es: Vector[EventLogEntry]): Future[(EventLogEntry, () => Unit)] =
    given ExecutionContext = executionContext
    var holding = false
    var held = List[TxMessage]()
    for
      poolId <- createPool()
      _ <- client.setSystemLogAllocator(PoolObjectAllocatorId(poolId))
      ptr <- SystemEventLog.statePointer(client)
      _ = makeService(ptr)
      _ <- logInGroups(es)
      _ <- awaitIncomingEmpty()
      st <- readState()
      _ = st.tails.pointers.foreach(p => p.poolId shouldBe poolId)
      newPool = (0 until poolIda.width).map(i => StoreId(poolId, i.toByte)).toSet
      // The new pool's stores never lead, and hear no outcome while holding
      _ = net.unreachableStores = newPool
      _ = net.dropTxMessage =
            case m @ (_: TxResolved | _: TxFinalized) if holding && newPool.contains(m.to) =>
              held = held :+ m
              true
            case _ => false
      _ = holding = true
      e = EventLogEntry(HLCTimestamp.now, typeA, "moved while held".getBytes)
      _ <- client.logSystemEvent(e)
      // Gone from incomingEvents: its move committed and applied there, not yet at the tail
      _ <- awaitIncomingEmpty()
    yield
      val release = () =>
        holding = false
        net.dropTxMessage = _ => false
        net.unreachableStores = Set()
        held.foreach(net.deliverTransactionMessage)
      (e, release)

  atest("a move whose outcome its tails' pool has not heard yet is still read"):
    given ExecutionContext = executionContext
    // Enough to compact after the change, so the tails move to the new pool
    val es = entries(130, 1000, 5)
    var released = false
    for
      (e, release) <- holdMoveOutcome(es)
      reader = new EventLogReader(client)
      // The outcome reaches the new pool only after the reader has read incomingEvents and sent its tail reads
      _ = reader.testAfterIncomingRead = () =>
            client.backgroundTaskManager.schedule(Duration(300, MILLISECONDS)):
              executionContext.execute: () =>
                release()
                released = true
            Future.unit
      read <- reader.read(EventLogReader.Earliest, None, None)
      releasedBeforeRead = released
    yield
      // An ordinary read of the tail returns the state the move replaced (three matching replies holding a lock of
      // a transaction this client does not know committed), and the read omits e. A settled read waits for the lock
      read should contain (e)
      releasedBeforeRead shouldBe true
      multiset(read).filter(_._1 != e) shouldBe multiset(es)

  atest("a read that cannot settle before its timeout fails with SystemEventLogReadTimeout"):
    given ExecutionContext = executionContext
    val es = entries(130, 1000, 6)
    val timeout = Duration(300, MILLISECONDS)
    for
      (e, release) <- holdMoveOutcome(es)
      // The tail's stores hold the move's lock until they hear its outcome, which they never do while held: a
      // settled read of the tail must wait, and returning without e would omit it
      started = System.nanoTime()
      err <- new EventLogReader(client).read(EventLogReader.Earliest, None, None, timeout).failed
      elapsed = Duration.fromNanos(System.nanoTime() - started)
      _ = release()
      read <- client.readSystemEvents()
    yield
      err shouldBe a[SystemEventLogReadTimeout]
      err.asInstanceOf[SystemEventLogReadTimeout].timeout shouldBe timeout
      // The cause names the object that could not settle
      err.getCause shouldBe a[org.aspen_ddp.aspen.client.ReadTimeout]
      elapsed should be >= timeout
      // Once the outcome arrives the log reads in full
      read should contain (e)
