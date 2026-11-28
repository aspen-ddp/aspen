package org.aspen_ddp.aspen.common.eventlog

import org.aspen_ddp.aspen.client.{PoolObjectAllocatorId, Transaction}
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.ida.{IDA, ReedSolomon}
import org.aspen_ddp.aspen.common.metadata.StorageDeviceSetId
import org.aspen_ddp.aspen.common.objects.Insert
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import org.aspen_ddp.aspen.server.store.backend.{MapBackend, RocksDBConfig}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

/** The event log on a pool whose slice limit caps log objects below one event.
  *
  * The bootstrap pool, which holds incomingEvents, keeps no slice limit: with the log in the same pool, an event that
  * fits incomingEvents always fits a log object, so nothing could be dropped. The log is pointed at a second, small
  * pool instead (its allocator and all ten tails), written straight into the service state.
  */
class EventLogSmallSliceSuite extends EventLogSuiteBase:

  override def testIda: IDA = ReedSolomon(5, 3, 4)

  override def userTypeFactories: List[org.aspen_ddp.aspen.client.RegisteredTypeFactory] =
    List(EventLogDurableService, ObserverTestTypes.FactoryA, ObserverTestTypes.FactoryB,
      ObserverTestTypes.ObserverFactory)

  private val logIda = ReedSolomon(5, 3, 4)
  private val logSliceSize = 2048

  /** A pool for the log, its stores loaded straight into the harness's host, which never picks a new pool's up */
  private def createLogPool(): Future[PoolId] =
    given ExecutionContext = executionContext
    for
      poolId <- client.createNewStoragePool(s"event-log-small-${UUID.randomUUID()}", logIda, Some(logSliceSize),
                  RocksDBConfig(), StorageDeviceSetId.BootstrapStorageDeviceSetId, 0L)
      loads = (0 until logIda.width).toList.map: i =>
        net.smgr.loadStore(net.storageDeviceId, new MapBackend(StoreId(poolId, i.toByte)), None)
      _ = handleEvents()
      _ <- Future.sequence(loads)
      _ <- waitForTransactionsToComplete()
    yield poolId

  /** Points the log allocator at `poolId` and replaces the tails with empty ones allocated there */
  private def moveLogTo(poolId: PoolId): Future[Unit] =
    given ExecutionContext = executionContext
    for
      ptr <- SystemEventLog.statePointer(client)
      pool <- client.getStoragePool(poolId)
      _ <- client.transact: tx =>
             given Transaction = tx
             for
               kvos <- client.read(ptr)
               tails <- Future.traverse(Vector.fill(EventLogState.TailCount)(()))(_ =>
                          pool.allocator.allocateDataObject(DataBuffer(Array.emptyByteArray)))
             yield
               val allocator = PoolObjectAllocatorId(poolId)
               def req(k: org.aspen_ddp.aspen.common.objects.Key) = KeyRevision(k, kvos.contents(k).revision)
               tx.update(ptr, None, None,
                 List(req(EventLogState.AllocatorKey), req(EventLogState.TailsKey)),
                 List(
                   Insert(EventLogState.AllocatorKey, EventLogState.encodeAllocator(allocator)),
                   Insert(EventLogState.TailsKey,
                     EventLogState.encodeTails(EventLogState.Tails(tails, 0, allocator)))))
    yield ()

  private def incompressible(rnd: Random, size: Int): Array[Byte] =
    val a = new Array[Byte](size)
    rnd.nextBytes(a)
    a

  atest("an event larger than the object cap is replaced by a DroppedEvent"):
    given ExecutionContext = executionContext
    val rnd = new Random(7)
    val base = HLCTimestamp.now.asLong
    val small = EventLogEntry(HLCTimestamp(base), typeA, incompressible(rnd, 100))
    val big = EventLogEntry(HLCTimestamp(base + 1), typeB, incompressible(rnd, 10 * 1024))
    for
      poolId <- createLogPool()
      pool <- client.getStoragePool(poolId)
      _ <- moveLogTo(poolId)
      // Logged before the service starts, so both move in one batch
      _ <- logInGroups(List(small, big))
      ptr <- SystemEventLog.statePointer(client)
      _ = makeService(ptr)
      _ <- awaitIncomingEmpty()
      logged <- tailEntries()
      full <- fullEntries()
    yield
      pool.maxObjectSize.exists(_ < big.data.length) shouldBe true
      val all = logged ++ full
      all should contain(small)
      all.filter(_.typeUUID == typeB) shouldBe empty
      val dropped = all.filter(_.typeUUID == DroppedEvent.TypeUUID).map(DroppedEvent.createEvent)
      dropped shouldBe Vector(DroppedEvent(big.timestamp, typeB, DropReason.ExceededMaximumLogEntrySize))

  atest("no log object exceeds the pool's maxObjectSize"):
    given ExecutionContext = executionContext
    val rnd = new Random(11)
    val base = HLCTimestamp.now.asLong
    // Mostly sizes below the cap, every tenth above it; incompressible so compression cannot hide an overshoot
    val entries = (0 until 80).map: i =>
      val size = if i % 10 == 9 then 7000 else rnd.nextInt(3000)
      EventLogEntry(HLCTimestamp(base + i), if i % 2 == 0 then typeA else typeB, incompressible(rnd, size))
    for
      poolId <- createLogPool()
      pool <- client.getStoragePool(poolId)
      _ <- moveLogTo(poolId)
      _ <- logInGroups(entries, 5)
      ptr <- SystemEventLog.statePointer(client)
      _ = makeService(ptr)
      _ <- awaitIncomingEmpty()
      st <- readState()
      tails <- Future.traverse(st.tails.pointers)(p => client.read(p))
      objs <- fullObjects()
      logged <- tailEntries()
      full <- fullEntries()
    yield
      val cap = pool.maxObjectSize.get
      objs.size should be > 1
      objs.foreach(o => o.state.pointer.poolId shouldBe poolId)
      st.tails.pointers.foreach(p => p.poolId shouldBe poolId)
      (tails.map(_.size) ++ objs.map(_.state.size)).foreach(_ should be <= cap)
      // Every entry is logged once, except the oversized ones, each replaced by one DroppedEvent
      val (oversized, kept) = entries.partition(_.data.length == 7000)
      val all = logged ++ full
      multiset(all.filter(_.typeUUID != DroppedEvent.TypeUUID)) shouldBe multiset(kept)
      all.filter(_.typeUUID == DroppedEvent.TypeUUID).map(DroppedEvent.createEvent).sortBy(_.timestamp) shouldBe
        oversized.map(e => DroppedEvent(e.timestamp, e.typeUUID, DropReason.ExceededMaximumLogEntrySize))

  atest("an observer sees the DroppedEvent replacement and not the dropped original"):
    given ExecutionContext = executionContext
    import ObserverTestTypes.*
    val rnd = new Random(9)
    val base = HLCTimestamp.now.asLong
    val small = EventLogEntry(HLCTimestamp(base), EvA, incompressible(rnd, 100))
    val big = EventLogEntry(HLCTimestamp(base + 1), EvB, incompressible(rnd, 10 * 1024))
    val c = s"${UUID.randomUUID()}|ALL"
    for
      poolId <- createLogPool()
      _ <- moveLogTo(poolId)
      _ <- client.addSystemEventObserver(ObserverFactory.typeUUID, c.getBytes)
      _ <- logInGroups(List(small, big))
      ptr <- SystemEventLog.statePointer(client)
      _ = makeService(ptr)
      _ <- awaitIncomingEmpty()
      _ <- awaitUntil("both events observed")(Future.successful(seenBy(c).size >= 2))
    yield
      seenBy(c) shouldBe Vector(TestEvent(small.timestamp, EvA),
        DroppedEvent(big.timestamp, EvB, DropReason.ExceededMaximumLogEntrySize))
