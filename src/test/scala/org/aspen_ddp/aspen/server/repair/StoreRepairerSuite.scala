package org.aspen_ddp.aspen.server.repair

import org.aspen_ddp.aspen.client.TestTransactions

import org.aspen_ddp.aspen.{IntegrationTestSuite, PinAudit}
import org.aspen_ddp.aspen.client.{ObjectState as ClientObjectState, ReadLocks, Transaction}
import org.aspen_ddp.aspen.client.internal.transaction.UnackedCommitEntry
import org.aspen_ddp.aspen.client.internal.transaction.MissedUpdateMarker.MissedUnackedCommit
import org.aspen_ddp.aspen.client.internal.allocation.PoolObjectAllocator
import org.aspen_ddp.aspen.client.tkvl.TieredKeyValueList
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Delete, Insert, Key, KeyValueObjectPointer, ObjectId,
                                           ObjectPointer, ObjectRefcount, Value}
import org.aspen_ddp.aspen.common.transaction.{KeyValueUpdate, TransactionId}
import org.aspen_ddp.aspen.server.store.{ReconcileResult, RepairSnapshot}
import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId

import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration.{Duration, SECONDS}
import scala.concurrent.{Future, Promise}

class StoreRepairerSuite extends IntegrationTestSuite:

  private val storeId = StoreId(PoolId.BootstrapPoolId, 0.toByte)

  /** Duplicated rather than shared with MissedUpdateFinalizationAction on purpose: a bug in the
    * key layout must not cancel itself out between the writer and the reader.
    */
  private def buildErrorKey(poolIndex: Byte, objectId: ObjectId): Key =
    val bb = ByteBuffer.allocate(17)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.put(poolIndex)
    bb.putLong(objectId.uuid.getMostSignificantBits)
    bb.putLong(objectId.uuid.getLeastSignificantBits)
    Key(bb.array())

  /** Duplicated rather than shared with MissedUpdateFinalizationAction for the same reason as
    * buildErrorKey: [missedTimestamp:8][storePointer...], big-endian.
    */
  private def buildErrorValue(missedTimestamp: HLCTimestamp, storePointer: Array[Byte]): Value =
    val bb = ByteBuffer.allocate(8 + storePointer.length)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.putLong(missedTimestamp.asLong)
    bb.put(storePointer)
    Value(bb.array())

  /** Records what the service asked the host to do, and lets the test make the store depart.
    * `duringRepair` runs before a repair completes, standing in for live traffic that lands
    * while the repair is underway.
    */
  private class RecordingTarget(hosted: List[StoreId],
                                duringRepair: ObjectId => Future[Unit] = _ => Future.unit)
      extends RepairTarget:
    val departed = new AtomicBoolean(false)
    @volatile var repairs: List[ObjectId] = Nil
    @volatile var deletions: List[(ObjectId, Seq[Byte])] = Nil

    def repairableStoreIds: List[StoreId] = if departed.get then Nil else hosted

    def isRepairable(storeId: StoreId): Boolean = repairableStoreIds.contains(storeId)

    def holdsTransaction(sid: StoreId, txid: TransactionId): Boolean = false
    def appliedStubs(sid: StoreId): Future[Set[TransactionId]] = Future.successful(Set())
    def dropAppliedStub(sid: StoreId, txid: TransactionId): Unit = ()

    def snapshot(sid: StoreId, pointer: ObjectPointer, p: Promise[RepairSnapshot]): Unit =
      p.success(RepairSnapshot.missing(pointer.id))

    def reconcile(sid: StoreId, snapshot: RepairSnapshot, os: ClientObjectState,
                  p: Promise[ReconcileResult]): Unit =
      synchronized { repairs = repairs :+ os.pointer.id }
      p.completeWith(duringRepair(os.pointer.id).map(_ => ReconcileResult(Set(), false, false)))

    def repairDelete(sid: StoreId, objectId: ObjectId,
                     storePointer: Array[Byte], completion: Promise[Unit]): Unit =
      synchronized { deletions = deletions :+ (objectId, storePointer.toSeq) }
      completion.success(())

  private def errorTree: Future[TieredKeyValueList] =
    client.getStoragePool(PoolId.BootstrapPoolId).map(_.errorTree)

  /** Seeds an errorTree entry directly. Provoking a real missed update is not deterministic in
    * TestNetwork, and the entry's shape is all StoreRepairer cares about.
    */
  private def seedErrorEntry(objectId: ObjectId, storePointer: Array[Byte],
                             missedTimestamp: HLCTimestamp = HLCTimestamp.Zero): Future[Unit] =
    errorTree.flatMap: tree =>
      val tx: Transaction = TestTransactions.replaySafe(client.newRepairTransaction())
      given Transaction = tx
      tx.replaySafe("test")
      tree.set(buildErrorKey(0.toByte, objectId), buildErrorValue(missedTimestamp, storePointer)).flatMap: _ =>
        tx.commit().map(_ => ())

  private def errorEntryExists(objectId: ObjectId): Future[Boolean] =
    errorTree.flatMap(_.get(buildErrorKey(0.toByte, objectId))).map(_.isDefined)

  /** Allocates a real object. AllocationFinalizationAction writes the allocationTree entry, so
    * nothing here has to fake one -- waiting for the transaction to settle is enough.
    */
  private def allocateObject(): Future[ObjectId] = allocateObjectPointer().map(_.id)

  private def allocateObjectPointer(): Future[KeyValueObjectPointer] =
    val f = client.transactUntilSuccessful: tx =>
      given Transaction = tx
      tx.replaySafe("test")
      for
        pool <- client.getStoragePool(PoolId.BootstrapPoolId)
        allocator = new PoolObjectAllocator(client, pool)
        ptr <- allocator.allocateKeyValueObject(Map(Key(Array[Byte](9)) -> Value(Array[Byte](9))))
      yield ptr
    for
      ptr <- f
      _ <- net.waitForTransactionsToComplete()
    yield ptr

  /** Commits a fresh update to the object and returns its new timestamp. No required revision,
    * as with TKVL inserts: the object's timestamp must advance anyway.
    */
  private def updateObject(ptr: KeyValueObjectPointer): Future[HLCTimestamp] =
    for
      _ <- client.transactUntilSuccessful: tx =>
             tx.replaySafe("test")
             tx.update(ptr, None, None, Nil, List(Insert(Key(Array[Byte](9)), Array[Byte](10))))
             Future.unit
      _ <- net.waitForTransactionsToComplete()
      os <- client.read(ptr)
    yield os.timestamp

  test("an entry whose object is in the allocation tree is repaired and the entry removed"):
    val target = RecordingTarget(List(storeId))
    val repairer = StoreRepairer(client, target)
    for
      ptr <- allocateObjectPointer()
      os <- client.read(ptr)
      _ <- seedErrorEntry(ptr.id, Array[Byte](1, 2, 3), os.timestamp)
      result <- repairer.scan(storeId, RepairPolicy.Default)
      stillThere <- errorEntryExists(ptr.id)
    yield
      target.repairs should contain (ptr.id)
      result.repaired shouldBe 1
      result.deferred shouldBe 0
      stillThere shouldBe false

  test("a store whose pool records it on another host is not scanned, so its entries stay for that host"):
    val target = RecordingTarget(List(storeId))
    for
      ptr <- allocateObjectPointer()
      os <- client.read(ptr)
      _ <- seedErrorEntry(ptr.id, Array[Byte](1, 2, 3), os.timestamp)
      elsewhere <- StoreRepairer(client, target, Some(HostId(new UUID(5, 5)))).scan(storeId, RepairPolicy.Default)
      keptForOwner <- errorEntryExists(ptr.id)
      repairsElsewhere = target.repairs
      owned <- StoreRepairer(client, target, Some(HostId.BootstrapHostId)).scan(storeId, RepairPolicy.Default)
      stillThere <- errorEntryExists(ptr.id)
    yield
      elsewhere.seen shouldBe 0
      repairsElsewhere shouldBe Nil
      keptForOwner shouldBe true
      owned.repaired shouldBe 1
      stillThere shouldBe false

  test("an entry is removed when its object was updated again after the miss"):
    val target = RecordingTarget(List(storeId))
    val repairer = StoreRepairer(client, target)
    for
      ptr <- allocateObjectPointer()
      missed <- client.read(ptr).map(_.timestamp)
      _ <- seedErrorEntry(ptr.id, Array[Byte](1), missed)
      // The update lands after the entry was written, so the entry's key timestamp predates it
      updated <- updateObject(ptr)
      result <- repairer.scan(storeId, RepairPolicy.Default)
      stillThere <- errorEntryExists(ptr.id)
    yield
      updated should be > missed
      result.repaired shouldBe 1
      result.deferred shouldBe 0
      stillThere shouldBe false

  test("an entry whose missed update is newer than the repaired state is left in place"):
    val target = RecordingTarget(List(storeId))
    val repairer = StoreRepairer(client, target)
    for
      ptr <- allocateObjectPointer()
      os <- client.read(ptr)
      _ <- seedErrorEntry(ptr.id, Array[Byte](1), HLCTimestamp.happensAfter(os.timestamp))
      result <- repairer.scan(storeId, RepairPolicy.Default)
      stillThere <- errorEntryExists(ptr.id)
    yield
      target.repairs should contain (ptr.id)
      result.repaired shouldBe 0
      result.deferred shouldBe 1
      stillThere shouldBe true

  test("an update missed while the repair is underway leaves the entry in place"):
    // The repair reads the object, then the store misses an update and the entry is rewritten
    // with that update's timestamp before the repair completes.
    @volatile var ptr: Option[KeyValueObjectPointer] = None
    val target = RecordingTarget(List(storeId), duringRepair = objectId =>
      for
        missed <- updateObject(ptr.get)
        _ <- seedErrorEntry(objectId, Array[Byte](1), missed)
      yield ())
    val repairer = StoreRepairer(client, target)
    for
      p <- allocateObjectPointer()
      _ = ptr = Some(p)
      os <- client.read(p)
      _ <- seedErrorEntry(p.id, Array[Byte](1), os.timestamp)
      _ <- repairer.scan(storeId, RepairPolicy.Default)
      stillThere <- errorEntryExists(p.id)
    yield
      target.repairs should contain (p.id)
      stillThere shouldBe true

  test("an aged entry with no allocation is deleted from the store and the tree"):
    val objectId = ObjectId(new UUID(7, 7))
    val target = RecordingTarget(List(storeId))
    val repairer = StoreRepairer(client, target)
    // minErrorEntryAgeForDeletion of zero makes a freshly seeded entry immediately eligible.
    val policy = RepairPolicy.Default.copy(minErrorEntryAgeForDeletion = Duration(0, SECONDS))
    for
      _ <- seedErrorEntry(objectId, Array[Byte](4, 5))
      result <- repairer.scan(storeId, policy)
      stillThere <- errorEntryExists(objectId)
    yield
      target.deletions shouldBe List((objectId, Seq[Byte](4, 5)))
      result.repaired shouldBe 1
      stillThere shouldBe false

  test("an empty store pointer reaches repairDelete as empty bytes"):
    val objectId = ObjectId(new UUID(8, 8))
    val target = RecordingTarget(List(storeId))
    val repairer = StoreRepairer(client, target)
    val policy = RepairPolicy.Default.copy(minErrorEntryAgeForDeletion = Duration(0, SECONDS))
    for
      _ <- seedErrorEntry(objectId, Array[Byte]())
      _ <- repairer.scan(storeId, policy)
    yield
      target.deletions shouldBe List((objectId, Seq.empty[Byte]))

  test("a fresh entry with no allocation is deferred, not deleted"):
    val objectId = ObjectId(new UUID(9, 9))
    val target = RecordingTarget(List(storeId))
    val repairer = StoreRepairer(client, target)
    for
      _ <- seedErrorEntry(objectId, Array[Byte](1))
      result <- repairer.scan(storeId, RepairPolicy.Default)
      stillThere <- errorEntryExists(objectId)
    yield
      target.deletions shouldBe Nil
      result.seen shouldBe 1
      result.deferred shouldBe 1
      result.repaired shouldBe 0
      stillThere shouldBe true

  test("a store that departs mid-scan stops receiving repair calls"):
    val objectId = ObjectId(new UUID(10, 10))
    val target = RecordingTarget(List(storeId))
    val repairer = StoreRepairer(client, target)
    val policy = RepairPolicy.Default.copy(minErrorEntryAgeForDeletion = Duration(0, SECONDS))
    for
      _ <- seedErrorEntry(objectId, Array[Byte](1))
      _ = target.departed.set(true)
      result <- repairer.scan(storeId, policy)
      stillThere <- errorEntryExists(objectId)
    yield
      target.deletions shouldBe Nil
      result.seen shouldBe 0
      stillThere shouldBe true

  test("a failed deletion leaves the error tree entry in place"):
    val objectId = ObjectId(new UUID(11, 11))
    val failing = new RepairTarget:
      def repairableStoreIds: List[StoreId] = List(storeId)
      def isRepairable(storeId: StoreId): Boolean = repairableStoreIds.contains(storeId)
      def holdsTransaction(sid: StoreId, txid: TransactionId): Boolean = false
      def appliedStubs(sid: StoreId): Future[Set[TransactionId]] = Future.successful(Set())
      def dropAppliedStub(sid: StoreId, txid: TransactionId): Unit = ()
      def snapshot(sid: StoreId, pointer: ObjectPointer, p: Promise[RepairSnapshot]): Unit =
        p.failure(StoreNotHosted(sid))
      def reconcile(sid: StoreId, snapshot: RepairSnapshot, os: ClientObjectState,
                    p: Promise[ReconcileResult]): Unit =
        p.failure(StoreNotHosted(sid))
      def repairDelete(sid: StoreId, oid: ObjectId,
                       sp: Array[Byte], completion: Promise[Unit]): Unit =
        completion.failure(StoreNotHosted(sid))
    val repairer = StoreRepairer(client, failing)
    val policy = RepairPolicy.Default.copy(minErrorEntryAgeForDeletion = Duration(0, SECONDS))
    for
      _ <- seedErrorEntry(objectId, Array[Byte](1))
      _ <- repairer.scan(storeId, policy)
      stillThere <- errorEntryExists(objectId)
    yield
      stillThere shouldBe true

  test("a failed update leaves the error tree entry in place"):
    val failing = new RepairTarget:
      def repairableStoreIds: List[StoreId] = List(storeId)
      def isRepairable(storeId: StoreId): Boolean = repairableStoreIds.contains(storeId)
      def holdsTransaction(sid: StoreId, txid: TransactionId): Boolean = false
      def appliedStubs(sid: StoreId): Future[Set[TransactionId]] = Future.successful(Set())
      def dropAppliedStub(sid: StoreId, txid: TransactionId): Unit = ()
      def snapshot(sid: StoreId, pointer: ObjectPointer, p: Promise[RepairSnapshot]): Unit =
        p.failure(StoreNotHosted(sid))
      def reconcile(sid: StoreId, snapshot: RepairSnapshot, os: ClientObjectState,
                    p: Promise[ReconcileResult]): Unit =
        p.failure(StoreNotHosted(sid))
      def repairDelete(sid: StoreId, oid: ObjectId,
                       sp: Array[Byte], completion: Promise[Unit]): Unit =
        completion.failure(StoreNotHosted(sid))
    val repairer = StoreRepairer(client, failing)
    for
      objectId <- allocateObject()
      _ <- seedErrorEntry(objectId, Array[Byte](2))
      _ <- repairer.scan(storeId, RepairPolicy.Default)
      stillThere <- errorEntryExists(objectId)
    yield
      stillThere shouldBe true

  // Idempotence only. This does NOT prove the recursion fix: in TestNetwork the store that
  // misses updates is index 2, so a re-tracking regression would write its entry outside the
  // [0,1) range this scan covers. Task 1's missedUpdateTrackingEnabled test is the direct proof
  // that repair transactions disable tracking; this one checks a drained range stays drained.
  test("a second scan after a drained tree finds nothing"):
    val objectId = ObjectId(new UUID(12, 12))
    val target = RecordingTarget(List(storeId))
    val repairer = StoreRepairer(client, target)
    val policy = RepairPolicy.Default.copy(minErrorEntryAgeForDeletion = Duration(0, SECONDS))
    for
      _ <- seedErrorEntry(objectId, Array[Byte](1))
      first <- repairer.scan(storeId, policy)
      _ <- net.waitForTransactionsToComplete()
      second <- repairer.scan(storeId, policy)
    yield
      first.seen shouldBe 1
      second.seen shouldBe 0

  test("a malformed errorTree entry does not hang the scan"):
    val objectId = ObjectId(new UUID(13, 13))
    val target = RecordingTarget(List(storeId))
    val repairer = StoreRepairer(client, target)
    val policy = RepairPolicy.Default.copy(minErrorEntryAgeForDeletion = Duration(0, SECONDS))
    for
      // Seed a too-short key (only 5 bytes instead of 17) directly into the errorTree
      tree <- errorTree
      tx = TestTransactions.replaySafe(client.newRepairTransaction())
      _ <- tree.set(Key(Array[Byte](0, 1, 2, 3, 4)), Value(Array[Byte]()))(using tx)
      _ <- tx.commit()
      // Seed a valid entry after the malformed one to prove the scan continued
      _ <- seedErrorEntry(objectId, Array[Byte](1))
      result <- repairer.scan(storeId, policy)
      validEntryStillThere <- errorEntryExists(objectId)
    yield
      // Both entries count as seen -- the malformed one is counted before it is decoded -- but
      // only the valid one is repaired. The point is that the scan completes at all: before the
      // decode was moved inside a Future, the throw escaped foreachInRange's failure handling
      // and left the walk's promise uncompleted, hanging this scan forever.
      target.deletions shouldBe List((objectId, Seq[Byte](1)))
      result.seen shouldBe 2
      result.repaired shouldBe 1
      validEntryStillThere shouldBe false

  // ---- Repair through the live host -----------------------------------------------------------
  //
  // TestNetwork can make a store miss a transaction outright: while its transaction messages are dropped it hears
  // nothing of the transaction, so the finalizer marks it in the errorTree. Dropping another store for the next
  // transaction makes the first one part of that transaction's write threshold, so it applies it.

  private def sid(i: Int): StoreId = net.stores(i).storeId

  /** The state store `i` has persisted for an object */
  private def stored(i: Int, id: ObjectId): Option[org.aspen_ddp.aspen.server.store.ObjectState] =
    net.stores(i).m.get(id)

  /** Whether store `i` holds a live copy. MapBackend keeps a deleted object's zero-refcount state; repairDelete drops
    * it
    */
  private def storeHasObject(i: Int, id: ObjectId): Boolean = stored(i, id).exists(_.metadata.refcount.count > 0)

  private def storeKeys(i: Int, ptr: KeyValueObjectPointer): Set[Key] =
    stored(i, ptr.id).flatMap(_.kvState).map(_.content.keySet).getOrElse(Set())

  private def storeRefcount(i: Int, ptr: DataObjectPointer): Option[ObjectRefcount] =
    stored(i, ptr.id).map(_.metadata.refcount)

  /** The keys of store `i`'s errorTree entries */
  private def errorEntries(i: Int): Future[List[Key]] =
    @volatile var keys: List[Key] = Nil
    val min = Key(Array[Byte](i.toByte))
    val max = Key(Array[Byte]((i + 1).toByte))
    for
      tree <- errorTree
      _ <- tree.foreachInRange(min, max, (_, key, _) => { synchronized { keys = key :: keys }; Future.unit })
    yield keys

  /** The keys of store `i`'s UnackedCommits entries */
  private def unackedEntries(i: Int): Future[List[Key]] =
    @volatile var keys: List[Key] = Nil
    val min = Key(Array[Byte](i.toByte))
    val max = Key(Array[Byte]((i + 1).toByte))
    for
      pool <- client.getStoragePool(PoolId.BootstrapPoolId)
      _ <- pool.unackedCommitsTree.foreachInRange(min, max,
             (_, key, _) => { synchronized { keys = key :: keys }; Future.unit })
    yield keys

  /** Deletes store `i`'s errorTree entries, so that only its UnackedCommits entries drive its repair */
  private def dropErrorEntries(i: Int): Future[Unit] =
    for
      keys <- errorEntries(i)
      tree <- errorTree
      _ <- keys.foldLeft(Future.unit): (f, key) =>
             f.flatMap: _ =>
               val tx = TestTransactions.replaySafe(client.newRepairTransaction())
               tree.delete(key)(using tx).flatMap(_ => pump(tx.commit(), deadline())).map(_ => ())
      _ <- settle()
      remaining <- errorEntries(i)
    yield
      remaining shouldBe empty

  /** Completes with `f`, draining the host's events while it is pending: the host only queues repair work */
  private def pump[T](f: Future[T], deadline: Long): Future[T] =
    if f.isCompleted then
      f
    else if System.nanoTime() > deadline then
      net.printTransactionStatus()
      Future.failed(new AssertionError("Timed out pumping events"))
    else
      Future { Thread.sleep(1); net.handleEvents() }.flatMap(_ => pump(f, deadline))

  private def deadline(): Long = System.nanoTime() + Duration(30, SECONDS).toNanos

  /** Completes once no host has transactions, draining the host's events meanwhile. Finalization actions, the
    * errorTree marks among them, need events drained after their last message
    */
  private def settle(until: Long = deadline()): Future[Unit] =
    net.handleEvents()
    if !net.hasTransactions then
      Future.unit
    else if System.nanoTime() > until then
      net.printTransactionStatus()
      Future.failed(new AssertionError("Transactions never completed"))
    else
      Future { Thread.sleep(1); net.handleEvents() }.flatMap(_ => settle(until))

  /** Commits a transaction built by `build` that store `i` misses outright: none of its messages reach the store.
    * The store is reported offline meanwhile, so it is not chosen as the transaction's designated leader
    */
  private def missedBy(i: Int)(build: Transaction => Unit): Future[Unit] = missedByTx(i)(build).map(_ => ())

  /** missedBy, completing with the missed transaction's id */
  private def missedByTx(i: Int)(build: Transaction => Unit): Future[TransactionId] =
    net.offlineStores = Set(sid(i))
    val tx = TestTransactions.replaySafe(client.newTransaction())
    build(tx)
    net.droppedTx = net.droppedTx + ((sid(i), tx.id))
    pump(tx.commit(), deadline()).transformWith: committed =>
      settle().transform: settled =>
        net.droppedTx = net.droppedTx - ((sid(i), tx.id))
        net.offlineStores = Set()
        committed.flatMap(_ => settled).map(_ => tx.id)

  /** missedByTx, standing in for a commit voter that crashed holding its vote and dropped it on recovery: its copies
    * lack the change, and it has the UnackedCommits entry the finalizer writes for a voter that sent no TxCommitted,
    * naming `objects`. A store that missed the transaction outright gets no entry: it never voted
    */
  private def missedByCrashedVoter(i: Int, objects: ObjectPointer*)(build: Transaction => Unit): Future[TransactionId] =
    val start = HLCTimestamp.now
    for
      txid <- missedByTx(i)(build)
      entry = MissedUnackedCommit(sid(i), txid, UnackedCommitEntry(start, objects.toList))
      _ <- pump(client.missedUpdateMarker.mark(Nil, List(entry)), deadline())
      _ <- settle()
    yield txid

  /** Scans store `i` through the live host until its errorTree and UnackedCommits ranges are empty */
  private def scanUntilQuiet(i: Int, repairer: StoreRepairer = StoreRepairer(client, net.smgr),
                             passes: Int = 20): Future[Unit] =
    for
      _ <- settle()
      _ <- pump(repairer.scan(sid(i), RepairPolicy.Default), deadline())
      _ <- settle()
      errors <- errorEntries(i)
      unacked <- unackedEntries(i)
      remaining = errors ++ unacked
      _ <- if remaining.isEmpty then Future.unit
           else if passes <= 1 then Future.failed(new AssertionError(s"store $i never went quiet: $remaining"))
           else scanUntilQuiet(i, repairer, passes - 1)
    yield ()

  private def scanAllUntilQuiet(): Future[Unit] =
    (0 until net.stores.size).foldLeft(Future.unit)((f, i) => f.flatMap(_ => scanUntilQuiet(i)))

  private val k0 = Key(Array[Byte](0))
  private val k1 = Key(Array[Byte](1))
  private val k2 = Key(Array[Byte](2))
  private val k3 = Key(Array[Byte](3))

  /** A KeyValue object every store holds, with `initial` as its content */
  private def sharedKVObject(initial: Map[Key, Value]): Future[KeyValueObjectPointer] =
    for
      ptr <- client.transactUntilSuccessful: tx =>
               given Transaction = tx
               tx.replaySafe("test")
               for
                 pool <- client.getStoragePool(PoolId.BootstrapPoolId)
                 p <- new PoolObjectAllocator(client, pool).allocateKeyValueObject(initial)
               yield p
      _ <- scanAllUntilQuiet()
    yield
      withClue("every store holds the object before the scenario: "):
        (0 until net.stores.size).foreach(i => storeKeys(i, ptr) shouldBe initial.keySet)
      ptr

  private def kvUpdate(ptr: KeyValueObjectPointer, ops: List[org.aspen_ddp.aspen.common.objects.KeyValueOperation])
                      (tx: Transaction): Unit =
    tx.update(ptr, None, None, Nil, ops)

  test("a store that missed a KV insert and then applied a later update to another key gets the missed key"):
    for
      kvPtr <- sharedKVObject(Map(k0 -> Value(Array[Byte](0))))
      _ <- missedBy(2)(kvUpdate(kvPtr, List(Insert(k1, Array[Byte](1)))))
      _ <- missedBy(0)(kvUpdate(kvPtr, List(Insert(k2, Array[Byte](2)))))
      marked <- errorEntries(2)
      _ = withClue("store 2 missed only the first insert: "):
            storeKeys(2, kvPtr) shouldBe Set(k0, k2)
            marked should not be empty
      _ <- scanUntilQuiet(2)
      remaining <- errorEntries(2)
    yield
      storeKeys(2, kvPtr) shouldBe Set(k0, k1, k2)
      remaining shouldBe empty

  test("a store that missed a KV delete and then applied a later update to another key loses the deleted key"):
    for
      kvPtr <- sharedKVObject(Map(k0 -> Value(Array[Byte](0)), k1 -> Value(Array[Byte](1))))
      _ <- missedBy(2)(kvUpdate(kvPtr, List(org.aspen_ddp.aspen.common.objects.Delete(k1))))
      _ <- missedBy(0)(kvUpdate(kvPtr, List(Insert(k2, Array[Byte](2)))))
      marked <- errorEntries(2)
      _ = withClue("store 2 missed only the delete: "):
            storeKeys(2, kvPtr) shouldBe Set(k0, k1, k2)
            marked should not be empty
      _ <- scanUntilQuiet(2)
      remaining <- errorEntries(2)
    yield
      storeKeys(2, kvPtr) should not contain k1
      storeKeys(2, kvPtr) shouldBe Set(k0, k2)
      remaining shouldBe empty

  test("a store that missed a refcount change on a DataObject and then applied a later data write gets the refcount"):
    for
      dataPtr <- client.transactUntilSuccessful: tx =>
                   given Transaction = tx
                   tx.replaySafe("test")
                   for
                     pool <- client.getStoragePool(PoolId.BootstrapPoolId)
                     p <- new PoolObjectAllocator(client, pool).allocateDataObject(DataBuffer(Array[Byte](1)))
                   yield p
      _ <- scanAllUntilQuiet()
      before <- client.read(dataPtr)
      expected = before.refcount.setCount(before.refcount.count + 1)
      _ <- missedBy(2)(_.setRefcount(dataPtr, before.refcount, expected))
      _ <- missedBy(0)(_.overwrite(dataPtr, before.revision, DataBuffer(Array[Byte](2))))
      marked <- errorEntries(2)
      _ = withClue("store 2 missed only the refcount change: "):
            storeRefcount(2, dataPtr) shouldBe Some(before.refcount)
            stored(2, dataPtr.id).map(_.data.getByteArray.toSeq) shouldBe Some(Seq[Byte](2))
            marked should not be empty
      _ <- scanUntilQuiet(2)
      remaining <- errorEntries(2)
    yield
      storeRefcount(2, dataPtr) shouldBe Some(expected)
      remaining shouldBe empty

  test("a store that missed a DataObject data write and then applied a later refcount change gets the missed data"):
    for
      dataPtr <- client.transactUntilSuccessful: tx =>
                   given Transaction = tx
                   tx.replaySafe("test")
                   for
                     pool <- client.getStoragePool(PoolId.BootstrapPoolId)
                     p <- new PoolObjectAllocator(client, pool).allocateDataObject(DataBuffer(Array[Byte](1)))
                   yield p
      _ <- scanAllUntilQuiet()
      before <- client.read(dataPtr)
      expected = before.refcount.setCount(before.refcount.count + 1)
      _ <- missedBy(2)(_.overwrite(dataPtr, before.revision, DataBuffer(Array[Byte](2))))
      _ <- missedBy(0)(_.setRefcount(dataPtr, before.refcount, expected))
      marked <- errorEntries(2)
      _ = withClue("store 2 missed only the data write: "):
            storeRefcount(2, dataPtr) shouldBe Some(expected)
            stored(2, dataPtr.id).map(_.data.getByteArray.toSeq) shouldBe Some(Seq[Byte](1))
            marked should not be empty
      _ <- scanUntilQuiet(2)
      after <- client.read(dataPtr)
      remaining <- errorEntries(2)
    yield
      stored(2, dataPtr.id).map(_.data.getByteArray.toSeq) shouldBe Some(Seq[Byte](2))
      stored(2, dataPtr.id).map(_.metadata.revision) shouldBe Some(after.revision)
      storeRefcount(2, dataPtr) shouldBe Some(expected)
      remaining shouldBe empty

  test("a commit applied between the snapshot and the install survives and is retried next pass"):
    // The repair reads the object before k3 is inserted, so the read lacks k3. The store applies the insert before
    // the install: only the snapshot check keeps the install from deleting it
    @volatile var target: Option[KeyValueObjectPointer] = None
    @volatile var paused = false
    val repairer = StoreRepairer(client, net.smgr, testingOnlyBeforeReconcile = (s, ptr) =>
      if !paused && s == sid(2) && target.exists(_.id == ptr.id) then
        paused = true
        missedBy(0)(kvUpdate(target.get, List(Insert(k3, Array[Byte](3)))))
      else
        Future.unit)
    for
      kvPtr <- sharedKVObject(Map(k0 -> Value(Array[Byte](0))))
      _ = target = Some(kvPtr)
      _ <- missedBy(2)(kvUpdate(kvPtr, List(Insert(k1, Array[Byte](1)))))
      _ <- pump(repairer.scan(sid(2), RepairPolicy.Default), deadline())
      _ <- settle()
      afterFirstPass <- errorEntries(2)
      keysAfterFirstPass = storeKeys(2, kvPtr)
      _ <- scanUntilQuiet(2, repairer)
      remaining <- errorEntries(2)
    yield
      paused shouldBe true
      keysAfterFirstPass should contain (k3)
      keysAfterFirstPass should contain (k1)
      afterFirstPass should not be empty
      storeKeys(2, kvPtr) shouldBe Set(k0, k1, k3)
      remaining shouldBe empty

  // ---- UnackedCommits entries, deferral and proofs ---------------------------------------------------------------

  /** The live host, with test seams: `live` stands in for transactions a store still holds live, `lockedKeys` and
    * `objectLocked` for locks every read of an object reports. Only a proof transaction gets past those locks: the
    * reconciliation that follows a committed proof ignores what the read reported on the proven parts
    */
  private class SeamTarget extends RepairTarget:
    @volatile var live: Set[(StoreId, TransactionId)] = Set()
    @volatile var lockedKeys: Map[ObjectId, Set[Key]] = Map()
    @volatile var objectLocked: Set[ObjectId] = Set()

    def repairableStoreIds: List[StoreId] = net.smgr.repairableStoreIds
    def isRepairable(storeId: StoreId): Boolean = net.smgr.isRepairable(storeId)
    def holdsTransaction(storeId: StoreId, txid: TransactionId): Boolean =
      live.contains((storeId, txid)) || net.smgr.holdsTransaction(storeId, txid)
    def snapshot(storeId: StoreId, pointer: ObjectPointer, p: Promise[RepairSnapshot]): Unit =
      net.smgr.snapshot(storeId, pointer, p)
    def reconcile(storeId: StoreId, snapshot: RepairSnapshot, read: ClientObjectState,
                  p: Promise[ReconcileResult]): Unit =
      val id = read.pointer.id
      read.readLocks = ReadLocks(read.readLocks.keys ++ lockedKeys.getOrElse(id, Set()),
                                 read.readLocks.objectLevel || objectLocked.contains(id))
      net.smgr.reconcile(storeId, snapshot, read, p)
    def repairDelete(storeId: StoreId, objectId: ObjectId, storePointer: Array[Byte], completion: Promise[Unit]): Unit =
      net.smgr.repairDelete(storeId, objectId, storePointer, completion)
    def appliedStubs(storeId: StoreId): Future[Set[TransactionId]] = net.smgr.appliedStubs(storeId)
    def dropAppliedStub(storeId: StoreId, txid: TransactionId): Unit = net.smgr.dropAppliedStub(storeId, txid)

  /** One scan of store `i`, then settle */
  private def pass(i: Int, repairer: StoreRepairer, deferral: DeferralState,
                   policy: RepairPolicy = RepairPolicy.Default): Future[ScanResult] =
    for
      _ <- settle()
      result <- pump(repairer.scan(sid(i), policy, deferral), deadline())
      _ <- settle()
    yield result

  private def passes(n: Int, i: Int, repairer: StoreRepairer, deferral: DeferralState): Future[Unit] =
    (1 to n).foldLeft(Future.unit)((f, _) => f.flatMap(_ => pass(i, repairer, deferral).map(_ => ())))

  private def sharedDataObject(): Future[DataObjectPointer] =
    for
      ptr <- client.transactUntilSuccessful: tx =>
               given Transaction = tx
               tx.replaySafe("test")
               for
                 pool <- client.getStoragePool(PoolId.BootstrapPoolId)
                 p <- new PoolObjectAllocator(client, pool).allocateDataObject(DataBuffer(Array[Byte](1)))
               yield p
      _ <- scanAllUntilQuiet()
    yield ptr

  test("a UnackedCommits entry for a transaction the store holds live is skipped"):
    val target = SeamTarget()
    val repairer = StoreRepairer(client, target)
    for
      kvPtr <- sharedKVObject(Map(k0 -> Value(Array[Byte](0)), k1 -> Value(Array[Byte](1))))
      txid <- missedByCrashedVoter(2, kvPtr)(kvUpdate(kvPtr, List(Delete(k1))))
      _ <- dropErrorEntries(2)
      before <- unackedEntries(2)
      _ = target.live += (sid(2) -> txid)
      result <- pass(2, repairer, DeferralState())
      after <- unackedEntries(2)
    yield
      before shouldBe List(UnackedCommitEntry.key(sid(2), txid))
      after shouldBe before
      result.unackedSeen shouldBe 1
      result.deferred shouldBe 1
      storeKeys(2, kvPtr) shouldBe Set(k0, k1)

  // ---- Applied stub cleanup ---------------------------------------------------------------------------------------

  private def storeFrontend(i: Int) = net.smgr.control.router.loaded(sid(i)).frontend

  /** Gives store `i` an applied stub for `txid`, durable */
  private def writeStub(i: Int, txid: TransactionId): Future[Unit] =
    for
      _ <- pump(net.smgr.control.router.onStore(sid(i))(_.frontend.recordAppliedStub(txid)(())), deadline())
      _ <- settle()
    yield ()

  private def hasStub(i: Int, txid: TransactionId): Boolean = storeFrontend(i).hasAppliedStub(txid)

  test("deleting an UnackedCommits entry drops the store's stub for it"):
    val repairer = StoreRepairer(client, SeamTarget())
    for
      kvPtr <- sharedKVObject(Map(k0 -> Value(Array[Byte](0)), k1 -> Value(Array[Byte](1))))
      txid <- missedByCrashedVoter(2, kvPtr)(kvUpdate(kvPtr, List(Delete(k1))))
      _ <- dropErrorEntries(2)
      _ <- writeStub(2, txid)
      _ <- scanUntilQuiet(2, repairer)
    yield
      hasStub(2, txid) shouldBe false

  // Ruling 8: a stub may be written before its entry is durable (Ruling 7), so one scan finding no entry is not enough
  test("an orphan stub survives one scan and is dropped by the second"):
    val repairer = StoreRepairer(client, SeamTarget())
    val deferral = DeferralState()
    val orphan = TransactionId(new UUID(77, 1))
    for
      _ <- writeStub(2, orphan)
      _ <- pass(2, repairer, deferral)
      afterFirst = hasStub(2, orphan)
      _ <- pass(2, repairer, deferral)
    yield
      afterFirst shouldBe true
      hasStub(2, orphan) shouldBe false

  test("a stub whose entry lands between two scans is kept"):
    val target = SeamTarget()
    val repairer = StoreRepairer(client, target)
    val deferral = DeferralState()
    val txid = TransactionId(new UUID(77, 3))
    for
      _ <- writeStub(2, txid)
      _ <- pass(2, repairer, deferral) // No entry yet: the stub is suspected
      // The planned entry lands. The store holds the transaction live, so the scan leaves the entry alone
      _ <- pump(client.missedUpdateMarker.mark(Nil,
             List(MissedUnackedCommit(sid(2), txid, UnackedCommitEntry(HLCTimestamp.now, Nil)))), deadline())
      _ <- settle()
      _ = target.live += (sid(2) -> txid)
      _ <- pass(2, repairer, deferral)
      afterSecond = hasStub(2, txid)
      // The entry gone again: the suspicion restarts, so one more scan keeps the stub
      pool <- client.getStoragePool(PoolId.BootstrapPoolId)
      tx = TestTransactions.replaySafe(client.newRepairTransaction())
      _ <- pool.unackedCommitsTree.delete(UnackedCommitEntry.key(sid(2), txid))(using tx)
      _ <- pump(tx.commit(), deadline())
      _ = target.live = Set()
      _ <- pass(2, repairer, deferral)
    yield
      afterSecond shouldBe true
      hasStub(2, txid) shouldBe true

  test("a stub whose entry is still present is kept"):
    val target = SeamTarget()
    val repairer = StoreRepairer(client, target)
    for
      kvPtr <- sharedKVObject(Map(k0 -> Value(Array[Byte](0)), k1 -> Value(Array[Byte](1))))
      txid <- missedByCrashedVoter(2, kvPtr)(kvUpdate(kvPtr, List(Delete(k1))))
      _ <- dropErrorEntries(2)
      _ <- writeStub(2, txid)
      _ = target.live += (sid(2) -> txid)
      _ <- pass(2, repairer, DeferralState())
      entries <- unackedEntries(2)
    yield
      entries should not be empty
      hasStub(2, txid) shouldBe true

  test("a stub written during the walk is kept, and dropped two scans later"):
    val late = TransactionId(new UUID(77, 2))
    var written: Future[Unit] = Future.unit
    // The hook runs inside the walk, when the scan asks whether the store holds the walked entry's transaction
    val target = new SeamTarget:
      override def holdsTransaction(storeId: StoreId, txid: TransactionId): Boolean =
        written = writeStub(2, late)
        super.holdsTransaction(storeId, txid)
    val repairer = StoreRepairer(client, target)
    for
      kvPtr <- sharedKVObject(Map(k0 -> Value(Array[Byte](0)), k1 -> Value(Array[Byte](1))))
      txid <- missedByCrashedVoter(2, kvPtr)(kvUpdate(kvPtr, List(Delete(k1))))
      _ <- dropErrorEntries(2)
      _ = target.live += (sid(2) -> txid)
      _ <- pass(2, repairer, DeferralState())
      _ <- written
      keptDuringScan = hasStub(2, late)
      _ = target.live = Set()
      next = StoreRepairer(client, SeamTarget())
      deferral = DeferralState()
      _ <- pass(2, next, deferral) // First scan to see the stub: suspected only (Ruling 8)
      keptByNext = hasStub(2, late)
      _ <- pass(2, next, deferral)
    yield
      keptDuringScan shouldBe true
      keptByNext shouldBe true
      hasStub(2, late) shouldBe false

  test("a UnackedCommits entry is reconciled and deleted once the store no longer holds it"):
    val target = SeamTarget()
    val repairer = StoreRepairer(client, target)
    for
      kvPtr <- sharedKVObject(Map(k0 -> Value(Array[Byte](0)), k1 -> Value(Array[Byte](1))))
      txid <- missedByCrashedVoter(2, kvPtr)(kvUpdate(kvPtr, List(Delete(k1))))
      _ <- dropErrorEntries(2)
      _ = target.live += (sid(2) -> txid)
      _ <- pass(2, repairer, DeferralState())
      held <- unackedEntries(2)
      _ = target.live = Set()
      _ <- scanUntilQuiet(2, repairer)
      remaining <- unackedEntries(2)
    yield
      held should not be empty
      remaining shouldBe empty
      storeKeys(2, kvPtr) should not contain k1
      storeKeys(2, kvPtr) shouldBe Set(k0)

  test("a UnackedCommits entry for a deleted object deletes it from the store"):
    for
      dataPtr <- sharedDataObject()
      before <- client.read(dataPtr)
      _ <- missedByCrashedVoter(2, dataPtr)(_.setRefcount(dataPtr, before.refcount, before.refcount.setCount(0)))
      _ <- dropErrorEntries(2)
      _ = withClue("store 2 missed the deletion: "):
            storeHasObject(2, dataPtr.id) shouldBe true
            storeHasObject(0, dataPtr.id) shouldBe false
      marked <- unackedEntries(2)
      _ <- pass(2, StoreRepairer(client, net.smgr), DeferralState(), noDeletionAge)
      remaining <- unackedEntries(2)
    yield
      marked should not be empty
      storeHasObject(2, dataPtr.id) shouldBe false
      stored(2, dataPtr.id) shouldBe None
      remaining shouldBe empty

  /** A policy treating any entry whose object has no allocation as a deletion, however young */
  private val noDeletionAge: RepairPolicy = RepairPolicy.Default.copy(minErrorEntryAgeForDeletion = Duration(0, SECONDS))

  test("a young UnackedCommits entry whose object has no allocation yet does not delete the live object"):
    // An allocation's RefcountUpdate is traceless, so a store missing it gets an entry before
    // AllocationFinalizationAction records the allocation: absent from the allocation tree is not yet a deletion
    val oid = ObjectId(new UUID(7, 70))
    val ptr = DataObjectPointer(oid, PoolId.BootstrapPoolId, Array[Byte]())
    val txid = TransactionId(new UUID(7, 71))
    net.stores(2).bootstrapAllocate(oid, org.aspen_ddp.aspen.common.objects.ObjectType.Data,
      org.aspen_ddp.aspen.common.objects.Metadata(org.aspen_ddp.aspen.common.objects.ObjectRevision(txid),
        ObjectRefcount(1, 1), HLCTimestamp.now), DataBuffer(Array[Byte](3)))
    for
      pool <- client.getStoragePool(PoolId.BootstrapPoolId)
      tx = TestTransactions.replaySafe(client.newRepairTransaction())
      _ <- pool.unackedCommitsTree.set(UnackedCommitEntry.key(sid(2), txid),
             Value(UnackedCommitEntry(HLCTimestamp.now, List(ptr)).encode))(using tx)
      _ <- pump(tx.commit(), deadline())
      result <- pass(2, StoreRepairer(client, net.smgr), DeferralState())
      remaining <- unackedEntries(2)
    yield
      storeHasObject(2, oid) shouldBe true
      remaining shouldBe List(UnackedCommitEntry.key(sid(2), txid))
      result.unackedSeen shouldBe 1
      result.deferred shouldBe 1

  test("a store holding none of its entries' transactions live clears every UnackedCommits entry in one scan"):
    for
      kvPtr <- sharedKVObject(Map(k0 -> Value(Array[Byte](0)), k1 -> Value(Array[Byte](1))))
      dataPtr <- sharedDataObject()
      before <- client.read(dataPtr)
      expected = before.refcount.setCount(before.refcount.count + 1)
      _ <- missedByCrashedVoter(2, kvPtr)(kvUpdate(kvPtr, List(Delete(k1))))
      _ <- missedByCrashedVoter(2, dataPtr)(_.setRefcount(dataPtr, before.refcount, expected))
      _ <- dropErrorEntries(2)
      marked <- unackedEntries(2)
      result <- pass(2, StoreRepairer(client, net.smgr), DeferralState())
      remaining <- unackedEntries(2)
    yield
      marked.size shouldBe 2
      result.unackedSeen shouldBe 2
      remaining shouldBe empty
      storeKeys(2, kvPtr) shouldBe Set(k0)
      storeRefcount(2, dataPtr) shouldBe Some(expected)

  // A rebuild drops the store's CRL, so it holds none of its entries' transactions live, and rebuilds its copies from
  // current reads: no hook in rebuild is needed, the first scan passes every entry at once and changes nothing. The
  // rebuild's end state is stood in for by store 2 restarted with no CRL state and a peer's copies (Replication, so
  // the slices are the same)
  test("after a rebuild, the first scan clears the store's UnackedCommits entries and changes nothing"):
    for
      kvPtr <- sharedKVObject(Map(k0 -> Value(Array[Byte](0)), k1 -> Value(Array[Byte](1))))
      dataPtr <- sharedDataObject()
      before <- client.read(dataPtr)
      expected = before.refcount.setCount(before.refcount.count + 1)
      _ <- missedByCrashedVoter(2, kvPtr)(kvUpdate(kvPtr, List(Delete(k1))))
      _ <- missedByCrashedVoter(2, dataPtr)(_.setRefcount(dataPtr, before.refcount, expected))
      _ <- dropErrorEntries(2)
      marked <- unackedEntries(2)
      _ = net.stores(2).m = net.stores(0).m.map: (id, os) =>
            id -> new org.aspen_ddp.aspen.server.store.ObjectState(id, os.metadata, os.objectType, os.data)
      _ <- pump(net.restartStore(net.stores(2)), deadline())
      _ <- settle()
      rebuilt = Map(kvPtr.id -> stored(2, kvPtr.id).map(_.metadata), dataPtr.id -> stored(2, dataPtr.id).map(_.metadata))
      result <- pass(2, StoreRepairer(client, net.smgr), DeferralState())
      remaining <- unackedEntries(2)
    yield
      withClue("the rebuilt store holds the transactions' changes: "):
        storeKeys(2, kvPtr) shouldBe Set(k0)
        storeRefcount(2, dataPtr) shouldBe Some(expected)
      marked.size shouldBe 2
      result.unackedSeen shouldBe 2
      result.repaired shouldBe 2
      remaining shouldBe empty
      Map(kvPtr.id -> stored(2, kvPtr.id).map(_.metadata), dataPtr.id -> stored(2, dataPtr.id).map(_.metadata)) shouldBe
        rebuilt
      storeKeys(2, kvPtr) shouldBe Set(k0)

  /** An object every store holds with k0, k1 and k2, and a transaction store 2 missed that deletes k1 and k2. Every
    * read reports k2 locked. Completes with the object and the deferral key of the missed transaction's entry
    */
  private def lockedKeyScenario(target: SeamTarget): Future[(KeyValueObjectPointer, Key)] =
    for
      kvPtr <- sharedKVObject(Map(k0 -> Value(Array[Byte](0)), k1 -> Value(Array[Byte](1)),
                                  k2 -> Value(Array[Byte](2))))
      txid <- missedByCrashedVoter(2, kvPtr)(kvUpdate(kvPtr, List(Delete(k1), Delete(k2))))
      _ <- dropErrorEntries(2)
      _ = target.lockedKeys += kvPtr.id -> Set(k2)
    yield
      withClue("store 2 missed the deletes: "):
        storeKeys(2, kvPtr) shouldBe Set(k0, k1, k2)
      (kvPtr, StoreRepairer.unackedDeferralKey(UnackedCommitEntry.key(sid(2), txid), kvPtr.id))

  test("one permanently locked key is deferred while every other key of the object is reconciled at once"):
    val target = SeamTarget()
    val repairer = StoreRepairer(client, target)
    val deferral = DeferralState()
    for
      (kvPtr, entryKey) <- lockedKeyScenario(target)
      result <- pass(2, repairer, deferral)
      remaining <- unackedEntries(2)
    yield
      storeKeys(2, kvPtr) shouldBe Set(k0, k2)
      deferral.deferredKeys(entryKey) shouldBe Some(Set(k2))
      result.deferred shouldBe 1
      remaining should not be empty

  test("a key deferred for proofAfterDeferrals passes is proven and reconciled"):
    val target = SeamTarget()
    val repairer = StoreRepairer(client, target)
    val deferral = DeferralState()
    val n = RepairPolicy.Default.proofAfterDeferrals
    for
      (kvPtr, entryKey) <- lockedKeyScenario(target)
      _ <- passes(n - 1, 2, repairer, deferral)
      keysBeforeProof = storeKeys(2, kvPtr)
      entriesBeforeProof <- unackedEntries(2)
      _ <- pass(2, repairer, deferral)
      remaining <- unackedEntries(2)
    yield
      keysBeforeProof shouldBe Set(k0, k2)
      entriesBeforeProof should not be empty
      storeKeys(2, kvPtr) shouldBe Set(k0)
      (0 until net.stores.size).foreach(i => storeKeys(i, kvPtr) shouldBe Set(k0))
      remaining shouldBe empty
      deferral.deferredKeys(entryKey) shouldBe None

  test("an aborted proof leaves the key deferred for a later pass"):
    // On the proof's pass, k2 is written between the read and the proof: the proof requires k2 absent, as the read
    // saw it, and aborts
    val target = SeamTarget()
    @volatile var writeK2: Option[KeyValueObjectPointer] = None
    @volatile var proofs: List[ReadLocks] = Nil
    val repairer = StoreRepairer(client, target, testingOnlyOnProof = (_, _, proven) => proofs = proofs :+ proven,
                                 testingOnlyBeforeReconcile = (s, ptr) =>
      writeK2.filter(p => s == sid(2) && p.id == ptr.id) match
        case Some(p) =>
          writeK2 = None
          val tx = TestTransactions.replaySafe(client.newTransaction())
          kvUpdate(p, List(Insert(k2, Array[Byte](7))))(tx)
          pump(tx.commit(), deadline()).flatMap(_ => settle())
        case None => Future.unit)
    val deferral = DeferralState()
    val n = RepairPolicy.Default.proofAfterDeferrals
    for
      (kvPtr, entryKey) <- lockedKeyScenario(target)
      _ <- passes(n - 1, 2, repairer, deferral)
      proofsBefore = proofs
      _ = writeK2 = Some(kvPtr)
      _ <- pass(2, repairer, deferral)
      proofsOnAbortPass = proofs
      afterAbort <- unackedEntries(2)
      keysAfterAbort = storeKeys(2, kvPtr)
      deferredAfterAbort = deferral.deferredKeys(entryKey)
      _ <- pass(2, repairer, deferral)
      remaining <- unackedEntries(2)
    yield
      writeK2 shouldBe None
      proofsBefore shouldBe empty
      withClue("the n-th pass ran a proof, which aborted and proved nothing: "):
        proofsOnAbortPass shouldBe List(ReadLocks(Set(), false))
      afterAbort should not be empty
      deferredAfterAbort shouldBe Some(Set(k2))
      keysAfterAbort shouldBe Set(k0, k2)
      (0 until net.stores.size).foreach(i => storeKeys(i, kvPtr) shouldBe Set(k0, k2))
      remaining shouldBe empty

  test("a refcount under an object lock is deferred and then proven"):
    val target = SeamTarget()
    val repairer = StoreRepairer(client, target)
    val deferral = DeferralState()
    val n = RepairPolicy.Default.proofAfterDeferrals
    for
      dataPtr <- sharedDataObject()
      before <- client.read(dataPtr)
      expected = before.refcount.setCount(before.refcount.count + 1)
      _ <- missedByCrashedVoter(2, dataPtr)(_.setRefcount(dataPtr, before.refcount, expected))
      _ <- dropErrorEntries(2)
      _ = target.objectLocked += dataPtr.id
      first <- pass(2, repairer, deferral)
      _ <- passes(n - 2, 2, repairer, deferral)
      refcountBeforeProof = storeRefcount(2, dataPtr)
      entriesBeforeProof <- unackedEntries(2)
      _ <- pass(2, repairer, deferral)
      remaining <- unackedEntries(2)
    yield
      first.deferred shouldBe 1
      refcountBeforeProof shouldBe Some(before.refcount)
      entriesBeforeProof should not be empty
      storeRefcount(2, dataPtr) shouldBe Some(expected)
      remaining shouldBe empty

  test("a future dated entry is never eligible for deletion repair"):
    Future.successful:
      val now = HLCTimestamp(1_756_742_400_000L << 16)
      val future = HLCTimestamp((1_756_742_400_000L + 5_000) << 16)
      StoreRepairer.errorEntryMayBeDeleted(future, now, Duration(60, SECONDS)).shouldBe(false)

  test("a freshly written entry is not yet eligible for deletion repair"):
    Future.successful:
      val now = HLCTimestamp(1_756_742_400_000L << 16)
      val recent = HLCTimestamp((1_756_742_400_000L - 5_000) << 16)
      StoreRepairer.errorEntryMayBeDeleted(recent, now, Duration(60, SECONDS)).shouldBe(false)

  test("an aged entry is eligible for deletion repair"):
    Future.successful:
      val now = HLCTimestamp(1_756_742_400_000L << 16)
      val old = HLCTimestamp((1_756_742_400_000L - 120_000) << 16)
      StoreRepairer.errorEntryMayBeDeleted(old, now, Duration(60, SECONDS)).shouldBe(true)

  test("a deletion repair deletes its entry under the entry's KeyRevision"):
    val objectId = ObjectId(new UUID(7, 8))
    val errorKey = buildErrorKey(0.toByte, objectId)
    val target = RecordingTarget(List(storeId))
    val repairer = StoreRepairer(client, target)
    val policy = RepairPolicy.Default.copy(minErrorEntryAgeForDeletion = Duration(0, SECONDS))
    for
      _ <- seedErrorEntry(objectId, Array[Byte](4, 5))
      tree <- errorTree
      entry <- tree.get(errorKey)
      audit = new PinAudit(client)
      _ <- repairer.scan(storeId, policy)
      _ = audit.stop()
      stillThere <- errorEntryExists(objectId)
    yield
      stillThere shouldBe false
      audit.built should not be empty
      audit.unpinned shouldBe empty
      val keyReqs = audit.built.flatMap(_.description.requirements).collect:
        case kvu: KeyValueUpdate => kvu.requirements
      keyReqs.flatten should contain (KeyValueUpdate.KeyRevision(errorKey, entry.get.revision))

  // Deleting an entry can empty its node, which then absorbs its right neighbor or is left empty. Either needs the
  // upper-tier finalization action the tree's own delete registers, or the tier above keeps an entry for a deleted
  // node and the tree never shrinks.
  test("deleting entries that join tier-0 nodes removes their upper-tier entries"):
    val target = RecordingTarget(List(storeId))
    val repairer = StoreRepairer(client, target)
    val policy = RepairPolicy.Default.copy(minErrorEntryAgeForDeletion = Duration(0, SECONDS))
    // Each entry carries a 400 KiB store pointer, so the 1 MiB tier-0 nodes hold two each
    val objectIds = (1 to 8).toList.map(i => ObjectId(new UUID(11, i)))
    val bigPointer = Array.fill[Byte](400 * 1024)(3)

    for
      _ <- objectIds.foldLeft(Future.unit)((f, oid) => f.flatMap(_ => seedErrorEntry(oid, bigPointer)))
      _ <- net.waitForTransactionsToComplete()
      tree <- errorTree
      (seededTier, _, _) <- tree.rootManager.getFreshRootNode()
      // One scan: its walk resumes past the joins its own deletes cause
      _ <- repairer.scan(storeId, policy)
      _ <- net.waitForTransactionsToComplete()
      present <- Future.sequence(objectIds.map(errorEntryExists))
      (tier, _, _) <- tree.rootManager.getFreshRootNode()
    yield
      seededTier should be >= 1
      present should not contain true
      target.deletions.map(_._1) shouldBe objectIds
      tier shouldBe 0

  test("repair proofs of a key and of a refcount are marked replay-safe"):
    val target = SeamTarget()
    val repairer = StoreRepairer(client, target)
    val deferral = DeferralState()
    val n = RepairPolicy.Default.proofAfterDeferrals
    for
      (kvPtr, _) <- lockedKeyScenario(target)
      _ <- passes(n - 1, 2, repairer, deferral)
      audit = new PinAudit(client)
      _ <- pass(2, repairer, deferral)
      _ = audit.stop()
      dataPtr <- sharedDataObject()
      before <- client.read(dataPtr)
      audit2 = new PinAudit(client)
      proven <- RepairProofs.proveRefcount(client, dataPtr, before.refcount)
      _ = audit2.stop()
    yield
      audit.unpinned shouldBe empty
      audit.replaySafeWith("repair proof changes nothing") should not be empty
      proven shouldBe true
      audit2.unpinned shouldBe empty
      audit2.replaySafeWith("repair proof changes nothing").size shouldBe 1

  test("a repair proof of the object level is marked replay-safe"):
    for
      kvPtr <- sharedKVObject(Map(k0 -> Value(Array[Byte](0))))
      read <- client.read(kvPtr)
      audit = new PinAudit(client)
      proven <- RepairProofs.proveObjectLevel(client, read)
      _ = audit.stop()
    yield
      proven shouldBe true
      audit.unpinned shouldBe empty
      audit.replaySafeWith("repair proof changes nothing").size shouldBe 1
