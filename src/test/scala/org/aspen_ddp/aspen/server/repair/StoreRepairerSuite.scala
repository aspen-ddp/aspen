package org.aspen_ddp.aspen.server.repair

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.{ObjectState as ClientObjectState, Transaction}
import org.aspen_ddp.aspen.client.internal.allocation.PoolObjectAllocator
import org.aspen_ddp.aspen.client.tkvl.TieredKeyValueList
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Insert, Key, KeyValueObjectPointer, ObjectId,
                                           ObjectPointer, ObjectRefcount, Value}
import org.aspen_ddp.aspen.server.store.{ReconcileResult, RepairSnapshot}
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
      val tx: Transaction = client.newRepairTransaction()
      given Transaction = tx
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
      tx = client.newRepairTransaction()
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
  private def missedBy(i: Int)(build: Transaction => Unit): Future[Unit] =
    net.offlineStores = Set(sid(i))
    val tx = client.newTransaction()
    build(tx)
    net.droppedTx = net.droppedTx + ((sid(i), tx.id))
    pump(tx.commit(), deadline()).transformWith: committed =>
      settle().transform: settled =>
        net.droppedTx = net.droppedTx - ((sid(i), tx.id))
        net.offlineStores = Set()
        committed.flatMap(_ => settled)

  /** Scans store `i` through the live host until its errorTree range is empty */
  private def scanUntilQuiet(i: Int, repairer: StoreRepairer = StoreRepairer(client, net.smgr),
                             passes: Int = 20): Future[Unit] =
    for
      _ <- settle()
      _ <- pump(repairer.scan(sid(i), RepairPolicy.Default), deadline())
      _ <- settle()
      remaining <- errorEntries(i)
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
