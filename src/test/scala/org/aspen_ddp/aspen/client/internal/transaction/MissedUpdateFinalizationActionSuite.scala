package org.aspen_ddp.aspen.client.internal.transaction

import org.aspen_ddp.aspen.{IntegrationTestSuite, PinAudit}
import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.client.internal.transaction.MissedUpdateMarker.{MissedUnackedCommit, MissedUpdate}
import org.aspen_ddp.aspen.common.{HLCTimestamp, Radicle}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Key, KeyValueObjectPointer, ObjectId, ObjectPointer, ObjectRefcount, ObjectRevision, Value}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{KeyValueUpdate, RefcountUpdate, DataUpdate, DataUpdateOperation, RevisionLock, SharedRevisionLock, TransactionDescription, TransactionId}

import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.metadata.StorageDeviceSetId
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.server.store.backend.RocksDBConfig

import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future, Promise}

/** Tests for MissedUpdateFinalizationAction's storePointer persistence.
  *
  * The trigger (markMissedUpdates() called directly) is synthetic because none of the three ways
  * to make a store miss a commit work in TestNetwork: leaving store2 unloaded makes Host reply
  * StoreNotFound, which BaseReadDriver answers by immediately re-reading, and TestNetwork's
  * synchronous re-entrant loop turns that into unbounded recursion; offlineStores is only
  * reachable via the private closeStore; and backend commit errors are discarded by
  * Tx.commitComplete. Everything downstream of the trigger is real — real TKVL, real transaction,
  * real Paxos across three MapBackends.
  */
class MissedUpdateFinalizationActionSuite extends IntegrationTestSuite:

  given ExecutionContext = executionContext

  /** Build the 17-byte errorTree key for a store and object: [poolIndex:1][objectUuid:16].
    *
    * Deliberately written out here rather than shared with the production encoder, so that a
    * bug in the key layout cannot cancel itself out between writer and reader.
    */
  private def buildErrorKey(storeId: StoreId, objectId: ObjectId): Key =
    val keyBytes = new Array[Byte](17)
    val bb = ByteBuffer.wrap(keyBytes)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.put(storeId.poolIndex)
    bb.putLong(objectId.uuid.getMostSignificantBits)
    bb.putLong(objectId.uuid.getLeastSignificantBits)
    Key(keyBytes)

  private val missedTimestamp = HLCTimestamp(1000L)

  /** Splits an errorTree value into [missedTimestamp:8][storePointer...], written out here for
    * the same reason as buildErrorKey.
    */
  private def decodeErrorValue(value: Value): (HLCTimestamp, Seq[Byte]) =
    val bb = ByteBuffer.wrap(value.bytes)
    bb.order(ByteOrder.BIG_ENDIAN)
    (HLCTimestamp(bb.getLong()), value.bytes.toSeq.drop(8))

  private def pointerWith(storePointer: Array[Byte]): DataObjectPointer =
    DataObjectPointer(ObjectId(UUID.randomUUID()), Radicle.poolId, storePointer)

  /** One DataUpdate requirement per pointer -- allReferencedObjectsSet is derived from the
    * requirements, so this is what makes the pointers visible to the action.
    */
  private def txdFor(ptrs: ObjectPointer*): TransactionDescription =
    TransactionDescription(
      transactionId = TransactionId(UUID.randomUUID()),
      startTimestamp = missedTimestamp,
      primaryObject = ptrs.head,
      designatedLeaderUID = 0.toByte,
      requirements = ptrs.toList.map: p =>
        DataUpdate(p, ObjectRevision(TransactionId(UUID.randomUUID())), DataUpdateOperation.Overwrite),
      finalizationActions = Nil,
      primaryObjectIDA = net.ida,
      poolIDAMap = Map(ptrs.head.poolId -> net.ida)
    )

  atest("errorTree value carries the missed timestamp and the object's storePointer bytes"):
    val storePointer = Array[Byte](1, 2, 3, 4)
    val ptr = pointerWith(storePointer)

    // commitErrors is left untouched, so every store takes the `case None` branch and the
    // pointer -- and its storePointer -- comes straight from allHostedObjects.
    val fa = new MissedUpdateFinalizationAction(client, txdFor(ptr))
    fa.markMissedUpdates()

    for
      _ <- fa.complete
      pool <- client.getStoragePool(Radicle.poolId)
      entry0 <- pool.errorTree.get(buildErrorKey(net.storeId0, ptr.id))
      entry1 <- pool.errorTree.get(buildErrorKey(net.storeId1, ptr.id))
      entry2 <- pool.errorTree.get(buildErrorKey(net.storeId2, ptr.id))
    yield
      entry0.isDefined shouldBe true
      decodeErrorValue(entry0.get.value) shouldBe ((missedTimestamp, storePointer.toSeq))
      entry1.isDefined shouldBe true
      decodeErrorValue(entry1.get.value) shouldBe ((missedTimestamp, storePointer.toSeq))
      entry2.isDefined shouldBe true
      decodeErrorValue(entry2.get.value) shouldBe ((missedTimestamp, storePointer.toSeq))

  atest("storePointer is recovered by id when commitErrors reports the object"):
    val storePointer = Array[Byte](5, 6, 7, 8)
    val ptr = pointerWith(storePointer)

    // Naming a single store keeps commitErrors smaller than allDataStores, so the all-clear
    // branch in updateCommitErrors does not fire and complete the promise before any write.
    val fa = new MissedUpdateFinalizationAction(client, txdFor(ptr))
    fa.updateCommitErrors(Map(net.storeId2 -> List(ptr.id)))
    fa.markMissedUpdates()

    for
      _ <- fa.complete
      pool <- client.getStoragePool(Radicle.poolId)
      entry <- pool.errorTree.get(buildErrorKey(net.storeId2, ptr.id))
    yield
      entry.isDefined shouldBe true
      decodeErrorValue(entry.get.value) shouldBe ((missedTimestamp, storePointer.toSeq))

  atest("a commitErrors id absent from the txd still records an entry"):
    val ptr = pointerWith(Array[Byte](9, 10))
    val unrelatedId = ObjectId(UUID.randomUUID())

    // The record must survive even though no pointer can be found for the id. Dropping it
    // would lose the fact that a store missed an update, which no repair pass would revisit.
    val fa = new MissedUpdateFinalizationAction(client, txdFor(ptr))
    fa.updateCommitErrors(Map(net.storeId1 -> List(unrelatedId)))
    fa.markMissedUpdates()

    for
      _ <- fa.complete
      pool <- client.getStoragePool(Radicle.poolId)
      entry <- pool.errorTree.get(buildErrorKey(net.storeId1, unrelatedId))
    yield
      entry.isDefined shouldBe true
      decodeErrorValue(entry.get.value) shouldBe ((missedTimestamp, ObjectPointer.EmptyArray.toSeq))

  atest("an empty storePointer round-trips after the timestamp"):
    // Guards against a fix that writes an encoded pointer or a length prefix rather than the
    // raw bytes -- both would show up here as bytes after the timestamp.
    val ptr = pointerWith(ObjectPointer.EmptyArray)

    val fa = new MissedUpdateFinalizationAction(client, txdFor(ptr))
    fa.markMissedUpdates()

    for
      _ <- fa.complete
      pool <- client.getStoragePool(Radicle.poolId)
      entry <- pool.errorTree.get(buildErrorKey(net.storeId0, ptr.id))
    yield
      entry.isDefined shouldBe true
      decodeErrorValue(entry.get.value) shouldBe ((missedTimestamp, ObjectPointer.EmptyArray.toSeq))

  atest("marking the same object twice is idempotent"):
    val storePointer = Array[Byte](11, 12, 13)
    val ptr = pointerWith(storePointer)
    val txd = txdFor(ptr)

    // Two separate action instances rather than calling markMissedUpdates() twice on one.
    // An action's completionPromise resolves once and stays resolved, so a second call on the
    // same instance would leave `complete` already satisfied and the assertion would race the
    // second write instead of waiting for it.
    val first = new MissedUpdateFinalizationAction(client, txd)
    first.markMissedUpdates()

    for
      _ <- first.complete
      second = new MissedUpdateFinalizationAction(client, txd)
      _ = second.markMissedUpdates()
      _ <- second.complete
      pool <- client.getStoragePool(Radicle.poolId)
      entry <- pool.errorTree.get(buildErrorKey(net.storeId0, ptr.id))
    yield
      // errorTree.set is an unconditional last-write-wins replace and storePointer is a stable
      // property of the pointer, so the re-mark writes identical bytes over the first entry.
      entry.isDefined shouldBe true
      decodeErrorValue(entry.get.value) shouldBe ((missedTimestamp, storePointer.toSeq))

  /** Stages an errorTree write for `poolId` on a fresh transaction, applies limitTracking and
    * returns whether tracking is still enabled. The transaction is aborted afterward.
    */
  private def trackingAfterErrorWrite(poolId: PoolId): Future[Boolean] =
    for
      pool <- client.getStoragePool(poolId)
      tx = client.newTransaction()
      _ <- pool.errorTree.set(Key(Array[Byte](1)), Value(Array[Byte](2)))(using tx)
      _ = MissedUpdateFinalizationAction.limitTracking(poolId, tx)
      enabled = tx.missedUpdateTrackingEnabled
      _ = tx.abort(new Exception("test complete"))
    yield enabled

  atest("an ErrorTree write for the bootstrap pool disables tracking"):
    trackingAfterErrorWrite(Radicle.poolId).map(_ shouldBe false)

  atest("an ErrorTree write for the initial metadata pool keeps tracking"):
    // Its trees live in the bootstrap pool, so the write cannot loop back on itself
    trackingAfterErrorWrite(PoolId.InitialMetadataPoolId).map(_ shouldBe true)

  atest("an ErrorTree write for an ordinary pool keeps tracking"):
    for
      poolId <- client.createNewStoragePool("tracking-pool", Replication(1, 1), None, RocksDBConfig(),
                                            StorageDeviceSetId.BootstrapStorageDeviceSetId, 0L)
      enabled <- trackingAfterErrorWrite(poolId)
    yield enabled shouldBe true

  private def missed(store: StoreId, objectId: ObjectId = ObjectId(UUID.randomUUID()),
                     timestamp: HLCTimestamp = missedTimestamp,
                     storePointer: Array[Byte] = Array[Byte](1)): MissedUpdate =
    MissedUpdate(store, objectId, timestamp, storePointer)

  /** A marker whose write transactions wait for `release` if one is given, counting them and whether they track */
  private class ObservedMarker(release: Option[Future[Unit]] = None, maxEntries: Int = 256)
    extends MissedUpdateMarker(client, maxEntries):
    @volatile var commits = 0
    @volatile var tracking: Option[Boolean] = None
    val firstCommit: Promise[Unit] = Promise()

    override private[aspen] def commitErrorTreeTransaction(tx: Transaction): Future[Unit] =
      commits += 1
      tracking = Some(tx.missedUpdateTrackingEnabled)
      firstCommit.trySuccess(())
      release.getOrElse(Future.unit).flatMap(_ => super.commitErrorTreeTransaction(tx))

  /** Writes an entry for a store of `poolId` and returns whether the transaction that committed it
    * still tracks missed updates, which pins limitTracking to after the errorTree write.
    */
  private def trackingAfterMark(poolId: PoolId): Future[Boolean] =
    val marker = new ObservedMarker
    marker.mark(List(missed(StoreId(poolId, 0.toByte)))).map(_ => marker.tracking.get)

  atest("a mark disables tracking for a bootstrap pool entry"):
    trackingAfterMark(Radicle.poolId).map(_ shouldBe false)

  atest("a mark keeps tracking for an ordinary pool entry"):
    for
      poolId <- client.createNewStoragePool("mark-pool", Replication(1, 1), None, RocksDBConfig(),
                                            StorageDeviceSetId.BootstrapStorageDeviceSetId, 0L)
      enabled <- trackingAfterMark(poolId)
    yield enabled shouldBe true

  atest("all of a transaction's entries are written in one transaction"):
    val marker = new ObservedMarker
    val objects = List.fill(2)(ObjectId(UUID.randomUUID()))
    val stores = List(net.storeId0, net.storeId1, net.storeId2)

    for
      _ <- marker.mark(for s <- stores; o <- objects yield missed(s, o))
      pool <- client.getStoragePool(Radicle.poolId)
      entries <- Future.sequence(for s <- stores; o <- objects yield pool.errorTree.get(buildErrorKey(s, o)))
    yield
      marker.commits shouldBe 1
      entries.forall(_.isDefined) shouldBe true

  atest("entries arriving during a write wait for one later write"):
    val release = Promise[Unit]()
    val marker = new ObservedMarker(Some(release.future))
    val first = missed(net.storeId0)
    val second = missed(net.storeId1)
    val third = missed(net.storeId2)

    val firstWritten = marker.mark(List(first))

    for
      _ <- marker.firstCommit.future
      laterWritten = List(marker.mark(List(second)), marker.mark(List(third)))
      queuedDuringWrite = marker.queued
      commitsDuringWrite = marker.commits
      _ = release.success(())
      _ <- firstWritten
      _ <- Future.sequence(laterWritten)
      pool <- client.getStoragePool(Radicle.poolId)
      entries <- Future.sequence(List(first, second, third).map(u => pool.errorTree.get(buildErrorKey(u.store, u.objectId))))
    yield
      queuedDuringWrite shouldBe 2
      commitsDuringWrite shouldBe 1
      marker.commits shouldBe 2
      marker.queued shouldBe 0
      entries.forall(_.isDefined) shouldBe true

  atest("waiting entries for the same store and object keep the newest miss, in either order"):
    val release = Promise[Unit]()
    val marker = new ObservedMarker(Some(release.future))
    val older = HLCTimestamp(1000L << 16)
    val newer = HLCTimestamp(2000L << 16)
    val rising = ObjectId(UUID.randomUUID())
    val falling = ObjectId(UUID.randomUUID())

    val blocker = marker.mark(List(missed(net.storeId1)))

    for
      _ <- marker.firstCommit.future
      written = List(marker.mark(List(missed(net.storeId0, rising, older))),
                     marker.mark(List(missed(net.storeId0, rising, newer, Array[Byte](7)))),
                     marker.mark(List(missed(net.storeId0, falling, newer, Array[Byte](7)))),
                     marker.mark(List(missed(net.storeId0, falling, older))))
      queuedDuringWrite = marker.queued
      _ = release.success(())
      _ <- blocker
      _ <- Future.sequence(written)
      pool <- client.getStoragePool(Radicle.poolId)
      risingEntry <- pool.errorTree.get(buildErrorKey(net.storeId0, rising))
      fallingEntry <- pool.errorTree.get(buildErrorKey(net.storeId0, falling))
    yield
      queuedDuringWrite shouldBe 2
      decodeErrorValue(risingEntry.get.value) shouldBe ((newer, Seq[Byte](7)))
      decodeErrorValue(fallingEntry.get.value) shouldBe ((newer, Seq[Byte](7)))

  atest("a backlog larger than a batch takes several writes"):
    val marker = new ObservedMarker(maxEntries = 2)
    val updates = List.fill(5)(missed(net.storeId0))

    for
      _ <- marker.mark(updates)
      pool <- client.getStoragePool(Radicle.poolId)
      entries <- Future.sequence(updates.map(u => pool.errorTree.get(buildErrorKey(u.store, u.objectId))))
    yield
      marker.commits shouldBe 3
      entries.forall(_.isDefined) shouldBe true

  atest("a batch holds at most maxEntriesPerBatch entries of the two trees together, half for each when both wait"):
    // Three of each kind with a cap of two: one of each per write, so three writes. Capping each tree separately took
    // two of each per write, and two writes
    val marker = new ObservedMarker(maxEntries = 2)
    val updates = List.fill(3)(missed(net.storeId0))
    val entry = UnackedCommitEntry(missedTimestamp, List(pointerWith(Array[Byte](3))))
    val txids = List.fill(3)(TransactionId(UUID.randomUUID()))
    val unacked = txids.map(MissedUnackedCommit(net.storeId0, _, entry))

    for
      _ <- marker.mark(updates, unacked)
      pool <- client.getStoragePool(Radicle.poolId)
      entries <- Future.sequence(updates.map(u => pool.errorTree.get(buildErrorKey(u.store, u.objectId))))
      written <- unackedEntries(Radicle.poolId, txids*)
    yield
      marker.commits shouldBe 3
      entries.forall(_.isDefined) shouldBe true
      written.size shouldBe 3

  private def revision = ObjectRevision(TransactionId(UUID.randomUUID()))

  /** Which of these stores have an entry for the object */
  private def markedStores(ptr: ObjectPointer, stores: List[StoreId]): Future[List[StoreId]] =
    for
      pool <- client.getStoragePool(Radicle.poolId)
      found <- Future.sequence(stores.map(s => pool.errorTree.get(buildErrorKey(s, ptr.id)).map(e => (s, e.isDefined))))
    yield found.collect { case (s, true) => s }

  /** An update of `updated` that holds locks on `shared` and `exclusive`, and on `updated` too */
  private def txdWithLocks(updated: ObjectPointer, shared: ObjectPointer, exclusive: ObjectPointer) =
    txdFor(updated).copy(requirements =
      List(DataUpdate(updated, revision, DataUpdateOperation.Overwrite),
           SharedRevisionLock(updated, revision), SharedRevisionLock(shared, revision), RevisionLock(exclusive, revision)))

  test("only objects locked and never changed by a transaction are lock-only"):
    val List(updated, shared, exclusive) = List.fill(3)(pointerWith(Array[Byte]()))
    txdWithLocks(updated, shared, exclusive).lockOnlyObjects shouldBe Set(shared.id, exclusive.id)

  atest("a store that never replied is not marked for objects the transaction only locks"):
    val List(updated, shared, exclusive) = List.fill(3)(pointerWith(Array[Byte]()))
    val fa = new MissedUpdateFinalizationAction(client, txdWithLocks(updated, shared, exclusive))
    fa.markMissedUpdates()
    val stores = List(net.storeId0, net.storeId1, net.storeId2)
    for
      _ <- fa.complete
      marked <- markedStores(updated, stores)
      markedShared <- markedStores(shared, stores)
      markedExclusive <- markedStores(exclusive, stores)
    yield
      marked shouldBe stores
      markedShared shouldBe Nil
      markedExclusive shouldBe Nil

  atest("a commit error on an object the transaction only locks is not marked"):
    val List(updated, shared, exclusive) = List.fill(3)(pointerWith(Array[Byte]()))
    val fa = new MissedUpdateFinalizationAction(client, txdWithLocks(updated, shared, exclusive))
    fa.updateCommitErrors(Map(net.storeId2 -> List(shared.id, exclusive.id)))
    fa.markMissedUpdates()
    for
      _ <- fa.complete
      markedShared <- markedStores(shared, List(net.storeId2))
      markedExclusive <- markedStores(exclusive, List(net.storeId2))
    yield
      markedShared shouldBe Nil
      markedExclusive shouldBe Nil

  test("commit errors only on objects the transaction only locks need no marks"):
    val List(updated, shared, exclusive) = List.fill(3)(pointerWith(Array[Byte]()))
    val txd = txdWithLocks(updated, shared, exclusive)
    val fa = new MissedUpdateFinalizationAction(client, txd)
    fa.updateCommitErrors(txd.allDataStores.map(_ -> List(shared.id)).toMap)
    fa.complete.isCompleted shouldBe true

  atest("an action with no marks to write completes"):
    val ptr = pointerWith(Array[Byte](1))
    val txd = txdFor(ptr)
    val fa = new MissedUpdateFinalizationAction(client, txd)
    fa.updateCommitErrors(txd.allDataStores.map(_ -> Nil).toMap)
    fa.markMissedUpdates()

    fa.complete.map(_ => succeed)

  // UnackedCommits entries

  /** The pool's UnackedCommits entries for `txids`. Only those: the system's own transactions write entries too
    * when a store misses one under load
    */
  private def unackedEntries(poolId: PoolId, txids: TransactionId*): Future[Map[Key, UnackedCommitEntry]] =
    for
      pool <- client.getStoragePool(poolId)
      all <- pool.unackedCommitsTree.foldLeft(Map[Key, UnackedCommitEntry]()): (acc, contents) =>
        acc ++ contents.map((k, vs) => k -> UnackedCommitEntry.decode(vs.value.bytes))
    yield all.filter((k, _) => txids.contains(UnackedCommitEntry.decodeTransactionId(k)))

  private def refcountTxd(ptr: ObjectPointer): TransactionDescription =
    txdFor(ptr).copy(requirements = List(RefcountUpdate(ptr, ObjectRefcount(0, 1), ObjectRefcount(1, 0))))

  private def kvTxd(ptr: KeyValueObjectPointer, deletes: Boolean): TransactionDescription =
    txdFor(ptr).copy(requirements =
      List(KeyValueUpdate(ptr, None, None, List(KeyValueUpdate.MayExist(Key(Array[Byte](1)))), deletes)))

  private def runMark(txd: TransactionDescription, commitErrors: Map[StoreId, List[ObjectId]]): Future[Unit] =
    val fa = new MissedUpdateFinalizationAction(client, txd)
    fa.updateCommitErrors(commitErrors)
    fa.markMissedUpdates()
    fa.complete

  atest("a store that sent no TxCommitted for a refcount change gets a UnackedCommits entry"):
    val ptr = pointerWith(Array[Byte](4, 2))
    val txd = refcountTxd(ptr)
    for
      _ <- runMark(txd, Map(net.storeId0 -> Nil, net.storeId1 -> Nil))
      entries <- unackedEntries(Radicle.poolId, txd.transactionId)
    yield
      entries shouldBe Map(UnackedCommitEntry.key(net.storeId2, txd.transactionId) ->
        UnackedCommitEntry(txd.startTimestamp, List(ptr)))

  /** Marks `txd` as a driver that counted `voters` reports them, with every store in `commitErrors` having replied */
  private def runMarkWithVoters(txd: TransactionDescription, voters: Set[StoreId],
                                commitErrors: Map[StoreId, List[ObjectId]]): Future[Unit] =
    val fa = new MissedUpdateFinalizationAction(client, txd)
    fa.commitVoters(Some(voters))
    fa.updateCommitErrors(commitErrors)
    fa.markMissedUpdates()
    fa.complete

  atest("a store that never voted commit gets an ErrorTree mark but no UnackedCommits entry"):
    // store2 was offline throughout: it holds no commit vote to recover, so repair through the ErrorTree is enough
    val ptr = pointerWith(Array[Byte](4, 3))
    val txd = refcountTxd(ptr)
    for
      _ <- runMarkWithVoters(txd, Set(net.storeId0, net.storeId1), Map(net.storeId0 -> Nil, net.storeId1 -> Nil))
      entries <- unackedEntries(Radicle.poolId, txd.transactionId)
      marked <- markedStores(ptr, List(net.storeId0, net.storeId1, net.storeId2))
    yield
      entries shouldBe empty
      marked shouldBe List(net.storeId2)

  atest("a commit voter that sent no TxCommitted gets a UnackedCommits entry"):
    // store1 voted commit and then crashed before applying: on recovery it looks for this entry
    val ptr = pointerWith(Array[Byte](4, 4))
    val txd = refcountTxd(ptr)
    for
      _ <- runMarkWithVoters(txd, Set(net.storeId0, net.storeId1), Map(net.storeId0 -> Nil))
      entries <- unackedEntries(Radicle.poolId, txd.transactionId)
    yield
      entries.keySet shouldBe Set(UnackedCommitEntry.key(net.storeId1, txd.transactionId))

  atest("a finalizer that wrote entries for stores 1 and 2 reports handedOffStores == {1, 2}"):
    val ptr = pointerWith(Array[Byte](4, 6))
    val txd = refcountTxd(ptr)
    val fa = new MissedUpdateFinalizationAction(client, txd)
    fa.commitVoters(Some(Set(net.storeId0, net.storeId1, net.storeId2)))
    fa.updateCommitErrors(Map(net.storeId0 -> Nil))
    fa.handedOffStores shouldBe empty
    fa.markMissedUpdates()
    for _ <- fa.complete yield
      fa.handedOffStores shouldBe Set(net.storeId1, net.storeId2)

  atest("a cancelled marker that wrote nothing owes nothing"):
    val ptr = pointerWith(Array[Byte](4, 7))
    val txd = refcountTxd(ptr)
    val fa = new MissedUpdateFinalizationAction(client, txd)
    fa.updateCommitErrors(Map(net.storeId0 -> Nil))
    fa.cancel()
    fa.markMissedUpdates()
    fa.handedOffStores shouldBe empty

  atest("a voter without TxCommitted gets a UnackedCommits entry for a data overwrite too"):
    val ptr = pointerWith(Array[Byte](4, 5))
    val txd = txdFor(ptr).copy(requirements =
      List(DataUpdate(ptr, ObjectRevision(TransactionId(UUID.randomUUID())), DataUpdateOperation.Overwrite)))
    for
      _ <- runMarkWithVoters(txd, Set(net.storeId0, net.storeId1), Map(net.storeId0 -> Nil))
      entries <- unackedEntries(Radicle.poolId, txd.transactionId)
    yield
      entries shouldBe Map(UnackedCommitEntry.key(net.storeId1, txd.transactionId) ->
        UnackedCommitEntry(txd.startTimestamp, List(ptr)))

  atest("a flagged KV delete without a required revision gets an entry, and so does an insert-only update"):
    val delPtr = KeyValueObjectPointer(ObjectId(UUID.randomUUID()), Radicle.poolId)
    val insPtr = KeyValueObjectPointer(ObjectId(UUID.randomUUID()), Radicle.poolId)
    val deleteTx = kvTxd(delPtr, deletes = true)
    val insertTx = kvTxd(insPtr, deletes = false)
    val seen = Map(net.storeId0 -> Nil, net.storeId1 -> Nil)
    for
      _ <- runMark(deleteTx, seen)
      _ <- runMark(insertTx, seen)
      entries <- unackedEntries(Radicle.poolId, deleteTx.transactionId, insertTx.transactionId)
    yield
      entries.keySet shouldBe Set(UnackedCommitEntry.key(net.storeId2, deleteTx.transactionId),
                                  UnackedCommitEntry.key(net.storeId2, insertTx.transactionId))

  atest("a store that sent TxCommitted with object errors gets no UnackedCommits entry"):
    val ptr = pointerWith(Array[Byte](1))
    val txd = refcountTxd(ptr)
    for
      _ <- runMark(txd, Map(net.storeId0 -> Nil, net.storeId1 -> Nil, net.storeId2 -> List(ptr.id)))
      entries <- unackedEntries(Radicle.poolId, txd.transactionId)
    yield entries.isEmpty shouldBe true

  atest("a transaction spanning two pools writes each store's entry to its own pool's tree, naming only its objects"):
    for
      poolB <- client.createNewStoragePool("unacked-pool-b", Replication(1, 1), None, RocksDBConfig(),
                                           StorageDeviceSetId.BootstrapStorageDeviceSetId, 0L)
      ptrA = pointerWith(Array[Byte](1))
      ptrB = DataObjectPointer(ObjectId(UUID.randomUUID()), poolB, Array[Byte](2))
      txd = txdFor(ptrA).copy(
        requirements = List(RefcountUpdate(ptrA, ObjectRefcount(0, 1), ObjectRefcount(1, 0)),
                            RefcountUpdate(ptrB, ObjectRefcount(0, 1), ObjectRefcount(1, 0))),
        poolIDAMap = Map(Radicle.poolId -> net.ida, poolB -> Replication(1, 1)))
      _ <- runMark(txd, Map.empty)
      a <- unackedEntries(Radicle.poolId, txd.transactionId)
      b <- unackedEntries(poolB, txd.transactionId)
    yield
      a.values.toList shouldBe List.fill(3)(UnackedCommitEntry(txd.startTimestamp, List(ptrA)))
      b shouldBe Map(UnackedCommitEntry.key(StoreId(poolB, 0.toByte), txd.transactionId) ->
        UnackedCommitEntry(txd.startTimestamp, List(ptrB)))

  atest("UnackedCommits entries are written in the same transaction as the pool's ErrorTree marks"):
    val marker = new ObservedMarker
    val txid = TransactionId(UUID.randomUUID())
    val entry = UnackedCommitEntry(missedTimestamp, List(pointerWith(Array[Byte](3))))
    for
      _ <- marker.mark(List(missed(net.storeId0), missed(net.storeId1)),
                       List(MissedUnackedCommit(net.storeId0, txid, entry), MissedUnackedCommit(net.storeId1, txid, entry)))
      entries <- unackedEntries(Radicle.poolId, txid)
    yield
      marker.commits shouldBe 1
      entries.keySet shouldBe Set(UnackedCommitEntry.key(net.storeId0, txid), UnackedCommitEntry.key(net.storeId1, txid))

  test("a unacked commit entry round-trips and its key names the transaction"):
    val ptr = pointerWith(Array[Byte](9, 9, 9))
    val entry = UnackedCommitEntry(missedTimestamp, List(ptr, pointerWith(Array[Byte]())))
    val txid = TransactionId(UUID.randomUUID())
    UnackedCommitEntry.decode(entry.encode) shouldBe entry
    UnackedCommitEntry.decodeTransactionId(UnackedCommitEntry.key(net.storeId1, txid)) shouldBe txid

  atest("a mark batch is blind and marked replay-safe"):
    val marker = new ObservedMarker
    val txid = TransactionId(UUID.randomUUID())
    val entry = UnackedCommitEntry(missedTimestamp, List(pointerWith(Array[Byte](3))))
    val update = missed(net.storeId0)
    val errorKey = buildErrorKey(update.store, update.objectId)
    for
      _ <- marker.mark(List(update))
      audit = new PinAudit(client)
      // A rewrite of a held key: blind, so it carries no KeyRevision a concurrent mark could fail
      _ <- marker.mark(List(update.copy(missedTimestamp = HLCTimestamp(2000L))),
                       List(MissedUnackedCommit(net.storeId0, txid, entry)))
      _ = audit.stop()
    yield
      audit.unpinned shouldBe empty
      audit.replaySafeWith("missed-update marks are idempotent") should not be empty
      val keyReqs = audit.built.flatMap(_.description.requirements).collect:
        case kvu: KeyValueUpdate => kvu.requirements
      keyReqs.flatten.exists:
        case KeyValueUpdate.KeyRevision(k, _) => k == errorKey
        case KeyValueUpdate.DoesNotExist(k) => k == errorKey
        case _ => false
      .shouldBe(false)

  // Cancellation (a late resolution, or TxFinalized from another driver): nothing is written once cancelled

  /** Runs tasks only when told to. `runAll(ignoreCancel = true)` also runs cancelled ones: a timer that fires anyway */
  private class ManualTasks extends org.aspen_ddp.aspen.common.util.BackgroundTaskManager(ExecutionContext.parasitic):
    private final class Task(fn: () => Unit) extends org.aspen_ddp.aspen.common.util.BackgroundTaskManager.ScheduledTask:
      @volatile var cancelled = false
      def cancel(): Unit = cancelled = true
      def run(ignoreCancel: Boolean): Unit = if ignoreCancel || !cancelled then fn()
    private var tasks: List[Task] = Nil
    def scheduled: Int = synchronized(tasks.size)
    def allCancelled: Boolean = synchronized(tasks.forall(_.cancelled))
    override def schedule(delay: scala.concurrent.duration.Duration)(fn: => Unit) = synchronized:
      val t = new Task(() => fn)
      tasks = t :: tasks
      t
    def runAll(ignoreCancel: Boolean = false): Unit =
      val ts = synchronized { val t = tasks; tasks = Nil; t }
      ts.reverse.foreach(_.run(ignoreCancel))

  private class RecordingMarker(txd: TransactionDescription, tasks: ManualTasks)
    extends MissedUpdateFinalizationAction(client, txd):
    @volatile var writes = 0
    override protected def scheduler: org.aspen_ddp.aspen.common.util.BackgroundTaskManager = tasks
    override protected def writeMarks(errors: List[MissedUpdate], unacked: List[MissedUnackedCommit]): Future[Unit] =
      writes += 1
      super.writeMarks(errors, unacked)

  atest("an executed marker writes when its timer fires"):
    val tasks = new ManualTasks
    val fa = new RecordingMarker(refcountTxd(pointerWith(Array[Byte](7, 1))), tasks)
    fa.execute()
    tasks.scheduled shouldBe 1
    tasks.runAll()
    fa.complete.map(_ => fa.writes shouldBe 1)

  test("a cancel before the timer fires writes nothing, even if the timer fires anyway"):
    val tasks = new ManualTasks
    val fa = new RecordingMarker(refcountTxd(pointerWith(Array[Byte](7, 2))), tasks)
    fa.execute()
    fa.cancel()
    tasks.allCancelled shouldBe true
    tasks.runAll(ignoreCancel = true)
    fa.writes shouldBe 0

  // The order a cancel racing the Finalizer's deferred execute produced: the cancel found nothing scheduled to cancel
  test("a cancel that precedes the deferred execute writes nothing"):
    val tasks = new ManualTasks
    val fa = new RecordingMarker(refcountTxd(pointerWith(Array[Byte](7, 3))), tasks)
    fa.cancel()
    fa.execute()
    tasks.scheduled shouldBe 0
    fa.markMissedUpdates()
    fa.writes shouldBe 0

  private class PendingWriteMarker(txd: TransactionDescription) extends MissedUpdateFinalizationAction(client, txd):
    val write: scala.concurrent.Promise[Unit] = scala.concurrent.Promise()
    override protected def writeMarks(errors: List[MissedUpdate], unacked: List[MissedUnackedCommit]): Future[Unit] =
      write.future

  atest("a late TxCommitted making every store clean cannot complete the marker before its writes land"):
    val txd = refcountTxd(pointerWith(Array[Byte](7, 4)))
    val fa = new PendingWriteMarker(txd)
    fa.commitVoters(Some(Set(net.storeId0, net.storeId1, net.storeId2)))
    fa.updateCommitErrors(Map(net.storeId0 -> Nil, net.storeId1 -> Nil))
    fa.handedOffStores shouldBe empty
    fa.markMissedUpdates()
    // The planned set is known from the hand-off, before the write lands (Ruling 7)
    fa.handedOffStores shouldBe Set(net.storeId2)

    // The store whose entry is being written now reports in
    fa.updateCommitErrors(Map(net.storeId0 -> Nil, net.storeId1 -> Nil, net.storeId2 -> Nil))
    fa.complete.isCompleted shouldBe false

    fa.write.success(())
    for _ <- fa.complete yield
      fa.handedOffStores shouldBe Set(net.storeId2)

  // R-N7 variant 2: a mark of several pools whose write fails part way (one pool's batch committed, another gave up)
  // fails as a whole. The planned set still lists every store, cancelled or not
  for cancelAfter <- List(false, true) do
    atest(s"a partially failed mark still lists every planned store (cancelled after hand-off: $cancelAfter)"):
      val txd = refcountTxd(pointerWith(Array[Byte](7, 5)))
      val fa = new PendingWriteMarker(txd)
      fa.commitVoters(Some(Set(net.storeId0, net.storeId1, net.storeId2)))
      fa.updateCommitErrors(Map(net.storeId0 -> Nil))
      fa.markMissedUpdates()
      if cancelAfter then fa.cancel()
      fa.write.failure(new Exception("one pool's batch gave up"))
      for _ <- fa.write.future.failed yield
        fa.handedOffStores shouldBe Set(net.storeId1, net.storeId2)
        // A failed write never completes the action: TxFinalized is not sent for entries that did not land
        fa.complete.isCompleted shouldBe false
