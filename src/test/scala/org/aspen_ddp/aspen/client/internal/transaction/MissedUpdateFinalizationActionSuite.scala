package org.aspen_ddp.aspen.client.internal.transaction

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.client.internal.transaction.MissedUpdateMarker.MissedUpdate
import org.aspen_ddp.aspen.common.{HLCTimestamp, Radicle}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Key, ObjectId, ObjectPointer, ObjectRevision, Value}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{DataUpdate, DataUpdateOperation, TransactionDescription, TransactionId}

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

  atest("an action with no marks to write completes"):
    val ptr = pointerWith(Array[Byte](1))
    val txd = txdFor(ptr)
    val fa = new MissedUpdateFinalizationAction(client, txd)
    fa.updateCommitErrors(txd.allDataStores.map(_ -> Nil).toMap)
    fa.markMissedUpdates()

    fa.complete.map(_ => succeed)
