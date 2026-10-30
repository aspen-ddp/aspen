package org.aspen_ddp.aspen.client.internal.transaction

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.Transaction
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
import scala.concurrent.{ExecutionContext, Future}

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

  /** Runs markMissedUpdate itself for a store of `poolId` and returns whether the transaction it
    * committed still tracks missed updates, which pins limitTracking to after the errorTree write.
    */
  private def trackingAfterMarkMissedUpdate(poolId: PoolId): Future[Boolean] =
    val ptr = pointerWith(Array[Byte](1))
    var tracking: Option[Boolean] = None
    val fa = new MissedUpdateFinalizationAction(client, txdFor(ptr)):
      override private[aspen] def commitErrorTreeTransaction(tx: Transaction): Future[Unit] =
        tracking = Some(tx.missedUpdateTrackingEnabled)
        super.commitErrorTreeTransaction(tx)
    fa.markMissedUpdate(StoreId(poolId, 0.toByte), ptr.id, Array[Byte](1)).map: _ =>
      tracking.get

  atest("markMissedUpdate disables tracking for a bootstrap pool entry"):
    trackingAfterMarkMissedUpdate(Radicle.poolId).map(_ shouldBe false)

  atest("markMissedUpdate keeps tracking for an ordinary pool entry"):
    for
      poolId <- client.createNewStoragePool("mark-pool", Replication(1, 1), None, RocksDBConfig(),
                                            StorageDeviceSetId.BootstrapStorageDeviceSetId, 0L)
      enabled <- trackingAfterMarkMissedUpdate(poolId)
    yield enabled shouldBe true
