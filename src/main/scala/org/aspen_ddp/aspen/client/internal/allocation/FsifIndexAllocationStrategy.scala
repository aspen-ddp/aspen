package org.aspen_ddp.aspen.client.internal.allocation

import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState, ObjectAllocator, RegisteredTypeFactory, StoragePool, StoragePoolAllocationStrategy, Transaction}
import org.aspen_ddp.aspen.common.metadata.StoragePoolState
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Insert, Key, KeyValueObjectPointer, ObjectId, ObjectPointer, Value}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate
import org.aspen_ddp.aspen.common.util.{int2byte, uuid2byte}

import scribe.Logging

import java.nio.ByteBuffer
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Random

/** The allocation strategy of FSIF pools. Each pointer's storePointer is a 4-byte external index in
  * `[0, indexCount)`, and the allocated indices are tracked in a bitmap KeyValue object (see FsifIndexBitmap) outside
  * the pool.
  *
  * A transaction reads the bitmap once per pool, on its first allocation or at commit. Its allocations take free
  * bits from one randomly chosen segment until that segment is full. At commit, one update to the bitmap sets the
  * bits of its staged allocations and clears those of its deletions, guarded by the revision of every segment key it
  * changes, so a concurrent change to one of those segments aborts the transaction.
  *
  * A transaction that called `deferAllocationRelease()` leaves its deletions' bits set and instead enrolls a system
  * FsifReleaseIndicesTask that clears them later. The task exists iff the delete commits, so no index leaks or is
  * freed twice, but the indices stay unavailable until it runs.
  */
object FsifIndexAllocationStrategy extends StoragePoolAllocationStrategy.Factory with RegisteredTypeFactory:
  override val typeUUID: UUID = UUID.fromString("c3a8e5d1-4b7f-4f29-8e06-91d2b7a4f35c")

  final case class IndicesExhausted(poolId: PoolId) extends Exception(s"FSIF pool $poolId has no free index")

  /** [indexCount:4][bitmap pointer] */
  def encodeConfig(indexCount: Int, bitmap: KeyValueObjectPointer): Array[Byte] =
    int2byte(indexCount) ++ bitmap.toArray

  def decodeConfig(bytes: Array[Byte]): (Int, KeyValueObjectPointer) =
    val bb = ByteBuffer.wrap(bytes)
    val indexCount = bb.getInt()
    (indexCount, KeyValueObjectPointer(bb))

  /** Allocates a zeroed bitmap for `indexCount` indices from `allocator` and returns the pool-state entries that
    * select this strategy and name the bitmap.
    */
  def prepareBitmap(indexCount: Int, allocator: ObjectAllocator)(using t: Transaction): Future[Map[Key, Value]] =
    given ExecutionContext = allocator.client.clientContext
    allocator.allocateKeyValueObject(FsifIndexBitmap.zeroedContent(indexCount)).map: bitmap =>
      Map(StoragePoolState.AllocationStrategyKey -> Value(uuid2byte(typeUUID)),
          StoragePoolState.AllocationStrategyConfigKey -> Value(encodeConfig(indexCount, bitmap)))

  override def createStrategy(client: AspenClient,
                              pool: StoragePool,
                              configData: Option[Array[Byte]]): StoragePoolAllocationStrategy =
    configData match
      case None => throw new IllegalArgumentException(s"FSIF pool ${pool.poolId} has no allocation strategy config")
      case Some(bytes) =>
        val (indexCount, bitmap) = decodeConfig(bytes)
        new Strategy(client, indexCount, bitmap)

  /** One transaction's view of one pool's bitmap. Guarded by its own monitor. */
  private final class TxState:
    private val readPromise = Promise[KeyValueObjectState]()
    private var readStarted = false
    /** The segment allocations are currently taken from */
    var segment: Option[Int] = None
    /** Indices handed out by this transaction, staged or not */
    var reserved: Set[Int] = Set()

    /** The transaction's read of the bitmap, started on the first call. The read is issued outside every lock. */
    def read(client: AspenClient, bitmap: KeyValueObjectPointer): Future[KeyValueObjectState] =
      val start = synchronized:
        val first = !readStarted
        readStarted = true
        first
      if start then
        readPromise.completeWith(client.read(bitmap, "FSIF allocation bitmap"))
      readPromise.future

  class Strategy(client: AspenClient, val indexCount: Int, val bitmap: KeyValueObjectPointer)
    extends StoragePoolAllocationStrategy with Logging:

    private given ExecutionContext = client.clientContext

    private val segmentCount = FsifIndexBitmap.segmentsFor(indexCount)

    private def state(poolId: PoolId)(using t: Transaction): TxState =
      t.attachment((typeUUID, poolId))(new TxState)

    private def segmentBytes(kvos: KeyValueObjectState, segment: Int): Array[Byte] =
      kvos.contents(FsifIndexBitmap.segmentKey(segment)).value.bytes

    /** A free index of `segment` that this transaction has not handed out */
    private def freeIndex(st: TxState, kvos: KeyValueObjectState, segment: Int): Option[Int] =
      FsifIndexBitmap.freeBit(segmentBytes(kvos, segment), FsifIndexBitmap.usableBits(segment, indexCount),
        b => st.reserved.contains(FsifIndexBitmap.index(segment, b)))
        .map(b => FsifIndexBitmap.index(segment, b))

    private def reserve(poolId: PoolId)(using t: Transaction): Future[Array[Byte]] =
      val st = state(poolId)
      st.read(client, bitmap).map: kvos =>
        st.synchronized:
          val current = st.segment.flatMap(s => freeIndex(st, kvos, s))
          val index = current.orElse:
            Random.shuffle((0 until segmentCount).toList).iterator
              .flatMap(s => freeIndex(st, kvos, s).map(i => (s, i)))
              .nextOption()
              .map: (s, i) =>
                st.segment = Some(s)
                i
          index match
            case None => throw IndicesExhausted(poolId)
            case Some(i) =>
              st.reserved += i
              FsifIndexBitmap.storePointer(i)

    override def createDataObjectPointer(poolId: PoolId)(using t: Transaction): Future[DataObjectPointer] =
      reserve(poolId).map(sp => new DataObjectPointer(ObjectId(UUID.randomUUID()), poolId, sp))

    override def createKeyValueObjectPointer(poolId: PoolId)(using t: Transaction): Future[KeyValueObjectPointer] =
      reserve(poolId).map(sp => new KeyValueObjectPointer(ObjectId(UUID.randomUUID()), poolId, sp))

    override def prepareCommit(poolId: PoolId, allocated: List[ObjectPointer], deleted: List[ObjectPointer],
                               deferRelease: Boolean)(using t: Transaction): Future[Unit] =
      // The indices are taken here so a bad pointer fails the transaction even when its release is deferred
      Future(deleted.map(p => FsifIndexBitmap.indexOf(p.storePointer, indexCount))).flatMap: deletedIndices =>
        if deferRelease && deletedIndices.nonEmpty then
          for
            _ <- updateBits(poolId, allocated, Nil)
            _ <- FsifReleaseIndicesTask.prepare(client, poolId, deletedIndices)
          yield ()
        else
          updateBits(poolId, allocated, deletedIndices)

    /** Clears the bits of `indices` in one guarded update to the bitmap within the transaction. Used by
      * FsifReleaseIndicesTask.
      */
    def prepareRelease(poolId: PoolId, indices: Seq[Int])(using t: Transaction): Future[Unit] =
      updateBits(poolId, Nil, indices)

    /** Reads the transaction's view of the bitmap, sets the bits of `allocated` and clears those of `released`, and
      * adds one update guarded by the revision of every segment key it changes. An already-set bit for an allocation
      * fails it; an already-clear bit for a release is logged and skipped. Adds nothing when no bit changes.
      */
    private def updateBits(poolId: PoolId, allocated: List[ObjectPointer], released: Seq[Int])
                          (using t: Transaction): Future[Unit] =
      if allocated.isEmpty && released.isEmpty then
        Future.unit
      else
        state(poolId).read(client, bitmap).map: kvos =>
          var segments = Map[Int, Array[Byte]]()
          def current(segment: Int): Array[Byte] = segments.getOrElse(segment, segmentBytes(kvos, segment))

          allocated.foreach: ptr =>
            val index = FsifIndexBitmap.indexOf(ptr.storePointer, indexCount)
            val (segment, bit) = FsifIndexBitmap.locate(index)
            val bytes = current(segment)
            if FsifIndexBitmap.isSet(bytes, bit) then
              throw new IllegalStateException(s"FSIF pool $poolId: index $index of ${ptr.id} is already allocated")
            segments += segment -> FsifIndexBitmap.withBit(bytes, bit, true)

          released.foreach: index =>
            val (segment, bit) = FsifIndexBitmap.locate(index)
            val bytes = current(segment)
            if FsifIndexBitmap.isSet(bytes, bit) then
              segments += segment -> FsifIndexBitmap.withBit(bytes, bit, false)
            else
              logger.warn(s"FSIF pool $poolId: released index $index is already free")

          if segments.nonEmpty then
            val keys = segments.keys.toList.sorted.map(FsifIndexBitmap.segmentKey)
            t.update(bitmap, None, None,
              keys.map(k => KeyValueUpdate.KeyRevision(k, kvos.contents(k).revision)),
              segments.toList.map((s, bytes) => Insert(FsifIndexBitmap.segmentKey(s), bytes)))
