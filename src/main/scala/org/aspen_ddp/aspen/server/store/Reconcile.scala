package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.client.{DataObjectState, KeyValueObjectState, ReadLocks, ObjectState as ClientObjectState}
import org.aspen_ddp.aspen.client.KeyValueObjectState.ValueState as ClientValueState
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.objects.{Key, ObjectId, ObjectRefcount, ObjectRevision, Value}
import org.aspen_ddp.aspen.common.store.StoreId

/** A store's copy of an object as repair recorded it: per key its revision, and the object's revision, refcount,
  * timestamp and object-level parts. Taken on the store's executor before the repair reads the object, and again
  * before it installs, so that anything the store applied in between is left alone. See Reconcile.
  *
  * @param objectLevel min, max, left and right, as the store holds them (left and right are the store's IDA slices).
  *                    None for DataObjects and for objects the store does not hold
  */
final case class RepairSnapshot(objectId: ObjectId, revision: ObjectRevision, refcount: ObjectRefcount,
                                timestamp: HLCTimestamp, keyRevisions: Map[Key, ObjectRevision],
                                objectLevel: Option[KVObjectLevel]):

  /** The store holds no copy: a live object never has a zero count */
  def isMissing: Boolean = refcount.count == 0

object RepairSnapshot:

  /** The snapshot of an object the store does not hold */
  def missing(objectId: ObjectId): RepairSnapshot =
    RepairSnapshot(objectId, ObjectRevision.Null, ObjectRefcount(0, 0), HLCTimestamp.Zero, Map(), None)

  def of(os: ObjectState): RepairSnapshot =
    RepairSnapshot(os.objectId, os.metadata.revision, os.metadata.refcount, os.metadata.timestamp,
      os.kvState.map(_.content.map((k, vs) => k -> vs.revision)).getOrElse(Map()),
      os.kvState.map(kvs => KVObjectLevel(kvs.min, kvs.max, kvs.left, kvs.right)))

/** The object-level parts of a KeyValue object other than its revision. Values compare by content */
final case class KVObjectLevel(min: Option[Key], max: Option[Key], left: Option[Value], right: Option[Value]):

  override def equals(other: Any): Boolean = other match
    case that: KVObjectLevel =>
      min == that.min && max == that.max &&
        KeyValueObjectState.cmp(left.map(_.bytes), that.left.map(_.bytes)) &&
        KeyValueObjectState.cmp(right.map(_.bytes), that.right.map(_.bytes))
    case _ => false

  override def hashCode: Int =
    def h(v: Option[Value]): Int = v.map(x => java.util.Arrays.hashCode(x.bytes)).getOrElse(0)
    (min, max, h(left), h(right)).##

object KVObjectLevel:
  val Empty: KVObjectLevel = KVObjectLevel(None, None, None, None)

/** What a reconciliation left undone. Keys and object-level parts reported locked are deferred; `stale` means
  * something the repair would have changed differed from the snapshot (it was left for a later pass), or the read
  * could not be used. Only a complete result lets the repair drop its entry
  */
final case class ReconcileResult(deferredKeys: Set[Key], objectLevelDeferred: Boolean, stale: Boolean):
  def complete: Boolean = deferredKeys.isEmpty && !objectLevelDeferred && !stale

/** What to change on the store. Every value is in the store's form: key values, left and right are its IDA slices,
  * `data` its slice of a DataObject. Each field is None or empty when that part stays as it is
  *
  * @param objectLevel KeyValue min, max, left and right, set together with `revision`
  * @param revision the KeyValue object's revision, or a DataObject's alongside `data`
  */
final case class ReconcilePlan(installs: Map[Key, ClientValueState],
                               deletes: Set[Key],
                               refcount: Option[ObjectRefcount],
                               objectLevel: Option[KVObjectLevel],
                               revision: Option[ObjectRevision],
                               data: Option[DataBuffer],
                               timestamp: Option[HLCTimestamp],
                               result: ReconcileResult):

  def changesNothing: Boolean =
    installs.isEmpty && deletes.isEmpty && refcount.isEmpty && objectLevel.isEmpty && revision.isEmpty &&
      data.isEmpty && timestamp.isEmpty

object ReconcilePlan:
  def nothing(result: ReconcileResult): ReconcilePlan =
    ReconcilePlan(Map(), Set(), None, None, None, None, None, result)

/** Exact reconciliation of one object on one store (missed-update repair).
  *
  * The repair snapshots the store's copy, reads the object, then installs, on the store's executor. A read restores
  * from at least a read threshold of stores and a committed transaction collected commit votes from at least a write
  * threshold, so some responder voted for every committed transaction: it applied it, or it still holds its locks and
  * the read reports the key (or the object) locked. An unlocked key in the read therefore reflects every committed
  * transaction, and making the store's key equal to it is exact, with no timestamp gate. Anything the store changed
  * after the snapshot was written by a newer committed transaction and is left alone.
  */
object Reconcile:

  /** What to change on the store. `read` is the store's IDA slice view of the restored object. Keys in `locks`, and
    * object-level parts when `locks.objectLevel`, are deferred; anything differing from `snapshot` in `current` is
    * left alone and reported stale. */
  def plan(snapshot: RepairSnapshot, current: RepairSnapshot, read: ClientObjectState, storeId: StoreId,
           locks: ReadLocks): ReconcilePlan =
    if storeId.poolId != read.pointer.poolId then
      ReconcilePlan.nothing(ReconcileResult(Set(), false, stale = true))
    else
      read match
        case kv: KeyValueObjectState if !kv.partial && storeId.poolIndex < kv.ida.width =>
          planKeyValue(snapshot, current, kv, storeId, locks)
        case d: DataObjectState if storeId.poolIndex < d.ida.width =>
          planData(snapshot, current, d, storeId, locks)
        case _ =>
          // Partial or metadata-only reads cannot rebuild the store's copy
          ReconcilePlan.nothing(ReconcileResult(Set(), false, stale = true))

  /** The read's refcount when its serial is higher than the store's, or when the store holds no copy. None when it
    * is not taken, and whether the store's refcount changed since the snapshot (left alone, so stale)
    */
  private def planRefcount(snapshot: RepairSnapshot, current: RepairSnapshot,
                           read: ClientObjectState): (Option[ObjectRefcount], Boolean) =
    val wanted = current.isMissing || read.refcount.updateSerial > current.refcount.updateSerial
    if !wanted then
      (None, false)
    else if current.refcount != snapshot.refcount then
      (None, true)
    else
      (Some(read.refcount), false)

  private def planData(snapshot: RepairSnapshot, current: RepairSnapshot, read: DataObjectState, storeId: StoreId,
                       locks: ReadLocks): ReconcilePlan =
    if current.isMissing && locks.objectLevel then
      // Creating the copy needs its refcount, which is deferred
      ReconcilePlan.nothing(ReconcileResult(Set(), objectLevelDeferred = true, stale = false))
    else
      val (refcount, refcountStale) =
        if locks.objectLevel then (None, false) else planRefcount(snapshot, current, read)

      // Revisions of a DataObject form a single chain, so the timestamp gate is exact for its contents
      val (data, dataStale) =
        if read.timestamp <= current.timestamp then
          (None, false)
        else if current.revision != snapshot.revision then
          (None, true)
        else
          (Some(read.ida.encode(read.data)(storeId.poolIndex)), false)

      ReconcilePlan(Map(), Set(), refcount, None,
        revision = data.map(_ => read.revision),
        data = data,
        timestamp = data.map(_ => read.timestamp),
        ReconcileResult(Set(), locks.objectLevel, refcountStale || dataStale))

  private def planKeyValue(snapshot: RepairSnapshot, current: RepairSnapshot, read: KeyValueObjectState,
                           storeId: StoreId, locks: ReadLocks): ReconcilePlan =
    if current.isMissing && locks.objectLevel then
      ReconcilePlan.nothing(ReconcileResult(locks.keys, objectLevelDeferred = true, stale = false))
    else
      val idx = storeId.poolIndex
      def slice(v: Value): Value = Value(read.ida.encode(v.bytes)(idx))

      var stale = false
      var installs = Map[Key, ClientValueState]()
      var deletes = Set[Key]()

      val keys = current.keyRevisions.keySet ++ read.contents.keySet ++ snapshot.keyRevisions.keySet
      keys.iterator.filterNot(locks.keys.contains).foreach { key =>
        val have = current.keyRevisions.get(key)
        val want = read.contents.get(key)
        if have != want.map(_.revision) then
          if have != snapshot.keyRevisions.get(key) then
            stale = true
          else
            want match
              case Some(vs) => installs += key -> ClientValueState(slice(vs.value), vs.revision, vs.timestamp)
              case None => deletes += key
      }

      val (refcount, refcountStale) =
        if locks.objectLevel then (None, false) else planRefcount(snapshot, current, read)

      val (objectLevel, revision) =
        if locks.objectLevel then
          (None, None)
        else
          val have = current.objectLevel.getOrElse(KVObjectLevel.Empty)
          val want = KVObjectLevel(read.minimum, read.maximum, read.left.map(slice), read.right.map(slice))
          if have == want && current.revision == read.revision && !current.isMissing then
            (None, None)
          else if current.objectLevel != snapshot.objectLevel || current.revision != snapshot.revision then
            stale = true
            (None, None)
          else
            (Some(want), Some(read.revision))

      val timestamp = if read.timestamp > current.timestamp then Some(read.timestamp) else None

      ReconcilePlan(installs, deletes, refcount, objectLevel, revision, None, timestamp,
        ReconcileResult(locks.keys, locks.objectLevel, stale || refcountStale))
