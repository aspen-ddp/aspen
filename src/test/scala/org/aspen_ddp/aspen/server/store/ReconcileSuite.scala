package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.client.{DataObjectState, KeyValueObjectState, ReadLocks}
import org.aspen_ddp.aspen.client.KeyValueObjectState.ValueState as ClientValueState
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.ida.ReedSolomon
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Key, KeyValueObjectPointer, ObjectId, ObjectRefcount,
                                           ObjectRevision, Value}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class ReconcileSuite extends AnyFunSuite with Matchers:

  // Reed-Solomon slices differ from the values, so the plan's slices are checked against the store's own index
  private val ida = ReedSolomon(5, 3, 4)
  private val poolId = PoolId(new UUID(0, 5))
  private val storeId = StoreId(poolId, 1.toByte)
  private val oid = ObjectId(new UUID(1, 1))
  private val kvPtr = KeyValueObjectPointer(oid, poolId, Array[Byte]())
  private val dataPtr = DataObjectPointer(oid, poolId, Array[Byte]())

  private def rev(n: Int): ObjectRevision = ObjectRevision(TransactionId(new UUID(9, n)))
  private def ts(n: Long): HLCTimestamp = HLCTimestamp(n << 16)
  private def key(n: Int): Key = Key(Array[Byte](n.toByte))
  private def value(n: Int): Value = Value(Array.fill[Byte](10)(n.toByte))
  private def slice(v: Value): Value = Value(ida.encode(v.bytes)(storeId.poolIndex))

  private val k0 = key(0)
  private val k1 = key(1)
  private val k2 = key(2)
  private val k3 = key(3)

  /** The store's copy of a KeyValue object: keys at their revisions, no object-level parts */
  private def kvStore(keys: Map[Key, ObjectRevision], revision: ObjectRevision = rev(0),
                      refcount: ObjectRefcount = ObjectRefcount(1, 1), timestamp: HLCTimestamp = ts(10),
                      level: KVObjectLevel = KVObjectLevel.Empty): RepairSnapshot =
    RepairSnapshot(oid, revision, refcount, timestamp, keys, Some(level))

  /** The read: keys at their revisions, each key `n` holding value(n) */
  private def kvRead(keys: Map[Key, ObjectRevision], revision: ObjectRevision = rev(0),
                     refcount: ObjectRefcount = ObjectRefcount(1, 1), timestamp: HLCTimestamp = ts(10),
                     min: Option[Key] = None, max: Option[Key] = None,
                     left: Option[Value] = None, right: Option[Value] = None,
                     locks: ReadLocks = ReadLocks.None): KeyValueObjectState =
    val contents = keys.map((k, r) => k -> ClientValueState(value(k.bytes(0)), r, ts(5)))
    val s = new KeyValueObjectState(kvPtr, revision, refcount, timestamp, timestamp, ida, min, max, left, right,
      contents)
    s.readLocks = locks
    s

  private def plan(snapshot: RepairSnapshot, current: RepairSnapshot, read: KeyValueObjectState | DataObjectState,
                   locks: ReadLocks = ReadLocks.None): ReconcilePlan =
    Reconcile.plan(snapshot, current, read, storeId, locks)

  private def installed(k: Key, r: ObjectRevision): (Key, ClientValueState) =
    k -> ClientValueState(slice(value(k.bytes(0))), r, ts(5))

  test("a key the store lacks is installed from the read"):
    val store = kvStore(Map(k0 -> rev(1)))
    val p = plan(store, store, kvRead(Map(k0 -> rev(1), k1 -> rev(2))))
    p.installs shouldBe Map(installed(k1, rev(2)))
    p.deletes shouldBe empty
    p.result.complete shouldBe true

  test("a key the read lacks is deleted from the store"):
    val store = kvStore(Map(k0 -> rev(1), k1 -> rev(2)))
    val p = plan(store, store, kvRead(Map(k0 -> rev(1))))
    p.installs shouldBe empty
    p.deletes shouldBe Set(k1)
    p.result.complete shouldBe true

  test("a key whose read version differs is replaced"):
    val store = kvStore(Map(k0 -> rev(1), k1 -> rev(2)))
    val p = plan(store, store, kvRead(Map(k0 -> rev(1), k1 -> rev(3))))
    p.installs shouldBe Map(installed(k1, rev(3)))
    p.deletes shouldBe empty
    p.result.complete shouldBe true

  test("a key changed since the snapshot is left alone and the result is stale"):
    val snapshot = kvStore(Map(k0 -> rev(1), k2 -> rev(4)))
    // The store applied an insert of k3 and a rewrite of k2 after the snapshot. The read predates both
    val current = kvStore(Map(k0 -> rev(1), k2 -> rev(5), k3 -> rev(6)))
    val p = plan(snapshot, current, kvRead(Map(k0 -> rev(1), k1 -> rev(2), k2 -> rev(4))))
    p.installs shouldBe Map(installed(k1, rev(2)))
    p.deletes shouldBe empty
    p.result.stale shouldBe true
    p.result.complete shouldBe false

  test("a locked key is deferred and every other key is reconciled"):
    val store = kvStore(Map(k0 -> rev(1), k3 -> rev(7)))
    val locks = ReadLocks(Set(k1, k3), objectLevel = false)
    val p = plan(store, store, kvRead(Map(k0 -> rev(1), k1 -> rev(2), k2 -> rev(3))), locks)
    p.installs shouldBe Map(installed(k2, rev(3)))
    p.deletes shouldBe empty
    p.result.deferredKeys shouldBe Set(k1, k3)
    p.result.objectLevelDeferred shouldBe false
    p.result.complete shouldBe false

  test("a pass of only the deferred keys reconciles them and leaves the rest of the object alone"):
    val store = kvStore(Map(k0 -> rev(1), k1 -> rev(2), k2 -> rev(3)))
    // Differing everywhere: only k1 and k2 are in scope, and k2 is still locked
    val read = kvRead(Map(k0 -> rev(9)), revision = rev(8), refcount = ObjectRefcount(2, 3), timestamp = ts(20),
      min = Some(k0))
    val p = plan(store.copy(only = Some(Set(k1, k2))), store, read, ReadLocks(Set(k2, k3), objectLevel = true))
    p.installs shouldBe empty
    p.deletes shouldBe Set(k1)
    p.refcount shouldBe None
    p.objectLevel shouldBe None
    p.revision shouldBe None
    p.timestamp shouldBe None
    p.result shouldBe ReconcileResult(Set(k2), objectLevelDeferred = false, stale = false)

  test("an object-level lock defers revision, min, max, left, right and refcount"):
    val store = kvStore(Map(k0 -> rev(1)))
    val read = kvRead(Map(k0 -> rev(1), k1 -> rev(2)), revision = rev(8), refcount = ObjectRefcount(2, 3),
      min = Some(k0), max = Some(k3), left = Some(value(20)), right = Some(value(21)))

    val locked = plan(store, store, read, ReadLocks(Set(), objectLevel = true))
    locked.objectLevel shouldBe None
    locked.revision shouldBe None
    locked.refcount shouldBe None
    locked.installs shouldBe Map(installed(k1, rev(2)))
    locked.result.objectLevelDeferred shouldBe true
    locked.result.complete shouldBe false

    val free = plan(store, store, read)
    free.objectLevel shouldBe Some(KVObjectLevel(Some(k0), Some(k3), Some(slice(value(20))), Some(slice(value(21)))))
    free.revision shouldBe Some(rev(8))
    free.refcount shouldBe Some(ObjectRefcount(2, 3))
    free.result.complete shouldBe true

  test("refcount follows the higher serial and never decreases"):
    val store = kvStore(Map(), refcount = ObjectRefcount(2, 3))
    plan(store, store, kvRead(Map(), refcount = ObjectRefcount(3, 1))).refcount shouldBe Some(ObjectRefcount(3, 1))
    plan(store, store, kvRead(Map(), refcount = ObjectRefcount(2, 5))).refcount shouldBe None
    plan(store, store, kvRead(Map(), refcount = ObjectRefcount(1, 9))).refcount shouldBe None
    // Changed on the store since the snapshot: left alone
    val changed = kvStore(Map(), refcount = ObjectRefcount(3, 3))
    val p = plan(store, changed, kvRead(Map(), refcount = ObjectRefcount(4, 1)))
    p.refcount shouldBe None
    p.result.stale shouldBe true

  test("a DataObject's data follows a differing revision from a read no older than the store, " +
       "while its refcount follows the serial"):
    val store = RepairSnapshot(oid, rev(1), ObjectRefcount(1, 1), ts(10), Map(), None)
    def read(revision: ObjectRevision, refcount: ObjectRefcount, timestamp: HLCTimestamp): DataObjectState =
      DataObjectState(dataPtr, revision, refcount, timestamp, timestamp, ida, 0, DataBuffer(value(30).bytes))

    val newer = plan(store, store, read(rev(2), ObjectRefcount(1, 1), ts(20)))
    newer.data.map(_.getByteArray.toSeq) shouldBe Some(slice(value(30)).bytes.toSeq)
    newer.revision shouldBe Some(rev(2))
    newer.timestamp shouldBe Some(ts(20))
    newer.refcount shouldBe None
    newer.result.complete shouldBe true

    // A refcount hidden behind a later data write the store applied: the data's timestamp is not newer
    val hidden = plan(store, store, read(rev(1), ObjectRefcount(2, 4), ts(10)))
    hidden.data shouldBe None
    hidden.revision shouldBe None
    hidden.timestamp shouldBe None
    hidden.refcount shouldBe Some(ObjectRefcount(2, 4))
    hidden.result.complete shouldBe true

    // Data the store rewrote after the snapshot is left alone
    val rewritten = RepairSnapshot(oid, rev(3), ObjectRefcount(1, 1), ts(15), Map(), None)
    val stale = plan(store, rewritten, read(rev(2), ObjectRefcount(1, 1), ts(20)))
    stale.data shouldBe None
    stale.result.stale shouldBe true

  test("a DataObject's data is installed when its revision differs and the read is not older"):
    def read(revision: ObjectRevision, timestamp: HLCTimestamp): DataObjectState =
      DataObjectState(dataPtr, revision, ObjectRefcount(1, 1), timestamp, timestamp, ida, 0,
        DataBuffer(value(31).bytes))
    // The store missed the data write but applied a later refcount change: the timestamps are equal
    val store = RepairSnapshot(oid, rev(1), ObjectRefcount(1, 1), ts(20), Map(), None)
    val equal = plan(store, store, read(rev(2), ts(20)))
    equal.data.map(_.getByteArray.toSeq) shouldBe Some(slice(value(31)).bytes.toSeq)
    equal.revision shouldBe Some(rev(2))
    equal.timestamp shouldBe None
    equal.result.complete shouldBe true

    // A read older than the store's copy changes nothing
    val older = plan(store, store, read(rev(2), ts(10)))
    older.changesNothing shouldBe true
    older.result.complete shouldBe true

    // The same revision changes nothing, whatever the timestamps
    plan(store, store, read(rev(1), ts(30))).data shouldBe None

    // The store's revision changed since the snapshot: left alone, stale
    val rewritten = RepairSnapshot(oid, rev(3), ObjectRefcount(1, 1), ts(20), Map(), None)
    val stale = plan(store, rewritten, read(rev(2), ts(20)))
    stale.data shouldBe None
    stale.revision shouldBe None
    stale.result.stale shouldBe true

  test("an object deleted on the store since the snapshot is not recreated"):
    val snapshot = kvStore(Map(k0 -> rev(1)))
    val p = plan(snapshot, RepairSnapshot.missing(oid), kvRead(Map(k0 -> rev(1), k1 -> rev(2))))
    p.changesNothing shouldBe true
    p.result.stale shouldBe true
    val data = RepairSnapshot(oid, rev(1), ObjectRefcount(1, 1), ts(10), Map(), None)
    val d = plan(data, RepairSnapshot.missing(oid),
      DataObjectState(dataPtr, rev(2), ObjectRefcount(1, 1), ts(20), ts(20), ida, 0, DataBuffer(value(30).bytes)))
    d.changesNothing shouldBe true
    d.result.stale shouldBe true

  test("an object-level lock defers a DataObject's data and revision"):
    val store = RepairSnapshot(oid, rev(1), ObjectRefcount(1, 1), ts(10), Map(), None)
    val read = DataObjectState(dataPtr, rev(2), ObjectRefcount(2, 1), ts(20), ts(20), ida, 0,
      DataBuffer(value(30).bytes))
    val p = plan(store, store, read, ReadLocks(Set(), objectLevel = true))
    p.changesNothing shouldBe true
    p.result.objectLevelDeferred shouldBe true
    p.result.complete shouldBe false

  test("the object timestamp becomes the larger of the two"):
    val store = kvStore(Map(k0 -> rev(1)), timestamp = ts(10))
    plan(store, store, kvRead(Map(k0 -> rev(1), k1 -> rev(2)), timestamp = ts(30))).timestamp shouldBe Some(ts(30))
    // The store is ahead: its timestamp stays, while the missed key is still installed
    val behind = plan(store, store, kvRead(Map(k0 -> rev(1), k1 -> rev(2)), timestamp = ts(5)))
    behind.timestamp shouldBe None
    behind.installs shouldBe Map(installed(k1, rev(2)))

  test("an object the store does not hold is created from the read"):
    val missing = RepairSnapshot.missing(oid)
    val p = plan(missing, missing, kvRead(Map(k1 -> rev(2)), revision = rev(8), refcount = ObjectRefcount(0, 1)))
    p.installs shouldBe Map(installed(k1, rev(2)))
    p.refcount shouldBe Some(ObjectRefcount(0, 1))
    p.revision shouldBe Some(rev(8))
    p.result.complete shouldBe true
    // Its refcount is deferred under an object-level lock, so nothing is created
    val locked = plan(missing, missing, kvRead(Map(k1 -> rev(2))), ReadLocks(Set(), objectLevel = true))
    locked.changesNothing shouldBe true
    locked.result.complete shouldBe false
