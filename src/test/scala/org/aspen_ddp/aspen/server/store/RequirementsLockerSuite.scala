package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.objects.{Key, KeyValueObjectPointer, Metadata, ObjectId, ObjectRefcount, ObjectRevision, ObjectType, Value}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.{DoesNotExist, FullContentLock, KeyRevision}
import org.aspen_ddp.aspen.common.transaction.{DataUpdate, DataUpdateOperation, KeyValueUpdate, TransactionId, TransactionRequirement}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

/** Two recovered commit votes may take one single-holder lock slot (transaction-requirements.md, recovered
  * transactions re-acquiring locks): the slot stays locked until both have released it, in either order
  */
class RequirementsLockerSuite extends AnyFunSuite with Matchers:

  private val tx1 = TransactionId(new UUID(0, 1))
  private val tx2 = TransactionId(new UUID(0, 2))
  private val oid = ObjectId(new UUID(0, 9))
  private val rev = ObjectRevision(TransactionId(new UUID(0, 8)))
  private val ptr = KeyValueObjectPointer(oid, PoolId(new UUID(0, 0)), Array[Byte]())
  private val present = Key(Array[Byte](1))
  private val absent = Key(Array[Byte](2))

  private def kvObject(): ObjectState =
    val os = new ObjectState(oid, Metadata(rev, ObjectRefcount(1, 1), HLCTimestamp(1)), ObjectType.KeyValue,
      DataBuffer(new Array[Byte](0)))
    val kvs = os.kvState.get
    kvs.content += present -> new ValueState(Value(Array[Byte](3)), rev, HLCTimestamp(1), None)
    os

  private def kv(contentLock: Option[FullContentLock] = None,
                 reqs: List[KeyValueUpdate.KeyRequirement] = Nil,
                 requiredRevision: Option[ObjectRevision] = None): KeyValueUpdate =
    KeyValueUpdate(ptr, requiredRevision, contentLock, reqs)

  private case class Slot(name: String, req: TransactionRequirement, holder: ObjectState => Option[TransactionId])

  private val slots = List(
    Slot("exclusive (X)", DataUpdate(ptr, rev, DataUpdateOperation.Overwrite), _.lockedToTransaction),
    Slot("content (C)", kv(contentLock = Some(FullContentLock(Nil))), _.kvState.get.contentLocked),
    Slot("present key (K)", kv(reqs = List(KeyRevision(present, rev))),
      _.kvState.get.content(present).lockedToTransaction),
    Slot("absent key (N)", kv(reqs = List(DoesNotExist(absent))), _.kvState.get.absentKeyLocks.get(absent)))

  private def lock(os: ObjectState, tx: TransactionId, req: TransactionRequirement): Unit =
    RequirementsLocker.lock(tx, List(req), Map(oid -> os), Map(), recovered = true)

  private def unlock(os: ObjectState, tx: TransactionId, req: TransactionRequirement): Unit =
    RequirementsLocker.unlock(tx, List(req), Map(oid -> os), Map())

  for s <- slots do
    test(s"${s.name}: the slot passes to the displaced holder when the current holder releases it"):
      val os = kvObject()
      lock(os, tx1, s.req)
      lock(os, tx2, s.req)
      s.holder(os) shouldBe Some(tx2)
      os.lockedWriteTransactions shouldBe Set(tx1, tx2)

      unlock(os, tx2, s.req)
      s.holder(os) shouldBe Some(tx1)
      os.displacedLocks shouldBe Nil
      unlock(os, tx1, s.req)
      s.holder(os) shouldBe None
      os.lockedWriteTransactions shouldBe Set()

    test(s"${s.name}: a displaced holder's release leaves the current holder locked"):
      val os = kvObject()
      lock(os, tx1, s.req)
      lock(os, tx2, s.req)

      unlock(os, tx1, s.req)
      s.holder(os) shouldBe Some(tx2)
      os.displacedLocks shouldBe Nil
      unlock(os, tx2, s.req)
      s.holder(os) shouldBe None

  test("a transaction taking one slot through several requirements releases it once for its displaced holder"):
    val os = kvObject()
    val twice = kv(requiredRevision = Some(rev), reqs = List(KeyValueUpdate.KeyObjectRevision(present, rev)))
    lock(os, tx1, DataUpdate(ptr, rev, DataUpdateOperation.Overwrite))
    lock(os, tx2, twice)
    os.displacedLocks shouldBe List(LockSlot.Object -> tx1)

    unlock(os, tx2, twice)
    os.lockedToTransaction shouldBe Some(tx1)
    os.displacedLocks shouldBe Nil

  test("lockedKeys and objectLevelLocked count displaced holders"):
    val os = kvObject()
    os.displacedLocks = List(LockSlot.AbsentKey(absent) -> tx1)
    os.lockedKeys shouldBe Set(absent)
    os.objectLevelLocked shouldBe false
    os.displacedLocks = List(LockSlot.Content -> tx1)
    os.objectLevelLocked shouldBe true
