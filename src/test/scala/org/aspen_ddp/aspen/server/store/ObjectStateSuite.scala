package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.objects.{Key, Metadata, ObjectId, ObjectRefcount, ObjectRevision, ObjectType, Value}
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class ObjectStateSuite extends AnyFunSuite with Matchers:

  private val tx1 = TransactionId(new UUID(0, 1))
  private val tx2 = TransactionId(new UUID(0, 2))
  private val tx3 = TransactionId(new UUID(0, 3))

  private def kvObject(): ObjectState = new ObjectState(
    ObjectId(new UUID(0, 9)),
    Metadata(ObjectRevision(TransactionId(new UUID(0, 8))), ObjectRefcount(1, 1), HLCTimestamp(1)),
    ObjectType.KeyValue,
    DataBuffer(new Array[Byte](0)))

  // Readers use these to recognize a store that has not yet applied a transaction they know committed
  test("lockedWriteTransactions includes key non-existence, full content and range locks"):
    val os = kvObject()
    val kvs = os.kvState.get
    kvs.absentKeyLocks += Key(Array[Byte](1)) -> tx1
    kvs.contentLocked = Some(tx2)
    kvs.rangeLocks += tx3
    os.lockedWriteTransactions shouldBe Set(tx1, tx2, tx3)

  // A shared lock holder will not change the object, so readers must not wait for it
  test("lockedWriteTransactions excludes SharedRevisionLock holders and lockHolders includes them"):
    val os = kvObject()
    os.lockedToTransaction = Some(tx1)
    os.sharedLocks += tx2
    os.lockedWriteTransactions shouldBe Set(tx1)
    os.lockHolders shouldBe Set(tx1, tx2)

  test("lockedKeys reports locked present keys and absent-key locks; objectLevelLocked reports object locks"):
    val k1 = Key(Array[Byte](1))
    val k2 = Key(Array[Byte](2))
    val os = kvObject()
    val kvs = os.kvState.get
    kvs.content += k1 -> new ValueState(Value(Array[Byte](9)), ObjectRevision(tx1), HLCTimestamp(1), None)
    kvs.content(k1).lockedToTransaction = Some(tx1)
    kvs.absentKeyLocks += (k2 -> tx2)
    os.lockedKeys shouldBe Set(k1, k2)
    os.objectLevelLocked shouldBe false
    os.lockedToTransaction = Some(tx3)
    os.objectLevelLocked shouldBe true
