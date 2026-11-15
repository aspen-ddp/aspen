package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.ida.{ReedSolomon, Replication}
import org.aspen_ddp.aspen.common.objects.*
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class DescriptionValidatorSuite extends AnyFunSuite with Matchers:

  private val poolA = PoolId(new UUID(0, 1))
  private val poolB = PoolId(new UUID(0, 2))
  private val ida = Replication(3, 2)
  private val storeA = StoreId(poolA, 1.toByte)
  private val rev0 = ObjectRevision(TransactionId(new UUID(0, 3)))
  private val data = DataObjectPointer(ObjectId(new UUID(0, 4)), poolA, Array[Byte]())
  private val kv = KeyValueObjectPointer(ObjectId(new UUID(0, 5)), poolA, Array[Byte]())
  private val kvB = KeyValueObjectPointer(ObjectId(new UUID(0, 6)), poolB, Array[Byte]())
  private val k1 = Key(Array[Byte](1))

  private def overwrite(p: DataObjectPointer = data) = DataUpdate(p, rev0, DataUpdateOperation.Overwrite)

  private def txd(reqs: List[TransactionRequirement], replaySafe: Boolean = false): TransactionDescription =
    TransactionDescription(TransactionId(new UUID(0, 7)), HLCTimestamp(1), reqs.head.asInstanceOf[TransactionObjectRequirement].objectPointer,
      0, reqs, Nil, None, Nil, Nil, ida, Map(poolA -> ida, poolB -> ida), replaySafe = replaySafe)

  private def validate(t: TransactionDescription) = DescriptionValidator.validate(t, storeA, Some(ida))

  test("a pinned single-pool description is valid") {
    validate(txd(List(overwrite()))) shouldBe None
  }

  test("a description naming another IDA for the store's pool is refused") {
    val other = Replication(3, 3)
    val t = txd(List(overwrite())).copy(primaryObjectIDA = other, poolIDAMap = Map(poolA -> other))
    validate(t).get should include("names IDA")
    // Unchecked without the store's IDA (tests only)
    DescriptionValidator.validate(t, storeA, None) shouldBe None
  }

  test("a referenced pool without an IDA is refused") {
    validate(txd(List(overwrite())).copy(poolIDAMap = Map(poolB -> ida))).get should include("no IDA")
  }

  test("a primary IDA that is not the primary pool's entry is refused") {
    validate(txd(List(overwrite())).copy(primaryObjectIDA = Replication(5, 3))).get should include("primary IDA")
  }

  test("an unreferenced primary object is refused") {
    val t = txd(List(overwrite())).copy(primaryObject = DataObjectPointer(ObjectId(new UUID(0, 9)), poolA, Array[Byte]()))
    validate(t).get should include("not referenced")
  }

  test("a primary pool that tolerates fewer failures than another is refused") {
    val wide = ReedSolomon(5, 3, 3)
    val t = txd(List(overwrite(), KeyValueUpdate(kvB, Some(rev0), None, Nil))).copy(
      poolIDAMap = Map(poolA -> ida, poolB -> wide))
    validate(t).get should include("tolerates fewer failures")
  }

  test("a designated leader outside the primary pool is refused") {
    validate(txd(List(overwrite())).copy(designatedLeaderUID = 3)).get should include("designated leader")
    validate(txd(List(overwrite())).copy(designatedLeaderUID = -1)).get should include("designated leader")
  }

  test("two content updates of one object are refused") {
    validate(txd(List(overwrite(), VersionBump(data, rev0)))).get should include("more than once")
    validate(txd(List(KeyValueUpdate(kv, Some(rev0), None, Nil), KeyValueUpdate(kv, Some(rev0), None, Nil))))
      .get should include("more than once")
  }

  test("two refcount updates of one object are refused, but one beside a content update is allowed") {
    val rc = RefcountUpdate(data, ObjectRefcount(1, 1), ObjectRefcount(2, 1))
    validate(txd(List(overwrite(), rc))) shouldBe None
    validate(txd(List(rc, rc))).get should include("refcount")
  }

  test("a lock on an updated object, or a shared lock beside another lock, is refused") {
    validate(txd(List(overwrite(), ExclusiveRevisionLock(data, rev0)))).get should include("also updates")
    validate(txd(List(overwrite(), SharedRevisionLock(data, rev0)))).get should include("also updates")
    validate(txd(List(SharedRevisionLock(data, rev0), ExclusiveRevisionLock(data, rev0)), replaySafe = true))
      .get should include("shared lock")
    validate(txd(List(SharedRevisionLock(data, rev0), SharedRevisionLock(data, rev0)), replaySafe = true))
      .get should include("shared lock")
    // The builder lets an exclusive lock be taken twice: harmless, so allowed
    validate(txd(List(ExclusiveRevisionLock(data, rev0), ExclusiveRevisionLock(data, rev0)), replaySafe = true)) shouldBe None
  }

  test("a description with no pin is refused unless it is marked replaySafe") {
    val locks = List(ExclusiveRevisionLock(data, rev0))
    validate(txd(locks)).get should include("no pin")
    validate(txd(locks, replaySafe = true)) shouldBe None
    val allocation = List(DataUpdate(data, ObjectRevision.Allocating, DataUpdateOperation.Overwrite))
    validate(txd(allocation)).get should include("no pin")
  }

  test("each kind of pin satisfies the rule") {
    val pins = List(
      overwrite(),
      VersionBump(data, rev0),
      KeyValueUpdate(kv, Some(rev0), None, Nil),
      KeyValueUpdate(kv, None, None, List(KeyValueUpdate.KeyRevision(k1, rev0)), pinned = true),
      RefcountUpdate(data, ObjectRefcount(1, 1), ObjectRefcount(2, 1)))
    pins.foreach(p => validate(txd(List(p))) shouldBe None)
    // A KeyRevision alone is a pin only with the flag, which the builder sets when the k1 is written
    validate(txd(List(KeyValueUpdate(kv, None, None, List(KeyValueUpdate.KeyRevision(k1, rev0)))))).get should include("no pin")
    validate(txd(List(RefcountUpdate(data, ObjectRefcount(1, 1), ObjectRefcount(1, 1))))).get should include("no pin")
  }

  test("a pin on another pool's object counts for this store") {
    val pinB = KeyValueUpdate(kvB, None, None, List(KeyValueUpdate.KeyRevision(k1, rev0)), pinned = true)
    validate(txd(List(KeyValueUpdate(kv, None, None, Nil), pinB))) shouldBe None
  }

  test("Pins.writesCheckedKey is set only by a write of a checked k1") {
    val u = KeyValueUpdate(kv, None, None, List(KeyValueUpdate.KeyRevision(k1, rev0)))
    Pins.writesCheckedKey(u, List(new Insert(k1, Array[Byte](1)))) shouldBe true
    Pins.writesCheckedKey(u, List(new Delete(k1))) shouldBe true
    Pins.writesCheckedKey(u, List(new Insert(Key(Array[Byte](2)), Array[Byte](1)))) shouldBe false
    Pins.writesCheckedKey(KeyValueUpdate(kv, None, None, Nil), List(new Insert(k1, Array[Byte](1)))) shouldBe false
  }
