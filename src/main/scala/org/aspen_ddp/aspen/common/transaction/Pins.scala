package org.aspen_ddp.aspen.common.transaction

import org.aspen_ddp.aspen.common.objects.{Delete, Insert, KeyValueOperation, ObjectId, ObjectRevision}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision

/** The pin rule. A transaction is safe to replay when something in it fails once the transaction has committed,
  * so a pin is any of:
  *  - a DataUpdate at a required revision other than ObjectRevision.Allocating
  *  - a VersionBump
  *  - a KeyValueUpdate with a required revision
  *  - a KeyRevision(k, _) with an Insert(k, _) or Delete(k) of the same key in the same update
  *  - a RefcountUpdate whose new refcount differs from the required one
  *
  * Allocations are never pins.
  *
  * The client checks the rule from its operations (hasPin). Stores hold the operations of their own objects only,
  * so the description records the key pin as KeyValueUpdate.pinned (writesCheckedKey) and every store checks the
  * rule from the description alone (declared). The stores holding a pinned object check its flag.
  */
object Pins:
  /** Whether the update writes a key it carries a KeyRevision on */
  def writesCheckedKey(kvu: KeyValueUpdate, ops: List[KeyValueOperation]): Boolean =
    val checked = kvu.requirements.iterator.collect { case KeyRevision(k, _) => k }.toSet
    checked.nonEmpty && ops.exists:
      case i: Insert => checked.contains(i.key)
      case d: Delete => checked.contains(d.key)
      case _ => false

  /** Whether the requirements carry a pin, taking each KeyValueUpdate's pinned flag as the key pin */
  def declared(requirements: List[TransactionRequirement]): Boolean =
    requirements.exists:
      case du: DataUpdate => du.requiredRevision != ObjectRevision.Allocating
      case _: VersionBump => true
      case kvu: KeyValueUpdate => kvu.requiredRevision.isDefined || kvu.pinned
      case ru: RefcountUpdate => ru.newRefcount != ru.requiredRefcount
      case _ => false

  def hasPin(requirements: List[TransactionRequirement], kvOps: Map[ObjectId, List[KeyValueOperation]]): Boolean =
    declared(requirements) || requirements.exists:
      case kvu: KeyValueUpdate => writesCheckedKey(kvu, kvOps.getOrElse(kvu.objectPointer.id, Nil))
      case _ => false
