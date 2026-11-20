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

  /** For an unpinned transaction, why each of its requirements is not a pin, one line per requirement on an object, so
    * the error names what the transaction holds that comes closest (UnpinnedTransaction)
    */
  def whyUnpinned(requirements: List[TransactionRequirement],
                  kvOps: Map[ObjectId, List[KeyValueOperation]],
                  allocating: Set[ObjectId]): List[String] =
    val allocations = requirements.collect {
      case r: TransactionObjectRequirement if allocating.contains(r.objectPointer.id) => r.objectPointer.id
    }.distinct.map(oid => s"$oid: allocated by the transaction (an allocation is not a pin)")
    allocations ++ requirements.filter {
      case r: TransactionObjectRequirement => !allocating.contains(r.objectPointer.id)
      case _ => true
    }.collect:
      case du: DataUpdate =>
        s"${du.objectPointer.id}: the DataUpdate allocates the object (an allocation is not a pin)"
      case kvu: KeyValueUpdate =>
        val ops = kvOps.getOrElse(kvu.objectPointer.id, Nil)
        val written = ops.collect { case i: Insert => i.key; case d: Delete => d.key }
        val checked = kvu.requirements.collect { case KeyRevision(k, _) => k }
        val what =
          if written.isEmpty then "writes no key"
          else if checked.isEmpty then s"writes ${written.mkString(", ")} with no KeyRevision on any of them"
          else s"writes ${written.mkString(", ")} but has a KeyRevision only on ${checked.mkString(", ")}"
        s"${kvu.objectPointer.id}: the KeyValue update has no required revision and $what" +
          (if kvu.requirements.exists(_.isInstanceOf[KeyValueUpdate.DoesNotExist]) then
            " (DoesNotExist is not a pin)" else "")
      case ru: RefcountUpdate =>
        s"${ru.objectPointer.id}: the RefcountUpdate leaves the refcount at ${ru.newRefcount}"
      case l: ExclusiveRevisionLock => s"${l.objectPointer.id}: an ExclusiveRevisionLock (a lock is not a pin)"
      case l: SharedRevisionLock => s"${l.objectPointer.id}: a SharedRevisionLock (a lock is not a pin)"

  def hasPin(requirements: List[TransactionRequirement], kvOps: Map[ObjectId, List[KeyValueOperation]]): Boolean =
    declared(requirements) || requirements.exists:
      case kvu: KeyValueUpdate => writesCheckedKey(kvu, kvOps.getOrElse(kvu.objectPointer.id, Nil))
      case _ => false
