package org.aspen_ddp.aspen.client.internal.transaction

import org.aspen_ddp.aspen.common.objects.{Delete, Insert, KeyValueOperation, ObjectId, ObjectRevision}
import org.aspen_ddp.aspen.common.transaction.*
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
  */
object Pins:
  def hasPin(requirements: List[TransactionRequirement], kvOps: Map[ObjectId, List[KeyValueOperation]]): Boolean =
    requirements.exists:
      case du: DataUpdate => du.requiredRevision != ObjectRevision.Allocating
      case _: VersionBump => true
      case kvu: KeyValueUpdate =>
        kvu.requiredRevision.isDefined || {
          val written = kvOps.getOrElse(kvu.objectPointer.id, Nil).flatMap:
            case i: Insert => Some(i.key)
            case d: Delete => Some(d.key)
            case _ => None
          kvu.requirements.exists:
            case KeyRevision(k, _) => written.contains(k)
            case _ => false
        }
      case ru: RefcountUpdate => ru.newRefcount != ru.requiredRefcount
      case _ => false
