package org.aspen_ddp.aspen.server.store.backend

import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.ida.AppendDelta
import org.aspen_ddp.aspen.common.objects.ObjectId
import org.aspen_ddp.aspen.common.transaction.{DataUpdate, DataUpdateOperation, KeyValueUpdate, RefcountUpdate,
  TransactionRequirement, VersionBump}

/** How an object's slice changed in a commit, so backends can avoid rewriting unchanged bytes */
enum UpdateForm:
  /** The whole slice is new (allocation, overwrite, key-value update, rebuild, repair) */
  case Full
  /** Only the metadata changed; the slice is untouched */
  case MetadataOnly
  /** The slice had oldLength bytes and was changed only within spans.
    * spans: sorted, non-overlapping (offset, length) pairs in the NEW slice; includes
    * [oldLength, newLength) when it grows */
  case Ranges(oldLength: Int, spans: List[(Int, Int)])

object UpdateForm:

  def of(objectId: ObjectId,
         requirements: List[TransactionRequirement],
         objectUpdates: Map[ObjectId, DataBuffer],
         allocating: Boolean): UpdateForm =
    if allocating then
      Full
    else
      val dataOp = requirements.collectFirst:
        case DataUpdate(ptr, _, op) if ptr.id == objectId => op
      dataOp match
        case Some(DataUpdateOperation.Append | DataUpdateOperation.WriteRange) =>
          objectUpdates.get(objectId) match
            case Some(update) => rangesOf(AppendDelta.decode(update))
            case None => Full
        case Some(_) => Full
        case None =>
          val isKeyValue = requirements.exists:
            case kv: KeyValueUpdate => kv.objectPointer.id == objectId
            case _ => false
          val metaOnly = requirements.exists:
            case r: RefcountUpdate => r.objectPointer.id == objectId
            case v: VersionBump => v.objectPointer.id == objectId
            case _ => false
          if !isKeyValue && metaOnly then MetadataOnly else Full

  private def rangesOf(delta: AppendDelta): UpdateForm =
    val extension =
      if delta.newSliceLength > delta.expectedSliceLength then
        List((delta.expectedSliceLength, delta.newSliceLength - delta.expectedSliceLength))
      else
        Nil
    val all = (extension ++ delta.regions.map(r => (r.offset, r.bytes.length))).sortBy(_._1)
    val merged = all.foldLeft(List.empty[(Int, Int)]):
      case ((off, len) :: rest, (o, l)) if o <= off + len =>
        (off, math.max(off + len, o + l) - off) :: rest
      case (acc, span) => span :: acc
    Ranges(delta.expectedSliceLength, merged.reverse)
