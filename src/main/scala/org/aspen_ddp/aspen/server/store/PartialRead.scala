package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.objects.{ByteRanges, Key, KeyRange, KeySet, MetadataOnly, ObjectType, ReadType}

/** Reduces a store's object data to just what a partial read asked for.
  *
  * MetadataOnly reads return no data. KeyValue objects read with KeySet or KeyRange are reduced; the
  * object's min, max, left, and right are always kept. Data objects read with ByteRanges are clipped to the
  * requested part of the slice. Everything else is returned unchanged.
  */
object PartialRead:

  def filter(objectType: ObjectType.Value, data: DataBuffer, readType: ReadType): Option[DataBuffer] =
    (objectType, readType) match
      case (_, _: MetadataOnly) => None

      case (ObjectType.KeyValue, rt: KeySet) =>
        val wanted = rt.keys.toSet
        Some(reduce(data, kvos => kvos.content.filter((key, _) => wanted.contains(key))))

      case (ObjectType.KeyValue, rt: KeyRange) =>
        Some(reduce(data, kvos => kvos.content.filter { (key, _) =>
          rt.ordering.compare(key, rt.minimum) >= 0 && rt.ordering.compare(key, rt.maximum) <= 0
        }))

      case (ObjectType.Data, rt: ByteRanges) =>
        Some(concat(rt.pieces(data.size).map((start, end) => data.slice(start, end - start))))

      case _ => Some(data)

  /** Concatenates the parts with a single allocation. A lone non-empty part is returned as is (a zero-copy view).
    * Throws IllegalStateException when the combined size would not fit in a DataBuffer.
    */
  private def concat(parts: List[DataBuffer]): DataBuffer =
    parts.filter(!_.isEmpty) match
      case Nil => DataBuffer()
      case single :: Nil => single
      case nonEmpty =>
        val total = nonEmpty.foldLeft(0L)(_ + _.size.toLong)
        if total > Int.MaxValue then
          throw new IllegalStateException(s"ByteRanges read of $total bytes exceeds the maximum of ${Int.MaxValue}")
        val bb = java.nio.ByteBuffer.allocate(total.toInt)
        nonEmpty.foreach(part => bb.put(part.asReadOnlyBuffer()))
        bb.flip()
        DataBuffer(bb)

  private def reduce(data: DataBuffer, select: KVObjectState => Map[Key, ValueState]): DataBuffer =
    val kvos = KVObjectState.decode(data)
    KVObjectState.encode(kvos.min, kvos.max, kvos.left, kvos.right, select(kvos))
