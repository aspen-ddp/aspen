package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.objects.{ByteRanges, Key, KeyRange, KeySet, MetadataOnly, ObjectType, ReadType}

/** Reduces a store's object data to just what a partial read asked for.
  *
  * MetadataOnly reads return no data. KeyValue objects read with KeySet or KeyRange are reduced; the
  * object's min, max, left, and right are always kept. Data objects read with ByteRanges are clipped to the requested part of the slice. Everything else is returned unchanged.
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
        val size = data.size.toLong
        val parts = rt.ranges.map((offset, length) => clip(data, offset.toLong, offset.toLong + length.toLong)) :+
          clip(data, size - rt.tail.toLong, size)
        Some(parts.foldLeft(DataBuffer())(_.append(_)))

      case _ => Some(data)

  /** Returns [start, end) of the data with both ends clamped to [0, size] */
  private def clip(data: DataBuffer, start: Long, end: Long): DataBuffer =
    val size = data.size.toLong
    val s = math.min(math.max(start, 0L), size)
    val e = math.max(s, math.min(end, size))
    data.slice(s.toInt, (e - s).toInt)

  private def reduce(data: DataBuffer, select: KVObjectState => Map[Key, ValueState]): DataBuffer =
    val kvos = KVObjectState.decode(data)
    KVObjectState.encode(kvos.min, kvos.max, kvos.left, kvos.right, select(kvos))
