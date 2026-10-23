package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.objects.{Key, KeyRange, KeySet, ObjectType, ReadType}

/** Reduces a store's object data to just what a partial read asked for.
  *
  * Only KeyValue objects read with KeySet or KeyRange are reduced. The object's min, max, left, and
  * right are always kept. Everything else is returned unchanged.
  */
object PartialRead:

  def filter(objectType: ObjectType.Value, data: DataBuffer, readType: ReadType): DataBuffer =
    (objectType, readType) match
      case (ObjectType.KeyValue, rt: KeySet) =>
        val wanted = rt.keys.toSet
        reduce(data, kvos => kvos.content.filter((key, _) => wanted.contains(key)))

      case (ObjectType.KeyValue, rt: KeyRange) =>
        reduce(data, kvos => kvos.content.filter { (key, _) =>
          rt.ordering.compare(key, rt.minimum) >= 0 && rt.ordering.compare(key, rt.maximum) <= 0
        })

      case _ => data

  private def reduce(data: DataBuffer, select: KVObjectState => Map[Key, ValueState]): DataBuffer =
    val kvos = KVObjectState.decode(data)
    KVObjectState.encode(kvos.min, kvos.max, kvos.left, kvos.right, select(kvos))
