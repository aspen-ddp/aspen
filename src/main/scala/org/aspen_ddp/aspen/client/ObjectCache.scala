package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, KeyValueObjectPointer, ObjectPointer}

trait ObjectCache {
  def get(pointer: DataObjectPointer): Option[DataObjectState]
  def get(pointer: KeyValueObjectPointer): Option[KeyValueObjectState]

  def get(pointer: ObjectPointer): Option[ObjectState] = pointer match {
    case dop: DataObjectPointer => get(dop)
    case kop: KeyValueObjectPointer => get(kop)
  }

  /** To be called ONLY by read drivers */
  private[client] def put(pointer: ObjectPointer, dos: ObjectState): Unit
}

object ObjectCache {
  object NoCache extends ObjectCache {
    def get(pointer: DataObjectPointer): Option[DataObjectState] = None
    def get(pointer: KeyValueObjectPointer): Option[KeyValueObjectState] = None

    private[client] def put(pointer: ObjectPointer, dos: ObjectState): Unit = ()
  }
}
