package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.client.{DataObjectState, ObjectReader}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Key, KeyOrdering, ObjectRefcount, ObjectRevision, Value}

/** A TKVL node stored as a DataObject. Everything but the revision and refcount comes from the object's bytes,
  * decoded by `DataObjectNodeEncoding.decode`. The node's maximum is the minimum of its right link.
  */
class DataObjectListNode(val reader: ObjectReader,
                         val ordering: KeyOrdering,
                         val state: DataObjectState) extends ListNode:

  val content: DataObjectNodeEncoding.NodeContent = DataObjectNodeEncoding.decode(state.data.getByteArray)

  private lazy val byKey: Map[Key, Value] = content.entries.toMap

  def pointer: DataObjectPointer = state.pointer
  def revision: ObjectRevision = state.revision
  def refcount: ObjectRefcount = state.refcount

  def minimum: Key = content.minimum
  def link: Option[NodeLink] = content.link
  def maximum: Option[Key] = link.map(_.minimum)

  def entries: List[(Key, Value)] = content.entries

  def get(key: Key): Option[Value] = byKey.get(key)

  def size: Int = content.entries.size

  def keyInRange(key: Key): Boolean =
    ordering.compare(key, minimum) >= 0 && maximum.forall(ordering.compare(key, _) < 0)
