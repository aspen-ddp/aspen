package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.common.objects.{Key, KeyOrdering, ObjectPointer, ObjectRefcount, ObjectRevision, Value}

/** Pointer to the next node to the right in a tier, along with that node's minimum key. */
final case class NodeLink(minimum: Key, pointer: ObjectPointer)

/** Kind-agnostic view of one node of a tier, independent of how the node is stored. */
trait ListNode:
  def pointer: ObjectPointer
  def minimum: Key
  def maximum: Option[Key]
  def link: Option[NodeLink]
  def ordering: KeyOrdering
  def revision: ObjectRevision
  def refcount: ObjectRefcount

  /** All entries, ascending by `ordering` */
  def entries: List[(Key, Value)]

  def get(key: Key): Option[Value]

  def size: Int

  def keyInRange(key: Key): Boolean
