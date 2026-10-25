package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, KeyValueObjectPointer, ObjectPointer}

/** The kind of object a TKVL's nodes are stored in */
enum TreeKind(val code: Byte):
  case KeyValueObjects extends TreeKind(0)
  case DataObjects extends TreeKind(1)

object TreeKind:
  def fromCode(code: Byte): TreeKind = code match
    case 0 => KeyValueObjects
    case 1 => DataObjects
    case _ => throw new IllegalArgumentException(s"Unknown TreeKind code $code")

  /** The kind of tree whose nodes `pointer` can point to. `Root` requires its pointer to match its kind, so this
    * is the root's kind for any node of the tree.
    */
  def of(pointer: ObjectPointer): TreeKind = pointer match
    case _: KeyValueObjectPointer => KeyValueObjects
    case _: DataObjectPointer => DataObjects
