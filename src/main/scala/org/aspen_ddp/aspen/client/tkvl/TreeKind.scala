package org.aspen_ddp.aspen.client.tkvl

/** The kind of object a TKVL's nodes are stored in */
enum TreeKind(val code: Byte):
  case KeyValueObjects extends TreeKind(0)
  case DataObjects extends TreeKind(1)

object TreeKind:
  def fromCode(code: Byte): TreeKind = code match
    case 0 => KeyValueObjects
    case 1 => DataObjects
    case _ => throw new IllegalArgumentException(s"Unknown TreeKind code $code")
