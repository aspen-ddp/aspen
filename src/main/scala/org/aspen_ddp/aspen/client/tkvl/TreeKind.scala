package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, KeyValueObjectPointer, ObjectPointer}

import scala.concurrent.{ExecutionContext, Future}

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

/** Checks that the tree `rootManager` manages is of kind `expected`, before a facade's first operation. The root's
  * kind can only be read asynchronously, so a facade's constructor cannot check it.
  */
private[tkvl] final class TreeKindCheck(rootManager: RootManager, expected: TreeKind):

  // The kind of a tree never changes, so one successful check covers every later operation
  @volatile private var passed = false

  /** Runs `f` once the root is known to be of kind `expected`. Otherwise fails with `WrongTreeKind` without
    * running it.
    */
  def apply[A](f: => Future[A])(using ExecutionContext): Future[A] =
    if passed then
      f
    else
      TreeKindCheck.check(rootManager, expected).flatMap: _ =>
        passed = true
        f

private[tkvl] object TreeKindCheck:

  /** Fails with `WrongTreeKind` if the root of the tree `rootManager` manages is not of kind `expected` */
  def check(rootManager: RootManager, expected: TreeKind)(using ExecutionContext): Future[Unit] =
    rootManager.getRoot().map: root =>
      if root.kind != expected then
        throw WrongTreeKind(expected, root.kind)
