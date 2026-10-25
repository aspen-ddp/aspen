package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.client.{AspenClient, ObjectAllocator, ObjectReader, Transaction}
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.objects.{Key, KeyOrdering, ObjectPointer, ObjectRevision, Value}

import scala.concurrent.Future

/** How the nodes of a TKVL are stored. Each `TreeKind` has one encoding; the tree code above it works only with
  * `ListNode`, `ObjectPointer` and `NodeLink`. Handing an encoding a node or pointer of another kind is a
  * programmer error and fails with `IllegalArgumentException`.
  */
trait NodeEncoding:
  def kind: TreeKind

  /** Reads the node at `pointer`, whose minimum key is `minimum`. Also used to refresh a node. */
  def read(reader: ObjectReader, pointer: ObjectPointer, minimum: Key, ordering: KeyOrdering,
           comment: String): Future[ListNode]

  /** Inserts or replaces `key` in `node`, which must be the node whose range holds `key`. A node that would grow
    * past `maxNodeSize` is split: the new right node is allocated from `allocator` and `prepareForSplit` is called
    * with its minimum and pointer. `requirement` is `Left(true)` for "key must not exist" or `Right(revision)` for
    * "key must be at this revision"; encodings that cannot enforce it throw `UnsupportedRequirement`.
    */
  def insert(node: ListNode,
             key: Key,
             value: Value,
             maxNodeSize: Int,
             allocator: ObjectAllocator,
             prepareForSplit: (Key, ObjectPointer) => Future[Unit],
             requirement: Option[Either[Boolean, ObjectRevision]])(using Transaction): Future[Unit]

  /** Deletes `key` from `node`. Deleting the last key of a node with a right link joins the right node into it
    * and calls `prepareForJoin` with the right node's minimum and pointer. `maxNodeSize` is the node's tier's; an
    * encoding whose joined node can outgrow the right node uses it to decide whether the join fits.
    */
  def delete(node: ListNode, key: Key, maxNodeSize: Int, prepareForJoin: (Key, ObjectPointer) => Future[Unit])
            (using Transaction): Future[Unit]

  /** Stages the removal of `node`'s link so it becomes the last node of its list (and, for a KeyValue node, drops its
    * stored maximum). The unlinked node is not touched.
    */
  def unlinkRight(node: ListNode)(using Transaction): Unit

  /** Splits the list at `splitAtKey`, keeping keys below it (and it too when `inclusive`) in `node`. Returns the
    * head of the new list holding the rest, whose minimum is `Key.AbsoluteMinimum` (see `KeyValueListNode.splitAt`).
    * `maxNodeSize` is the node's tier's. An encoding that cannot fit the rest of the list within it throws
    * `NodeSizeExceeded` before staging anything.
    */
  def splitAt(node: ListNode,
              splitAtKey: Key,
              inclusive: Boolean,
              maxNodeSize: Int,
              optionalDownPointer: Option[ObjectPointer],
              allocator: ObjectAllocator)(using Transaction): Future[NodeLink]

  /** Allocates the first node of a tree, or of a new tier, holding `entries`, whose order is given by `ordering` */
  def allocate(allocator: ObjectAllocator, ordering: KeyOrdering, entries: Map[Key, Value])
              (using Transaction): Future[ObjectPointer]

  /** Deletes every node of the list starting at `head`, calling `odeleteEntry` for each entry first if given */
  def deleteList(client: AspenClient,
                 head: NodeLink,
                 ordering: KeyOrdering,
                 odeleteEntry: Option[(Key, Value) => Future[Unit]]): Future[Unit]

  /** Largest `maxNodeSize` a node of this kind stored in a pool with this IDA and slice limit may use, or `None`
    * for no limit. See `NodeAllocator.nodeSizeLimit`.
    */
  def nodeSizeLimit(ida: IDA, maxSliceSize: Option[Int]): Option[Int]

object NodeEncoding:
  def apply(kind: TreeKind): NodeEncoding = kind match
    case TreeKind.KeyValueObjects => KeyValueNodeEncoding
    case TreeKind.DataObjects => DataObjectNodeEncoding

object SplitPlan:

  /** Chooses the keys that move to the new right node when a full node splits.
   *
   *  `sizes` holds every key of the node, including the pending insert, with its encoded size, ascending by
   *  key ordering. Keys are taken from the top until the moved size reaches half the total. At least the last
   *  key always moves. The result is ascending, so its head is the new node's minimum.
   *  A one-key input returns that key. Empty input is not allowed and throws.
   */
  def keysToMove(sizes: List[(Key, Int)]): List[Key] =
    val halfSize = sizes.foldLeft(0)((sz, t) => sz + t._2) / 2

    def rmove(rsizes: List[(Key, Int)], moveList: List[Key], moveSize: Int): List[Key] =
      rsizes match
        case Nil => moveList
        case (k, sz) :: rest =>
          if moveSize + sz >= halfSize then
            moveList
          else
            rmove(rest, k :: moveList, moveSize + sz)

    val rsizes = sizes.reverse
    rmove(rsizes.tail, rsizes.head._1 :: Nil, rsizes.head._2) // Ensure at least 1 move
