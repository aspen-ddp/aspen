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
    * "key must be at this revision"; encodings that cannot enforce it throw `UnsupportedRequirement`. A key that
    * must not exist but does aborts the transaction with `KeyAlreadyExists`.
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
    * encoding whose joined node can outgrow the right node uses it to decide whether the join fits. If it does not,
    * the node is left empty with its link.
    *
    * The result is true if the node is left empty. The caller then removes it from its list, see
    * `TierEngine.prepareEmptyNodeRemoval`.
    */
  def delete(node: ListNode, key: Key, maxNodeSize: Int, prepareForJoin: (Key, ObjectPointer) => Future[Unit])
            (using Transaction): Future[Boolean]

  /** Applies a batch of `ops` to `node`: ascending, at most one per key and all within the node's range. The node is
    * updated once, so it can take any number of changes in one transaction. A node that no longer fits splits, into
    * more than two nodes if one split is not enough, calling `prepareForSplit` for each new node. `joinSource` is the
    * node's right neighbor, as read, when the caller found that the batch removes every entry of the node and leaves
    * the neighbor alone: the node absorbs it if the join fits, calling `prepareForJoin`. Reads nothing.
    *
    * A put demanding its key be new (`Left(true)`) aborts the transaction with `KeyAlreadyExists` if the node holds
    * the key. The result is true if the node is left empty.
    */
  def applyBatch(node: ListNode,
                 ops: List[BatchOp],
                 maxNodeSize: Int,
                 allocator: ObjectAllocator,
                 joinSource: Option[ListNode],
                 prepareForSplit: (Key, ObjectPointer) => Future[Unit],
                 prepareForJoin: (Key, ObjectPointer) => Future[Unit])(using Transaction): Future[Boolean]

  /** Stages pointing `node`, the predecessor of an empty node being removed from the list, at `newLink`, the removed
    * node's link. `newLink` can be longer than the old link, in which case `node` splits if it no longer fits
    * within `maxNodeSize`: the new node is allocated from `allocator` and `prepareForSplit` is called as for an
    * insert. Throws `NodeSizeExceeded` before staging anything if no split point fits. The removed node is not
    * touched.
    */
  def relink(node: ListNode,
             newLink: Option[NodeLink],
             maxNodeSize: Int,
             allocator: ObjectAllocator,
             prepareForSplit: (Key, ObjectPointer) => Future[Unit])(using Transaction): Future[Unit]

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

  /** As `allocate`, for the first node of a tree. Entries too many for one node of `maxNodeSize` are divided among a
    * list of nodes, calling `prepareForSplit` for each node after the first. Returns the first node.
    */
  def allocateChain(allocator: ObjectAllocator,
                    ordering: KeyOrdering,
                    entries: Map[Key, Value],
                    maxNodeSize: Int,
                    prepareForSplit: (Key, ObjectPointer) => Future[Unit])(using Transaction): Future[ObjectPointer]

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

  /** Splits the entries of a node that is too large for one split into consecutive runs, each fitting a node. Used
   *  when a batch adds more to a node than two nodes can hold.
   *
   *  `sizes` holds the encoded size of each entry, ascending by key ordering. `fits(i, j)` says whether entries
   *  `i until j` fit in one node: the run starting at 0 is the original node, every other run a new node whose
   *  minimum is entry `i`, and every run but the last links to a node whose minimum is entry `j`. A run's entries
   *  alone exceeding `maxNodeSize` is taken as not fitting without asking `fits`.
   *
   *  Returns the start index of each run, the first being 0, using as few nodes as possible and, among those, runs as
   *  close to equal size as the greedy choice allows. There are always at least two runs. None if no division fits.
   */
  def runs(sizes: IndexedSeq[Int], maxNodeSize: Int, fits: (Int, Int) => Boolean): Option[List[Int]] =
    val n = sizes.size
    val prefix = sizes.scanLeft(0)(_ + _)

    def pairs(i: Int, j: Int): Int = prefix(j) - prefix(i)

    // The ends a run starting at i can have. The original node may not hold every entry.
    def ends(i: Int): Iterator[Int] =
      Iterator.range(i + 1, n + 1).takeWhile(j => pairs(i, j) <= maxNodeSize).filter(j => !(i == 0 && j == n))
        .filter(j => fits(i, j))

    // nodes(i): fewest nodes holding entries i until n, if any division fits
    val nodes = new Array[Int](n + 1)
    java.util.Arrays.fill(nodes, -1)
    nodes(n) = 0
    val fitting = Array.fill(n)(List.empty[Int])
    for i <- (n - 1) to 0 by -1 do
      fitting(i) = ends(i).filter(j => nodes(j) >= 0).toList
      if fitting(i).nonEmpty then
        nodes(i) = fitting(i).map(nodes(_)).min + 1

    if nodes(0) < 0 then
      None
    else
      def choose(i: Int): List[Int] =
        if i == n then
          Nil
        else
          val target = pairs(i, n) / nodes(i)
          val j = fitting(i).filter(j => nodes(j) == nodes(i) - 1).minBy(j => math.abs(pairs(i, j) - target))
          i :: choose(j)
      Some(choose(0))
