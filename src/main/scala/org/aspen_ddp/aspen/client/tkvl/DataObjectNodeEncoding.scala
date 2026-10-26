package org.aspen_ddp.aspen.client.tkvl

import java.nio.ByteBuffer

import org.aspen_ddp.aspen.client.{AspenClient, ConflictingRequirements, InvalidObject, MultipleDataUpdatesToObject, MultipleRefcountUpdatesToObject, ObjectAllocator, ObjectReader, Transaction}
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Key, KeyAlreadyExists, KeyOrdering, ObjectPointer, ObjectRevision, Value}
import org.aspen_ddp.aspen.common.util.Varint
import scribe.Logging

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Failure

/** Nodes stored as DataObjects. A DataObject has no side fields, so the node's minimum and right link live in its
  * bytes. Every update is a whole-node rewrite guarded by the node's revision; there are no per-entry revisions, so
  * revision requirements are not supported. "Key must not exist" is: the node's range changes only through an update
  * of the node, so a key absent from the node as read is absent at commit if the node's revision still matches.
  *
  * A transaction can modify a node only once, so an update to a node the transaction already modifies fails with
  * `NodeAlreadyModified` (see `writeNode`). It stages nothing unless an earlier part of the same update is already
  * staged, as in a split or join, in which case the transaction is aborted.
  *
  * Layout, with every length an unsigned varint:
  * {{{
  *   header    minimum length << 1, plus 1 if the node has a right link
  *   minimum   the minimum key's bytes
  *   link      only when flagged: link minimum length, link minimum bytes, link pointer (ObjectPointer encoding,
  *             which is self-delimiting)
  *   entries   key length, key bytes, value length, value bytes for each entry, ascending by key ordering
  * }}}
  * Entries run to the end of the object, so there is no entry count. The node's maximum is its link's minimum, so
  * it is not stored separately. An empty node with the absolute minimum and no link is a single byte.
  */
object DataObjectNodeEncoding extends NodeEncoding with Logging:

  /** The decoded bytes of a node */
  final case class NodeContent(minimum: Key, link: Option[NodeLink], entries: List[(Key, Value)])

  def kind: TreeKind = TreeKind.DataObjects

  // --------------------------------------------------------------------------------------------------------------
  // Byte layout

  private def lengthPrefixedSize(length: Int): Int = Varint.getUnsignedIntEncodingLength(length) + length

  /** Encoded bytes of one entry */
  def entrySize(key: Key, value: Value): Int =
    lengthPrefixedSize(key.bytes.length) + lengthPrefixedSize(value.bytes.length)

  /** Encoded bytes of a node with no entries */
  def nodeOverhead(minimum: Key, link: Option[NodeLink]): Int =
    val header = Varint.getUnsignedIntEncodingLength(minimum.bytes.length << 1) + minimum.bytes.length
    link match
      case None => header
      case Some(l) => header + lengthPrefixedSize(l.minimum.bytes.length) + l.pointer.encodedSize

  /** Exactly `encode(entries, minimum, link).length` */
  def encodedSize(entries: List[(Key, Value)], minimum: Key, link: Option[NodeLink]): Int =
    entries.foldLeft(nodeOverhead(minimum, link))((sz, e) => sz + entrySize(e._1, e._2))

  /** `entries` must be ascending by the tree's key ordering */
  def encode(entries: List[(Key, Value)], minimum: Key, link: Option[NodeLink]): Array[Byte] =
    val arr = new Array[Byte](encodedSize(entries, minimum, link))
    val bb = ByteBuffer.wrap(arr)

    def put(bytes: Array[Byte]): Unit =
      Varint.putUnsignedInt(bb, bytes.length)
      bb.put(bytes)

    Varint.putUnsignedInt(bb, (minimum.bytes.length << 1) | (if link.isDefined then 1 else 0))
    bb.put(minimum.bytes)
    link.foreach: l =>
      put(l.minimum.bytes)
      l.pointer.encodeInto(bb)
    entries.foreach: (k, v) =>
      put(k.bytes)
      put(v.bytes)
    arr

  /** Throws `CorruptedLinkedList` if the link pointer is not a DataObjectPointer */
  def decode(bytes: Array[Byte]): NodeContent =
    val bb = ByteBuffer.wrap(bytes)

    def get(length: Int): Array[Byte] =
      val arr = new Array[Byte](length)
      bb.get(arr)
      arr

    def getPrefixed(): Array[Byte] = get(Varint.getUnsignedInt(bb))

    val header = Varint.getUnsignedInt(bb)
    val minimum = Key(get(header >>> 1))

    val link =
      if (header & 1) == 0 then
        None
      else
        val linkMinimum = Key(getPrefixed())
        ObjectPointer.fromByteBuffer(bb) match
          case p: DataObjectPointer => Some(NodeLink(linkMinimum, p))
          case _ => throw new CorruptedLinkedList

    val entries = ListBuffer[(Key, Value)]()
    while bb.hasRemaining do
      val k = Key(getPrefixed())
      entries += k -> Value(getPrefixed())

    NodeContent(minimum, link, entries.toList)

  // --------------------------------------------------------------------------------------------------------------
  // NodeEncoding

  private def dataNode(node: ListNode): DataObjectListNode = node match
    case n: DataObjectListNode => n
    case n => throw new IllegalArgumentException(
      s"DataObjectNodeEncoding requires a DataObjectListNode, got ${n.getClass.getName}")

  private def dataPointer(pointer: ObjectPointer): DataObjectPointer = pointer match
    case p: DataObjectPointer => p
    case p => throw new IllegalArgumentException(
      s"DataObjectNodeEncoding requires a DataObjectPointer, got ${p.getClass.getName}")

  /** Every update of an existing node goes through here. A transaction can update a node only once: the transaction
    * itself rejects a second data update to an object, or one to an object it holds a revision lock on (a node a join
    * absorbed). Both are reported as `NodeAlreadyModified`, with nothing staged by this write. Newly allocated nodes
    * get their content from the allocation, not from here.
    */
  private def writeNode(node: DataObjectListNode, content: Array[Byte])(using tx: Transaction): Unit =
    try
      tx.overwrite(node.pointer, node.revision, DataBuffer(content))
    catch
      case _: MultipleDataUpdatesToObject | _: ConflictingRequirements => throw NodeAlreadyModified(node.pointer)

  /** Runs `stage`, which adds to `tx` after an earlier part of the same update is already staged. A
    * `NodeAlreadyModified` from it would leave that earlier part staged without the rest, so it aborts the transaction.
    */
  private def abortIfAlreadyModified(tx: Transaction)(stage: => Unit): Unit =
    try
      stage
    catch
      case e: NodeAlreadyModified => tx.abortAndThrow(e)

  /** `entries` with `key` set to `value`, kept ascending */
  private def withEntry(entries: List[(Key, Value)], key: Key, value: Value, ordering: KeyOrdering): List[(Key, Value)] =
    val (below, rest) = entries.filterNot(_._1 == key).span(e => ordering.compare(e._1, key) < 0)
    below ++ ((key -> value) :: rest)

  /** The minimum is taken from the node's bytes; `minimum` is not needed since nodes are self-contained */
  def read(reader: ObjectReader, pointer: ObjectPointer, minimum: Key, ordering: KeyOrdering,
           comment: String): Future[ListNode] =
    given ExecutionContext = reader.client.clientContext
    reader.read(dataPointer(pointer), comment).map(s => new DataObjectListNode(reader, ordering, s))

  // --------------------------------------------------------------------------------------------------------------
  // Planning. Everything in this section is a pure function of a node's decoded bytes: it decides what an insert,
  // delete, relink or splitAt must do (rewrite in place, split, join, leave empty, or fail) and returns that as a plan,
  // touching no Transaction, allocator or reader.
  //
  // This separation exists FOR TESTING. The size arithmetic and split/join decisions are subtle (a split gives the new
  // node a longer minimum and a link, a join changes the minimum, and every node must stay within maxNodeSize), and
  // they are easy to get wrong in ways only many edge cases reveal. Written inline with the transaction calls, they
  // could only be exercised through a full Aspen system, which is slow and cannot explore enough cases. As plain
  // functions they are tested directly, over thousands of randomized operations, in DataObjectNodePlanSuite. Keep
  // decision logic here and keep the NodeEncoding methods below limited to applying a plan.

  /** The outcome of planning an insert or relink */
  sealed abstract class RewritePlan

  /** The node still fits: rewrite it with `entries` */
  final case class Rewrite(entries: List[(Key, Value)]) extends RewritePlan

  /** The node no longer fits and must split. `all` is every entry the two nodes will hold */
  final case class NeedsSplit(all: List[(Key, Value)]) extends RewritePlan

  /** A split: the node keeps `left` and links to a new node, which holds `right` with minimum `newMinimum` */
  final case class NodeSplit(left: List[(Key, Value)], right: List[(Key, Value)], newMinimum: Key)

  /** Plans an insert of `key`/`value` into the node at `pointer`. An entry is accepted only if it fits in the node
    * with a link as long as one to its own key, so that it still fits alone in a node that a later split gives a
    * link. The new node's pointer is not known yet, so the node's own `pointer` stands in for the link's. Throws
    * `NodeSizeExceeded` if the entry is not accepted.
    */
  def planInsert(pointer: DataObjectPointer,
                 content: NodeContent,
                 ordering: KeyOrdering,
                 key: Key,
                 value: Value,
                 maxNodeSize: Int): RewritePlan =
    val reserved = math.max(nodeOverhead(content.minimum, content.link),
      nodeOverhead(content.minimum, Some(NodeLink(key, pointer))))
    if entrySize(key, value) > maxNodeSize - reserved then
      throw new NodeSizeExceeded

    val all = withEntry(content.entries, key, value, ordering)

    if encodedSize(all, content.minimum, content.link) <= maxNodeSize then
      Rewrite(all)
    else
      NeedsSplit(all)

  /** Throws `KeyAlreadyExists` if `requirement` demands `key` be new and `content` holds it, and
    * `UnsupportedRequirement` for a revision requirement
    */
  private def checkRequirement(content: NodeContent,
                               key: Key,
                               requirement: Option[Either[Boolean, ObjectRevision]]): Unit =
    requirement match
      case None | Some(Left(false)) =>
      case Some(Left(true)) =>
        if content.entries.exists(_._1 == key) then
          throw new KeyAlreadyExists(key)
      case Some(Right(_)) => throw new UnsupportedRequirement(kind)

  /** Plans a batch of `ops`, ascending and at most one per key, all within the node's range, or None if the batch
    * changes nothing (it only deletes keys the node does not hold). Generalizes `planInsert` and `planDelete`: each
    * put is accepted as `planInsert` accepts an entry, and the result is the node's new entries, which are empty when
    * the batch deletes every entry (the caller then considers a join as for `ConsiderJoin`). Throws
    * `NodeSizeExceeded` for an entry that is not accepted, and `KeyAlreadyExists` or `UnsupportedRequirement` for a
    * put's requirement.
    */
  def planBatch(pointer: DataObjectPointer,
                content: NodeContent,
                ordering: KeyOrdering,
                ops: List[BatchOp],
                maxNodeSize: Int): Option[RewritePlan] =
    val puts = ops.collect { case p: BatchOp.Put => p }
    val removes = ops.collect { case BatchOp.Remove(k) => k }.toSet

    puts.foreach: p =>
      checkRequirement(content, p.key, p.requirement)
      val reserved = math.max(nodeOverhead(content.minimum, content.link),
        nodeOverhead(content.minimum, Some(NodeLink(p.key, pointer))))
      if entrySize(p.key, p.value) > maxNodeSize - reserved then
        throw new NodeSizeExceeded

    val putKeys = puts.iterator.map(_.key).toSet

    if puts.isEmpty && !content.entries.exists(e => removes.contains(e._1)) then
      None
    else
      val all = (content.entries.filterNot(e => removes.contains(e._1) || putKeys.contains(e._1)) ++
        puts.map(p => p.key -> p.value)).sortWith((a, b) => ordering.compare(a._1, b._1) < 0)

      if encodedSize(all, content.minimum, content.link) <= maxNodeSize then
        Some(Rewrite(all))
      else
        Some(NeedsSplit(all))

  /** Divides `all`, too many entries for one split, into runs that each fit a node: the first stays in the node,
    * with `minimum`, and each other moves to a new node, the last taking `link`. Uses as few nodes as possible.
    * `standIn` is the size the new nodes' pointers are assumed to have. Throws `NodeSizeExceeded` if no division fits.
    */
  def planMultiSplit(minimum: Key,
                     link: Option[NodeLink],
                     all: List[(Key, Value)],
                     standIn: DataObjectPointer,
                     maxNodeSize: Int): List[List[(Key, Value)]] =
    val entries = all.toIndexedSeq
    val sizes = entries.map((k, v) => entrySize(k, v))
    val prefix = sizes.scanLeft(0)(_ + _)

    def fits(i: Int, j: Int): Boolean =
      val nodeLink = if j == entries.size then link else Some(NodeLink(entries(j)._1, standIn))
      nodeOverhead(if i == 0 then minimum else entries(i)._1, nodeLink) + prefix(j) - prefix(i) <= maxNodeSize

    val starts = SplitPlan.runs(sizes, maxNodeSize, fits).getOrElse(throw new NodeSizeExceeded)

    (starts :+ entries.size).sliding(2).map(b => entries.slice(b(0), b(1)).toList).toList

  /** Whether `runs`, from `planMultiSplit`, fit with `pointers` as the new nodes' pointers */
  private[tkvl] def multiSplitFits(minimum: Key,
                                   link: Option[NodeLink],
                                   runs: List[List[(Key, Value)]],
                                   pointers: List[DataObjectPointer],
                                   maxNodeSize: Int): Boolean =
    multiSplitNodes(minimum, link, runs, pointers).forall((min, l, entries) => encodedSize(entries, min, l) <= maxNodeSize)

  /** The minimum, link and entries of each node of a multi-split, the original node first */
  private def multiSplitNodes(minimum: Key,
                              link: Option[NodeLink],
                              runs: List[List[(Key, Value)]],
                              pointers: List[DataObjectPointer]): List[(Key, Option[NodeLink], List[(Key, Value)])] =
    val links = runs.tail.zip(pointers).map((run, p) => Some(NodeLink(run.head._1, p))) :+ link
    val minimums = minimum :: runs.tail.map(_.head._1)
    minimums.lazyZip(links).lazyZip(runs).toList

  /** Plans pointing a node, the predecessor of an empty node being removed from its list, at `newLink` */
  def planRelink(content: NodeContent, newLink: Option[NodeLink], maxNodeSize: Int): RewritePlan =
    if encodedSize(content.entries, content.minimum, newLink) <= maxNodeSize then
      Rewrite(content.entries)
    else if content.entries.size < 2 then
      throw new NodeSizeExceeded
    else
      NeedsSplit(content.entries)

  /** Splits the node's `all` entries between it and a new node at `rightPointer` that takes `link` as its own. Takes
    * SplitPlan's choice of keys if both nodes fit and otherwise the nearest split point where they do. Throws
    * `NodeSizeExceeded` if none does.
    */
  def planSplit(minimum: Key,
                link: Option[NodeLink],
                all: List[(Key, Value)],
                rightPointer: DataObjectPointer,
                maxNodeSize: Int): NodeSplit =
    val index = splitIndex(minimum, all, link, rightPointer, maxNodeSize).getOrElse(throw new NodeSizeExceeded)
    val (left, right) = all.splitAt(index)
    NodeSplit(left, right, right.head._1)

  /** The outcome of planning a delete */
  sealed abstract class DeletePlan

  case object NothingToDelete extends DeletePlan

  /** Rewrite the node with `remaining`, which may be empty if the node has no link */
  final case class RemoveEntry(remaining: List[(Key, Value)]) extends DeletePlan

  /** The node's last entry is deleted and the node has a right link, so `planJoin` decides whether to absorb it */
  final case class ConsiderJoin(right: NodeLink) extends DeletePlan

  def planDelete(content: NodeContent, key: Key): DeletePlan =
    if !content.entries.exists(_._1 == key) then
      NothingToDelete
    else
      val remaining = content.entries.filterNot(_._1 == key)
      content.link match
        case Some(right) if remaining.isEmpty => ConsiderJoin(right)
        case _ => RemoveEntry(remaining)

  /** The node's new bytes when its last entry is deleted and its right neighbor `right` is considered for joining:
    * the neighbor's entries and link under this node's minimum, or None if that is larger than `maxNodeSize`, in which
    * case the node is left empty.
    */
  def planJoin(content: NodeContent, right: NodeContent, maxNodeSize: Int): Option[NodeContent] =
    if encodedSize(right.entries, content.minimum, right.link) > maxNodeSize then
      None
    else
      Some(NodeContent(content.minimum, right.link, right.entries))

  /** The entries `splitAt` keeps and moves. Throws `NodeSizeExceeded` if the node holding the moved entries, whose
    * minimum is `splitAtKey`, would not fit within `maxNodeSize`.
    */
  def planSplitAt(content: NodeContent,
                  ordering: KeyOrdering,
                  splitAtKey: Key,
                  inclusive: Boolean,
                  maxNodeSize: Int): (List[(Key, Value)], List[(Key, Value)]) =
    def stays(k: Key): Boolean =
      if inclusive then
        ordering.compare(k, splitAtKey) <= 0
      else
        ordering.compare(k, splitAtKey) < 0

    val (left, right) = content.entries.partition(e => stays(e._1))

    if encodedSize(right, splitAtKey, content.link) > maxNodeSize then
      throw new NodeSizeExceeded

    (left, right)

  // --------------------------------------------------------------------------------------------------------------
  // Executors. Apply a plan to a transaction. These only stage what the plan dictates (writeNode, allocation, reads of
  // the right neighbor, locks and refcounts, finalization callbacks); anything that decides what to do belongs in the
  // planning section above, where it can be tested without a Transaction.

  /** As `KeyValueListNode.insert`, rewriting the whole node.
    *
    * An entry is accepted only if it fits in this node with a link as long as one to its own key, so that it still
    * fits alone in a node that a later split gives a link. A split takes SplitPlan's choice of keys if both nodes
    * fit and otherwise the nearest split point where they do. If none does it throws `NodeSizeExceeded`. Like every
    * other `NodeSizeExceeded` here, that happens before anything is added to the transaction.
    */
  def insert(node: ListNode,
             key: Key,
             value: Value,
             maxNodeSize: Int,
             allocator: ObjectAllocator,
             prepareForSplit: (Key, ObjectPointer) => Future[Unit],
             requirement: Option[Either[Boolean, ObjectRevision]])(using tx: Transaction): Future[Unit] =
    val n = dataNode(node)
    given ExecutionContext = n.reader.client.clientContext

    try checkRequirement(n.content, key, requirement)
    catch case e: KeyAlreadyExists => tx.abortAndThrow(e)

    planInsert(n.pointer, n.content, n.ordering, key, value, maxNodeSize) match
      case Rewrite(entries) =>
        writeNode(n, encode(entries, n.minimum, n.link))
        Future.unit

      case NeedsSplit(all) =>
        // Creating the pointer stages nothing, so the split can be sized with it before anything is staged
        allocator.createDataObjectPointer().flatMap: rightPointer =>
          stageSplit(n, planSplit(n.minimum, n.link, all, rightPointer, maxNodeSize), n.link, rightPointer, allocator,
            prepareForSplit)

  private def stageSplit(n: DataObjectListNode,
                         split: NodeSplit,
                         rightLink: Option[NodeLink],
                         rightPointer: DataObjectPointer,
                         allocator: ObjectAllocator,
                         prepareForSplit: (Key, ObjectPointer) => Future[Unit])(using tx: Transaction): Future[Unit] =
    allocator.stageDataObjectAllocation(rightPointer, DataBuffer(encode(split.right, split.newMinimum, rightLink)))

    // The new right node is already staged
    abortIfAlreadyModified(tx)(
      writeNode(n, encode(split.left, n.minimum, Some(NodeLink(split.newMinimum, rightPointer)))))
    prepareForSplit(split.newMinimum, rightPointer)

  /** Where to split `all`, the node's entries with the new one, which do not fit in `n`: the number of entries
    * `n` keeps, the rest moving to the new node at `rightPointer` and taking `link` as its own. SplitPlan's choice
    * comes first, then the other split points nearest it. None if no split point leaves both nodes within
    * `maxNodeSize`.
    */
  private[tkvl] def splitIndex(minimum: Key,
                                all: List[(Key, Value)],
                                link: Option[NodeLink],
                                rightPointer: DataObjectPointer,
                                maxNodeSize: Int): Option[Int] =
    val keys = all.map(_._1).toArray
    val sizes = all.map((k, v) => entrySize(k, v))
    val prefix = sizes.scanLeft(0)(_ + _).toArray
    val total = prefix.last

    def fits(i: Int): Boolean =
      val leftSize = nodeOverhead(minimum, Some(NodeLink(keys(i), rightPointer))) + prefix(i)
      val rightSize = nodeOverhead(keys(i), link) + total - prefix(i)
      leftSize <= maxNodeSize && rightSize <= maxNodeSize

    // The moved keys are always a suffix of `all`
    val planned = all.size - SplitPlan.keysToMove(keys.toList.zip(sizes)).size
    val others = (1 until all.size).filter(_ != planned).sortBy(i => math.abs(i - planned))
    (planned +: others).find(fits)

  /** As `KeyValueListNode.delete`. A join rewrites this node with the right node's entries and link, locks the right
    * node's revision and drops its refcount.
    *
    * The joined node takes this node's minimum, which may be longer than the right node's, so it can be larger than
    * the right node. The join happens only when the joined node fits within `maxNodeSize`. Otherwise this node is
    * left empty, keeping its link, and no join is made, so the delete always succeeds. The result is true if the node
    * is left empty.
    */
  def delete(node: ListNode, key: Key, maxNodeSize: Int, prepareForJoin: (Key, ObjectPointer) => Future[Unit])
            (using tx: Transaction): Future[Boolean] =
    val n = dataNode(node)
    given ExecutionContext = n.reader.client.clientContext

    planDelete(n.content, key) match
      case NothingToDelete => Future.successful(false)

      case RemoveEntry(remaining) =>
        writeNode(n, encode(remaining, n.minimum, n.link))
        Future.successful(remaining.isEmpty)

      case ConsiderJoin(right) =>
        n.reader.read(dataPointer(right.pointer),
          s"Joining DataObject TKVL node ${right.pointer.id}. Minimum: ${right.minimum}. target: $key").flatMap: rs =>
          val r = new DataObjectListNode(n.reader, n.ordering, rs)
          planJoin(n.content, r.content, maxNodeSize) match
            case None =>
              writeNode(n, encode(Nil, n.minimum, n.link))
              Future.successful(true)

            case Some(joined) => stageJoin(n, joined, r, prepareForJoin)

  /** Stages `n` absorbing its right neighbor `r` as `joined`, planned by `planJoin`. True if the joined node is empty */
  private def stageJoin(n: DataObjectListNode,
                        joined: NodeContent,
                        r: DataObjectListNode,
                        prepareForJoin: (Key, ObjectPointer) => Future[Unit])(using tx: Transaction): Future[Boolean] =
    given ExecutionContext = n.reader.client.clientContext
    writeNode(n, encode(joined.entries, joined.minimum, joined.link))
    // This node's rewrite is already staged. The transaction rejects the bump if it already updates or locks the
    // right node, and the refcount update if it already changes the right node's refcount. The right node's version
    // is bumped rather than its revision locked, so that a racing writer that read it before the join, such as the
    // removal of an empty node it precedes, fails rather than rewriting a deleted node.
    abortIfAlreadyModified(tx):
      try
        tx.bumpVersion(r.pointer, r.revision)
        tx.setRefcount(r.pointer, r.refcount, r.refcount.decrement())
      catch
        case _: ConflictingRequirements | _: MultipleDataUpdatesToObject | _: MultipleRefcountUpdatesToObject =>
          throw NodeAlreadyModified(r.pointer)
    prepareForJoin(r.minimum, r.pointer).map(_ => r.entries.isEmpty)

  /** As `KeyValueListNode.applyBatch`, rewriting the whole node once. A put demanding its key be new relies on the
    * node's revision, which every rewrite requires, so no further requirement is added.
    */
  def applyBatch(node: ListNode,
                 ops: List[BatchOp],
                 maxNodeSize: Int,
                 allocator: ObjectAllocator,
                 joinSource: Option[ListNode],
                 prepareForSplit: (Key, ObjectPointer) => Future[Unit],
                 prepareForJoin: (Key, ObjectPointer) => Future[Unit])(using tx: Transaction): Future[Boolean] =
    val n = dataNode(node)
    given ExecutionContext = n.reader.client.clientContext

    val plan =
      try planBatch(n.pointer, n.content, n.ordering, ops, maxNodeSize)
      catch case e: KeyAlreadyExists => tx.abortAndThrow(e)

    plan match
      case None => Future.successful(false)

      case Some(Rewrite(Nil)) if n.link.isDefined && joinSource.isDefined =>
        val r = dataNode(joinSource.get)
        planJoin(n.content, r.content, maxNodeSize) match
          case None =>
            writeNode(n, encode(Nil, n.minimum, n.link))
            Future.successful(true)
          case Some(joined) => stageJoin(n, joined, r, prepareForJoin)

      case Some(Rewrite(entries)) =>
        writeNode(n, encode(entries, n.minimum, n.link))
        Future.successful(entries.isEmpty)

      case Some(NeedsSplit(all)) =>
        allocator.createDataObjectPointer().flatMap: rightPointer =>
          splitIndex(n.minimum, all, n.link, rightPointer, maxNodeSize) match
            case Some(_) =>
              stageSplit(n, planSplit(n.minimum, n.link, all, rightPointer, maxNodeSize), n.link, rightPointer,
                allocator, prepareForSplit).map(_ => false)
            case None =>
              multiSplit(n.minimum, n.link, all, rightPointer, maxNodeSize, allocator).flatMap: (runs, pointers) =>
                val first = stageNewNodes(n.link, runs, pointers, allocator)
                // The new nodes are already staged
                abortIfAlreadyModified(tx)(writeNode(n, encode(runs.head, n.minimum, first)))
                prepareNewNodes(runs, pointers, prepareForSplit).map(_ => false)

  /** How many times a multi-split plans again with larger pointers before giving up */
  private val MultiSplitAttempts = 3

  /** The runs `all` divides into (see `planMultiSplit`) and a pointer for each new node. Plans with `standIn`, a
    * pointer already created for the split, creates the rest, and plans again with the largest of them while a real
    * pointer is too large for the plan. Creating pointers stages nothing.
    */
  private def multiSplit(minimum: Key,
                         link: Option[NodeLink],
                         all: List[(Key, Value)],
                         standIn: DataObjectPointer,
                         maxNodeSize: Int,
                         allocator: ObjectAllocator)
                        (using Transaction, ExecutionContext): Future[(List[List[(Key, Value)]], List[DataObjectPointer])] =
    def attempt(sizing: DataObjectPointer,
                created: List[DataObjectPointer],
                remaining: Int): Future[(List[List[(Key, Value)]], List[DataObjectPointer])] =
      val runs = planMultiSplit(minimum, link, all, sizing, maxNodeSize)
      val needed = runs.size - 1
      val fmore = Future.sequence(List.fill(math.max(0, needed - created.size))(allocator.createDataObjectPointer()))
      fmore.flatMap: more =>
        val pointers = (created ++ more).take(needed)
        if multiSplitFits(minimum, link, runs, pointers, maxNodeSize) then
          Future.successful((runs, pointers))
        else if remaining == 0 then
          Future.failed(new NodeSizeExceeded)
        else
          attempt(pointers.maxBy(_.encodedSize), created ++ more, remaining - 1)

    attempt(standIn, List(standIn), MultiSplitAttempts)

  /** Stages the allocation of every new node of a multi-split, the last taking `link`. Returns the link to the first */
  private def stageNewNodes(link: Option[NodeLink],
                            runs: List[List[(Key, Value)]],
                            pointers: List[DataObjectPointer],
                            allocator: ObjectAllocator)(using Transaction): Option[NodeLink] =
    multiSplitNodes(Key.AbsoluteMinimum, link, runs, pointers).tail.zip(pointers).foreach: (node, ptr) =>
      val (min, l, entries) = node
      allocator.stageDataObjectAllocation(ptr, DataBuffer(encode(entries, min, l)))
    Some(NodeLink(runs(1).head._1, pointers.head))

  private def prepareNewNodes(runs: List[List[(Key, Value)]],
                              pointers: List[DataObjectPointer],
                              prepareForSplit: (Key, ObjectPointer) => Future[Unit])
                             (using ExecutionContext): Future[Unit] =
    runs.tail.zip(pointers).foldLeft(Future.unit): (prev, t) =>
      prev.flatMap(_ => prepareForSplit(t._1.head._1, t._2))

  def allocateChain(allocator: ObjectAllocator,
                    ordering: KeyOrdering,
                    entries: Map[Key, Value],
                    maxNodeSize: Int,
                    prepareForSplit: (Key, ObjectPointer) => Future[Unit])(using Transaction): Future[ObjectPointer] =
    given ExecutionContext = allocator.client.clientContext
    val sorted = entries.toList.sortWith((a, b) => ordering.compare(a._1, b._1) < 0)

    allocator.createDataObjectPointer().flatMap: head =>
      if encodedSize(sorted, Key.AbsoluteMinimum, None) <= maxNodeSize then
        allocator.stageDataObjectAllocation(head, DataBuffer(encode(sorted, Key.AbsoluteMinimum, None)))
        Future.successful(head)
      else
        allocator.createDataObjectPointer().flatMap: standIn =>
          multiSplit(Key.AbsoluteMinimum, None, sorted, standIn, maxNodeSize, allocator).flatMap: (runs, pointers) =>
            val first = stageNewNodes(None, runs, pointers, allocator)
            allocator.stageDataObjectAllocation(head, DataBuffer(encode(runs.head, Key.AbsoluteMinimum, first)))
            prepareNewNodes(runs, pointers, prepareForSplit).map(_ => head)

  /** As `KeyValueListNode.relink`: this node's entries are rewritten with `newLink`, or split in two if that does not
    * fit within `maxNodeSize`.
    */
  def relink(node: ListNode,
             newLink: Option[NodeLink],
             maxNodeSize: Int,
             allocator: ObjectAllocator,
             prepareForSplit: (Key, ObjectPointer) => Future[Unit])(using tx: Transaction): Future[Unit] =
    val n = dataNode(node)
    given ExecutionContext = n.reader.client.clientContext

    planRelink(n.content, newLink, maxNodeSize) match
      case Rewrite(entries) =>
        writeNode(n, encode(entries, n.minimum, newLink))
        Future.unit

      case NeedsSplit(all) =>
        allocator.createDataObjectPointer().flatMap: rightPointer =>
          stageSplit(n, planSplit(n.minimum, newLink, all, rightPointer, maxNodeSize), newLink, rightPointer,
            allocator, prepareForSplit)

  /** The two-node scheme of `KeyValueListNode.splitAt`. This node keeps the lower keys and loses its link.
    *
    * Nodes store no maximum apart from their link, so after `splitTree` the left tree's last node has no maximum and
    * keeps accepting keys at or above `splitAtKey`. A KeyValue left tree keeps the split key as its stored maximum and
    * aborts such inserts.
    *
    * The new right node's minimum is `splitAtKey`, which may be longer than this node's minimum, so the right node
    * can be larger than this node was. Nothing is staged or allocated and `NodeSizeExceeded` is thrown if it would
    * not fit within `maxNodeSize`.
    */
  def splitAt(node: ListNode,
              splitAtKey: Key,
              inclusive: Boolean,
              maxNodeSize: Int,
              optionalDownPointer: Option[ObjectPointer],
              allocator: ObjectAllocator)(using tx: Transaction): Future[NodeLink] =
    val n = dataNode(node)
    given ExecutionContext = n.reader.client.clientContext

    require(n.ordering.compare(splitAtKey, Key.AbsoluteMinimum) > 0)

    val (left, right) = planSplitAt(n.content, n.ordering, splitAtKey, inclusive, maxNodeSize)
    val headEntries = optionalDownPointer.map(p => Key.AbsoluteMinimum -> Value(dataPointer(p).toArray)).toList

    writeNode(n, encode(left, n.minimum, None))

    for
      rightPointer <- allocator.allocateDataObject(DataBuffer(encode(right, splitAtKey, n.link)))
      headPointer <- allocator.allocateDataObject(DataBuffer(
        encode(headEntries, Key.AbsoluteMinimum, Some(NodeLink(splitAtKey, rightPointer)))))
    yield
      NodeLink(Key.AbsoluteMinimum, headPointer)

  def allocate(allocator: ObjectAllocator, ordering: KeyOrdering, entries: Map[Key, Value])
              (using Transaction): Future[ObjectPointer] =
    val sorted = entries.toList.sortWith((a, b) => ordering.compare(a._1, b._1) < 0)
    allocator.allocateDataObject(DataBuffer(encode(sorted, Key.AbsoluteMinimum, None)))

  /** As `KeyValueListNode.deleteList`: empties and deletes the nodes right of `head` one at a time, relinking `head`
    * past each, then empties and deletes `head`. With `odeleteEntry`, each entry is handed to it before being removed
    * in a transaction of its own. Nodes that are already deleted are skipped.
    */
  def deleteList(client: AspenClient,
                 head: NodeLink,
                 ordering: KeyOrdering,
                 odeleteEntry: Option[(Key, Value) => Future[Unit]]): Future[Unit] =
    given ExecutionContext = client.clientContext

    val headPointer = dataPointer(head.pointer)

    def readNode(ptr: DataObjectPointer): Future[DataObjectListNode] =
      client.read(ptr, s"Deleting DataObject TKVL node ${ptr.id}").map(s => new DataObjectListNode(client, ordering, s))

    def readOptionalNode(ptr: DataObjectPointer): Future[Option[DataObjectListNode]] =
      readNode(ptr).map(Some(_)).recover:
        case _: InvalidObject => None

    def rewrite(node: DataObjectListNode, entries: List[(Key, Value)]): Future[Unit] =
      val tx = client.newTransaction()
      writeNode(node, encode(entries, node.minimum, node.link))(using tx)
      tx.commit().map(_ => ())

    def deleteOneAtATime(ptr: DataObjectPointer,
                         entries: List[(Key, Value)],
                         deleteEntry: (Key, Value) => Future[Unit]): Future[Unit] =
      entries match
        case Nil => Future.unit
        case (k, v) :: rest =>
          for
            _ <- Future.delegate(deleteEntry(k, v))
            node <- readNode(ptr)
            _ <- rewrite(node, node.entries.filterNot(_._1 == k))
            _ <- deleteOneAtATime(ptr, rest, deleteEntry)
          yield
            ()

    // Empties the node, if it still exists
    def deleteContents(ptr: DataObjectPointer): Future[Unit] =
      readNode(ptr).flatMap { node =>
        odeleteEntry match
          case None => if node.entries.isEmpty then Future.unit else rewrite(node, Nil)
          case Some(deleteEntry) => deleteOneAtATime(ptr, node.entries, deleteEntry)
      }.recover {
        case _: InvalidObject => ()
      }.andThen:
        case Failure(err) => logger.warn(s"Failed to delete DataObject TKVL node content ${ptr.id}: $err")

    // Deletes the empty node at `ptr`, if it still exists, and links `headNode` past it
    def deleteNode(ptr: DataObjectPointer, headNode: DataObjectListNode): Future[Unit] =
      readNode(ptr).flatMap { node =>
        val tx = client.newTransaction()

        if node.entries.nonEmpty then
          tx.abortAndThrow(new Exception("Node is not empty"))

        // The revision lock ensures nothing is inserted or joined into the node while it is being deleted
        tx.lockRevision(node.pointer, node.revision)
        tx.setRefcount(node.pointer, node.refcount, node.refcount.decrement())
        writeNode(headNode, encode(headNode.entries, headNode.minimum, node.link))(using tx)
        tx.commit().map(_ => ())
      }.recover:
        case _: InvalidObject => ()

    // Deletes the node `head` currently links to. False ends the walk: either `head` has no link left or it is
    // already deleted. The link is read from `head` each time rather than remembered from the previous step, so a
    // step whose commit outcome was lost cannot end the walk early.
    def deleteRight(): Future[Boolean] =
      client.retryStrategy.retryUntilSuccessful:
        // reread the head node each time to ensure an accurate revision
        readOptionalNode(headPointer).flatMap:
          case None => Future.successful(false)
          case Some(headNode) =>
            headNode.link match
              case None => Future.successful(false)
              case Some(link) =>
                val ptr = dataPointer(link.pointer)
                deleteContents(ptr).flatMap(_ => deleteNode(ptr, headNode)).map(_ => true)

    def deleteRights(): Future[Unit] =
      deleteRight().flatMap: more =>
        if more then deleteRights() else Future.unit

    def deleteHead(): Future[Unit] =
      client.retryStrategy.retryUntilSuccessful:
        for
          _ <- deleteContents(headPointer)
          onode <- readOptionalNode(headPointer)
          _ <- onode match
            case None => Future.unit
            case Some(node) =>
              val tx = client.newTransaction()
              if node.entries.nonEmpty then
                tx.abortAndThrow(new Exception("Node is not empty"))
              tx.lockRevision(node.pointer, node.revision)
              tx.setRefcount(node.pointer, node.refcount, node.refcount.decrement())
              tx.commit().map(_ => ())
        yield
          ()

    readOptionalNode(headPointer).flatMap:
      case None => Future.unit
      case Some(_) => deleteRights().flatMap(_ => deleteHead())

  def nodeSizeLimit(ida: IDA, maxSliceSize: Option[Int]): Option[Int] =
    NodeAllocator.nodeSizeLimit(ida, maxSliceSize, TreeKind.DataObjects)
