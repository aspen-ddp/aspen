package org.aspen_ddp.aspen.client.tkvl

import java.nio.ByteBuffer

import org.aspen_ddp.aspen.client.{AspenClient, ConflictingRequirements, InvalidObject, MultipleDataUpdatesToObject, MultipleRefcountUpdatesToObject, ObjectAllocator, ObjectReader, Transaction}
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Key, KeyOrdering, ObjectPointer, ObjectRevision, Value}
import org.aspen_ddp.aspen.common.util.Varint
import scribe.Logging

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Failure

/** Nodes stored as DataObjects. A DataObject has no side fields, so the node's minimum and right link live in its
  * bytes. Every update is a whole-node rewrite guarded by the node's revision; there are no per-entry revisions, so
  * insert requirements are not supported.
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

    if requirement.isDefined then
      throw new UnsupportedRequirement(kind)

    // The new node's pointer is not known yet, so this node's own pointer stands in for the link's
    val reserved = math.max(nodeOverhead(n.minimum, n.link), nodeOverhead(n.minimum, Some(NodeLink(key, n.pointer))))
    if entrySize(key, value) > maxNodeSize - reserved then
      throw new NodeSizeExceeded

    val all = withEntry(n.entries, key, value, n.ordering)

    if encodedSize(all, n.minimum, n.link) <= maxNodeSize then
      writeNode(n, encode(all, n.minimum, n.link))
      Future.unit
    else
      // Creating the pointer stages nothing, so the split can be sized with it before anything is staged
      allocator.createDataObjectPointer().flatMap: rightPointer =>
        val index = splitIndex(n, all, rightPointer, maxNodeSize).getOrElse(throw new NodeSizeExceeded)
        val (left, right) = all.splitAt(index)
        val newMinimum = right.head._1

        allocator.stageDataObjectAllocation(rightPointer, DataBuffer(encode(right, newMinimum, n.link)))

        // The new right node is already staged
        abortIfAlreadyModified(tx)(writeNode(n, encode(left, n.minimum, Some(NodeLink(newMinimum, rightPointer)))))
        prepareForSplit(newMinimum, rightPointer)

  /** Where to split `all`, the node's entries with the new one, which do not fit in `n`: the number of entries
    * `n` keeps, the rest moving to the new node at `rightPointer`. SplitPlan's choice comes first, then the other
    * split points nearest it. None if no split point leaves both nodes within `maxNodeSize`.
    */
  private def splitIndex(n: DataObjectListNode,
                         all: List[(Key, Value)],
                         rightPointer: DataObjectPointer,
                         maxNodeSize: Int): Option[Int] =
    val keys = all.map(_._1).toArray
    val sizes = all.map((k, v) => entrySize(k, v))
    val prefix = sizes.scanLeft(0)(_ + _).toArray
    val total = prefix.last

    def fits(i: Int): Boolean =
      val leftSize = nodeOverhead(n.minimum, Some(NodeLink(keys(i), rightPointer))) + prefix(i)
      val rightSize = nodeOverhead(keys(i), n.link) + total - prefix(i)
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
    * left empty, keeping its link, and no join is made, so the delete always succeeds.
    */
  def delete(node: ListNode, key: Key, maxNodeSize: Int, prepareForJoin: (Key, ObjectPointer) => Future[Unit])
            (using tx: Transaction): Future[Unit] =
    val n = dataNode(node)
    given ExecutionContext = n.reader.client.clientContext

    if n.get(key).isEmpty then
      Future.unit
    else
      val remaining = n.entries.filterNot(_._1 == key)
      n.link match
        case Some(right) if remaining.isEmpty =>
          n.reader.read(dataPointer(right.pointer),
            s"Joining DataObject TKVL node ${right.pointer.id}. Minimum: ${right.minimum}. target: $key").flatMap: rs =>
            val r = new DataObjectListNode(n.reader, n.ordering, rs)
            if encodedSize(r.entries, n.minimum, r.link) > maxNodeSize then
              writeNode(n, encode(Nil, n.minimum, n.link))
              Future.unit
            else
              writeNode(n, encode(r.entries, n.minimum, r.link))
              // This node's rewrite is already staged. The transaction rejects the lock if it already updates the
              // right node, and the refcount update if it already changes the right node's refcount.
              abortIfAlreadyModified(tx):
                try
                  tx.lockRevision(r.pointer, r.revision)
                  tx.setRefcount(r.pointer, r.refcount, r.refcount.decrement())
                catch
                  case _: ConflictingRequirements | _: MultipleRefcountUpdatesToObject =>
                    throw NodeAlreadyModified(r.pointer)
              prepareForJoin(right.minimum, right.pointer)

        case _ =>
          writeNode(n, encode(remaining, n.minimum, n.link))
          Future.unit

  def unlinkRight(node: ListNode)(using tx: Transaction): Unit =
    val n = dataNode(node)
    writeNode(n, encode(n.entries, n.minimum, None))

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

    def stays(k: Key): Boolean =
      if inclusive then
        n.ordering.compare(k, splitAtKey) <= 0
      else
        n.ordering.compare(k, splitAtKey) < 0

    val (left, right) = n.entries.partition(e => stays(e._1))
    val headEntries = optionalDownPointer.map(p => Key.AbsoluteMinimum -> Value(dataPointer(p).toArray)).toList

    if encodedSize(right, splitAtKey, n.link) > maxNodeSize then
      throw new NodeSizeExceeded

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
