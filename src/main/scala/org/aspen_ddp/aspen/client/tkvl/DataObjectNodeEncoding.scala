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

  /** As `KeyValueListNode.insert`, rewriting the whole node. The split also throws `NodeSizeExceeded`, after the
    * new right node is allocated, if the left node with its new link would still exceed `maxNodeSize`.
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

    if entrySize(key, value) > maxNodeSize - nodeOverhead(n.minimum, n.link) then
      throw new NodeSizeExceeded

    val all = withEntry(n.entries, key, value, n.ordering)

    if encodedSize(all, n.minimum, n.link) <= maxNodeSize then
      writeNode(n, encode(all, n.minimum, n.link))
      Future.unit
    else
      // The moved keys are always a suffix of `all`
      val moveCount = SplitPlan.keysToMove(all.map((k, v) => k -> entrySize(k, v))).size
      val (left, right) = all.splitAt(all.size - moveCount)
      val newMinimum = right.head._1
      val rightContent = encode(right, newMinimum, n.link)

      if rightContent.length > maxNodeSize then
        throw new NodeSizeExceeded

      allocator.allocateDataObject(DataBuffer(rightContent)).flatMap: rightPointer =>
        val leftLink = Some(NodeLink(newMinimum, rightPointer))
        if encodedSize(left, n.minimum, leftLink) > maxNodeSize then
          tx.abortAndThrow(new NodeSizeExceeded)

        // The new right node is already staged
        abortIfAlreadyModified(tx)(writeNode(n, encode(left, n.minimum, leftLink)))
        prepareForSplit(newMinimum, rightPointer)

  /** As `KeyValueListNode.delete`. A join rewrites this node with the right node's entries and link, locks the right
    * node's revision and drops its refcount.
    *
    * The joined node takes this node's minimum, which may be longer than the right node's, so it can be larger than
    * the right node. No size limit is available here, so the join happens only when the joined node is no larger
    * than the right node is now. Otherwise this node is left empty, keeping its link, and no join is made.
    */
  def delete(node: ListNode, key: Key, prepareForJoin: (Key, ObjectPointer) => Future[Unit])
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
            if encodedSize(r.entries, n.minimum, r.link) > rs.size then
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

  /** The two-node scheme of `KeyValueListNode.splitAt`. This node keeps the lower keys and loses its link.
    *
    * Nodes store no maximum apart from their link, so after `splitTree` the left tree's last node has no maximum and
    * keeps accepting keys at or above `splitAtKey`. A KeyValue left tree keeps the split key as its stored maximum and
    * aborts such inserts.
    *
    * Limitation: the new right node's minimum is `splitAtKey`, which may be longer than this node's minimum, so the
    * right node can be larger than this node was and is not checked against `maxNodeSize`, which is not available
    * here. The KeyValue encoding has the same limitation.
    */
  def splitAt(node: ListNode,
              splitAtKey: Key,
              inclusive: Boolean,
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

    writeNode(n, encode(left, n.minimum, None))

    for
      // TODO: the right node is not checked against maxNodeSize (see the limitation above)
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

    // Empties the node and returns its link, or None if it is already deleted
    def deleteContents(ptr: DataObjectPointer): Future[Option[NodeLink]] =
      readNode(ptr).flatMap { node =>
        val fdeleted = odeleteEntry match
          case None => if node.entries.isEmpty then Future.unit else rewrite(node, Nil)
          case Some(deleteEntry) => deleteOneAtATime(ptr, node.entries, deleteEntry)
        fdeleted.map(_ => node.link)
      }.recover {
        case _: InvalidObject => None
      }.andThen:
        case Failure(err) => logger.warn(s"Failed to delete DataObject TKVL node content ${ptr.id}: $err")

    // Deletes the empty node at `ptr` and links `headNode` past it. Returns the link `head` now has.
    def deleteNode(ptr: DataObjectPointer,
                   headNode: DataObjectListNode,
                   olink: Option[NodeLink]): Future[Option[NodeLink]] =
      readNode(ptr).flatMap { node =>
        val tx = client.newTransaction()

        if node.entries.nonEmpty then
          tx.abortAndThrow(new Exception("Node is not empty"))

        // The revision lock ensures nothing is inserted or joined into the node while it is being deleted
        tx.lockRevision(node.pointer, node.revision)
        tx.setRefcount(node.pointer, node.refcount, node.refcount.decrement())
        writeNode(headNode, encode(headNode.entries, headNode.minimum, node.link))(using tx)
        tx.commit().map(_ => node.link)
      }.recover:
        case _: InvalidObject => olink

    // None ends the walk: either the list is done or `head` is already deleted
    def deleteRight(link: NodeLink): Future[Option[NodeLink]] =
      client.retryStrategy.retryUntilSuccessful:
        // reread the head node each time to ensure an accurate revision
        readOptionalNode(headPointer).flatMap:
          case None => Future.successful(None)
          case Some(headNode) =>
            val ptr = dataPointer(link.pointer)
            deleteContents(ptr).flatMap(olink => deleteNode(ptr, headNode, olink))

    def deleteRights(olink: Option[NodeLink]): Future[Unit] = olink match
      case None => Future.unit
      case Some(link) => deleteRight(link).flatMap(deleteRights)

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
      case Some(headNode) => deleteRights(headNode.link).flatMap(_ => deleteHead())

  def nodeSizeLimit(ida: IDA, maxSliceSize: Option[Int]): Option[Int] =
    NodeAllocator.nodeSizeLimit(ida, maxSliceSize, TreeKind.DataObjects)
