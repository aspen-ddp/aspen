package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.client.KeyValueObjectState.ValueState
import org.aspen_ddp.aspen.client.{AspenClient, InvalidObject, KeyValueObjectState, ObjectAllocator, ObjectReader, StopRetrying, Transaction}
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.objects.*
import org.aspen_ddp.aspen.common.transaction.{KeyValueUpdate, RevisionLock}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.{DoesNotExist, FullContentLock, KeyObjectRevision, KeyRequirement, KeyRevision, WithinRange}
import org.aspen_ddp.aspen.server.store.KVObjectState
import org.aspen_ddp.aspen.common.util.Varint
import scribe.Logging

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

class KeyValueListNode(val reader: ObjectReader,
                       val ida: IDA,
                       val pointer: KeyValueObjectPointer,
                       val ordering: KeyOrdering,
                       val minimum: Key,
                       val revision: ObjectRevision,
                       val refcount: ObjectRefcount,
                       val contents: Map[Key, ValueState],
                       val tail: Option[KeyValueListPointer]) extends ListNode with Logging {

  given ExecutionContext = reader.client.clientContext

  def maximum: Option[Key] = tail.map(rp => rp.minimum)

  def link: Option[NodeLink] = tail.map(rp => NodeLink(rp.minimum, rp.pointer))

  def entries: List[(Key, Value)] =
    contents.iterator.map((k, vs) => (k, vs.value)).toList.sortWith((a, b) => ordering.compare(a._1, b._1) < 0)

  def get(key: Key): Option[Value] = contents.get(key).map(_.value)

  def size: Int = contents.size

  def refresh(): Future[KeyValueListNode] = reader.read(pointer, s"Refresh KVListNode node ${pointer.id}. Minimum: $minimum").map { kvos =>
    new KeyValueListNode(reader, kvos.ida, pointer, ordering, minimum,
      kvos.revision, kvos.refcount, kvos.contents, kvos.right.map(v => KeyValueListPointer(v.bytes)))
  }

  def keyInRange(key: Key): Boolean = {
    ordering.compare(key, minimum) >= 0 && maximum.forall( ordering.compare(key, _) < 0 )
  }

  def shape: KeyValueListNode.NodeShape =
    KeyValueListNode.NodeShape(pointer, ida, ordering, minimum, maximum, tail, contents)

  def fullContentLock: FullContentLock = FullContentLock(contents.iterator.map(t => KeyRevision(t._1, t._2.revision)).toList)

  def fetchContainingNode(target: Key,
                          blacklist: Set[ObjectId] = Set()): Future[KeyValueListNode] = {
    // exit immediately if the requested key is below the minimum range
    if (ordering.compare(target, minimum) < 0)
      return Future.failed(new BelowMinimumError(minimum, target))

    val p = Promise[KeyValueListNode]()

    def scan(right: KeyValueListPointer): Unit = reader.read(right.pointer, s"Scanning right KVListNode node ${right.pointer.id}. Minimum: ${right.minimum}. target: $target") onComplete {
      case Failure(err) => p.failure(err)

      case Success(kvos) =>
        val node = new KeyValueListNode(reader, kvos.ida, right.pointer, ordering, right.minimum,
          kvos.revision, kvos.refcount, kvos.contents, kvos.right.map(v => KeyValueListPointer(v.bytes)))

        node.tail match {
          case None =>
            p.success(node)
          case Some(next) =>
            if (node.keyInRange(target) || blacklist.contains(next.pointer.id))
              p.success(node)
            else
              scan(next)
        }
    }

    tail match {
      case None => p.success(this)
      case Some(ptr) =>
        if (keyInRange(target) || blacklist.contains(ptr.pointer.id))
          p.success(this)
        else
          scan(ptr)
    }

    p.future
  }

  def fetch(key: Key): Future[Option[ValueState]] = fetchContainingNode(key).map { node =>
    node.contents.get(key)
  }

  def insert(key: Key,
             value: Value,
             maxNodeSize: Int,
             allocator: ObjectAllocator,
             prepareForSplit: (Key, KeyValueObjectPointer) => Future[Unit] = (_,_) => Future.unit,
             requirement: Option[Either[Boolean, ObjectRevision]] = None
            )(using tx: Transaction): Future[Unit] = fetchContainingNode(key).flatMap { node =>
    logger.trace(s"KeyValueListNode got containing node for key $key. Min: ${node.minimum}")
    KeyValueListNode.insert(node, ordering, key, value, maxNodeSize, allocator, prepareForSplit, requirement)
  }

  def rename(oldKey: Key,
             newKey: Key,
             maxNodeSize: Int,
             allocator: ObjectAllocator,
             prepareForSplit: (Key, KeyValueObjectPointer) => Future[Unit] = (_,_) => Future.unit,
            )(using tx: Transaction): Future[Unit] = fetchContainingNode(oldKey).flatMap { node =>
    KeyValueListNode.rename(node, ordering, oldKey, newKey, maxNodeSize, allocator, prepareForSplit)
  }

  /** Deletes `key`. The result is true if the node is left empty, as when its join with the right node was
    * declined. See `KeyValueListNode.delete`.
    */
  def delete(key: Key,
             prepareForJoin: (Key, KeyValueObjectPointer) => Future[Unit] = (_,_) => Future.unit
            )(using tx: Transaction): Future[Boolean] = fetchContainingNode(key).flatMap { node =>
    KeyValueListNode.delete(node, key, reader, prepareForJoin)
  }

  def delete(key: Key,
             requiredRevision: Option[ObjectRevision],
             requirements: List[KeyValueUpdate.KeyRequirement],
             prepareForJoin: (Key, KeyValueObjectPointer) => Future[Unit]
            )(using tx: Transaction): Future[Boolean] = fetchContainingNode(key).flatMap { node =>
    KeyValueListNode.delete(node, key, requiredRevision, requirements, reader, prepareForJoin)
  }

  def splitAt(ordering: KeyOrdering,
              splitAtKey: Key,
              inclusive: Boolean,
              optionalDownPointer: Option[KeyValueObjectPointer],
              allocator: ObjectAllocator,
             )(using tx: Transaction): Future[KeyValueListPointer] = fetchContainingNode(splitAtKey).flatMap { node =>
    KeyValueListNode.splitAt(node, ordering, splitAtKey, inclusive, optionalDownPointer, allocator)
  }

  /** Reads the node `nodeTail` points at. Every rightward walk advances through here.
   *
   *  `ctx` is the diagnostic handed to the reader. Callers name themselves and the node they
   *  are advancing *from*.
   */
  private def readNext(nodeTail: KeyValueListPointer, ctx: String): Future[KeyValueListNode] =
    reader.read(nodeTail.pointer, ctx).map { kvos =>
      new KeyValueListNode(reader, kvos.ida, kvos.pointer, ordering, nodeTail.minimum,
        kvos.revision, kvos.refcount, kvos.contents,
        kvos.right.map(v => KeyValueListPointer(v.bytes)))
    }

  def foldLeft[B](initialZ: B)(fn: (B, Map[Key, ValueState]) => B): Future[B] = {
    val p = Promise[B]()

    def recurse(z: B, node: KeyValueListNode): Unit = {
      val newz = fn(z, node.contents)

      node.tail match {
        case None => p.success(newz)

        case Some(nodeTail) =>
          readNext(nodeTail, s"foldLeft() KVListNode node ${node.pointer.id}. Minimum: ${node.minimum}.") onComplete {

            case Failure(err) => p.failure(err)

            case Success(nextNode) => recurse(newz, nextNode)
          }
      }
    }

    recurse(initialZ, this)

    p.future
  }

  /** Visit every entry in this node and in every node to its right.
   *
   *  Exactly `foreachFrom(Key.AbsoluteMinimum, fn)`: all three KeyOrderings compare every key
   *  as greater than or equal to the empty key, so foreachFrom's filter is a total no-op at
   *  that bound.
   *
   *  A failing `fn` is logged and the walk continues to the next key. A failing read fails the
   *  returned future.
   */
  def foreach(fn: (KeyValueListNode, Key, ValueState) => Future[Unit]): Future[Unit] =
    walkFrom(Key.AbsoluteMinimum, fn, "foreach")

  /** Visit every entry at or above `minKey`, from this node rightward.
   *
   *  The open-ended counterpart to foreachInRange: keys are arbitrary-length byte arrays, so
   *  there is no maximum key that could serve as an upper bound.
   *
   *  Every node's contents are filtered, not just the head node's. When reached through
   *  TieredKeyValueList.foreachFrom the filter is a no-op past the first node, but filtering
   *  unconditionally keeps this correct when called directly on a node to the left of minKey.
   *
   *  As with foreach and foreachInRange, a failing `fn` is logged and the walk continues.
   */
  def foreachFrom(minKey: Key,
                  fn: (KeyValueListNode, Key, ValueState) => Future[Unit]): Future[Unit] =
    walkFrom(minKey, fn, "foreachFrom")

  /** Shared body of foreach and foreachFrom.
   *
   *  `label` is the name of the method the caller actually invoked; it appears in both the
   *  read diagnostics and the `fn`-failure log so those stay accurate across the two entry
   *  points.
   */
  private def walkFrom(minKey: Key,
                       fn: (KeyValueListNode, Key, ValueState) => Future[Unit],
                       label: String): Future[Unit] =
    val p = Promise[Unit]()

    def inRange(node: KeyValueListNode): List[(Key, ValueState)] =
      node.contents.filter(tpl => ordering.compare(tpl._1, minKey) >= 0)
        .toList.sortWith((a, b) => ordering.compare(a._1, b._1) < 0)

    // The only place a node is paired with a list of contents. Routing every node through a
    // function of one argument leaves the walk nowhere to pair a node with some other node's
    // contents, which is the defect this branch fixed twice.
    def advance(node: KeyValueListNode): Unit = recurse(node, inRange(node))

    def recurse(node: KeyValueListNode, contents: List[(Key, ValueState)]): Unit = {
      contents.headOption match
        case Some((key, value)) =>
          fn(node, key, value) onComplete:
            case Failure(err) =>
              logger.error(f"Failure in KeyValueListNode.$label: $err", err)
              recurse(node, contents.tail)
            case Success(_) =>
              recurse(node, contents.tail)

        case None =>
          node.tail match
            case None =>
              p.success(())

            case Some(nodeTail) =>
              readNext(nodeTail, s"$label() KVListNode node ${node.pointer.id}. Minimum: ${node.minimum}.") onComplete {

                case Failure(err) =>
                  p.failure(err)

                case Success(nextNode) =>
                  advance(nextNode)
              }
    }

    advance(this)

    p.future

  /** Visit every entry in `[minKey, maxKey)`, from this node rightward.
   *
   *  Kept separate from walkFrom rather than folded into it behind an optional upper bound:
   *  the bounded and unbounded walks are easier to tell apart as distinct methods.
   *
   *  As with foreach and foreachFrom, a failing `fn` is logged and the walk continues.
   */
  def foreachInRange(minKey: Key,
                     maxKey: Key,
                     fn: (KeyValueListNode, Key, ValueState) => Future[Unit]): Future[Unit] =
    val p = Promise[Unit]()

    def inRange(node: KeyValueListNode): List[(Key, ValueState)] =
      node.contents
        .filter(tpl => ordering.compare(tpl._1, minKey) >= 0 && ordering.compare(tpl._1, maxKey) < 0)
        .toList.sortWith((a, b) => ordering.compare(a._1, b._1) < 0)

    // As in walkFrom: the only place a node is paired with a list of contents.
    def advance(node: KeyValueListNode): Unit = recurse(node, inRange(node))

    def recurse(node: KeyValueListNode, contents: List[(Key, ValueState)]): Unit = {
      contents.headOption match
        case Some((key, value)) =>
          fn(node, key, value) onComplete:
            case Failure(err) =>
              logger.error(f"Failure in KeyValueListNode.foreachInRange: $err", err)
              recurse(node, contents.tail)
            case Success(_) =>
              recurse(node, contents.tail)

        case None =>
          node.tail match
            case None =>
              p.success(())

            case Some(nodeTail) =>
              // nodeTail.minimum is the minimum key of the node we are about to read. The
              // range is [minKey, maxKey), so a next node whose minimum is at or above maxKey
              // holds nothing in range and neither does anything to its right.
              if ordering.compare(maxKey, nodeTail.minimum) <= 0 then
                p.success(())
              else
                readNext(nodeTail, s"foreachInRange() KVListNode node ${node.pointer.id}. Minimum: ${node.minimum}.") onComplete {

                  case Failure(err) =>
                    p.failure(err)

                  case Success(nextNode) =>
                    advance(nextNode)
                }
    }

    advance(this)

    p.future
}

object KeyValueListNode extends Logging {
  def apply(reader: ObjectReader,
            pointer: KeyValueListPointer,
            ordering: KeyOrdering,
            kvos: KeyValueObjectState): KeyValueListNode = {
    new KeyValueListNode(reader, kvos.ida, pointer.pointer, ordering, pointer.minimum, kvos.revision,
      kvos.refcount, kvos.contents, kvos.right.map(v => KeyValueListPointer(v.bytes)))
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Planning. Everything in this section is a pure function of the node's shape: it decides what an insert, rename,
  // delete or relink must do (fit in place, split, join, or fail) and returns that as a plan, touching no Transaction,
  // allocator or reader.
  //
  // This separation exists FOR TESTING. The size arithmetic and split/join decisions are subtle (a split changes the
  // overhead of both nodes, a join changes the minimum, and every node must stay within the store's slice limit), and
  // they are easy to get wrong in ways only many edge cases reveal. Written inline with the transaction calls, they
  // could only be exercised through a full Aspen system, which is slow and cannot explore enough cases. As plain
  // functions they are tested directly, over thousands of randomized operations, in KeyValueNodePlanSuite. Keep
  // decision logic here and keep the executors below limited to applying a plan.

  /** What the planning functions need to know about a node */
  final case class NodeShape(pointer: KeyValueObjectPointer,
                             ida: IDA,
                             ordering: KeyOrdering,
                             minimum: Key,
                             maximum: Option[Key],
                             tail: Option[KeyValueListPointer],
                             contents: Map[Key, ValueState])

  /** The outcome of planning an insert or rename */
  sealed abstract class ChangePlan

  /** The change fits in the node. `ops` are applied to it as is */
  final case class InPlace(requirements: List[KeyRequirement], ops: List[KeyValueOperation]) extends ChangePlan

  /** The node must split. The keys of `newContent` move to a new node allocated with `newMinimum` as its minimum
    * and `newTail` as its right pointer. Once allocated, the node is updated with `SetMax(newMinimum)`,
    * `SetRight(pointer to the new node)` and `leftOps`.
    */
  final case class Split(requirements: List[KeyRequirement],
                         newMinimum: Key,
                         newContent: Map[Key, Value],
                         newTail: Option[Value],
                         leftOps: List[KeyValueOperation]) extends ChangePlan

  /** The outcome of planning a delete once the last key of a node is known to need the right neighbor */
  sealed abstract class DeletePlan

  /** The key is not in the node */
  case object NothingToDelete extends DeletePlan

  /** Delete the key from the node. If it was the node's last key the node is left empty */
  final case class RemoveKey(key: Key) extends DeletePlan

  /** The node's last key is deleted and the node needs its right neighbor to decide whether to join with it */
  final case class ConsiderJoin(right: KeyValueListPointer) extends DeletePlan

  /** The right neighbor, as read, that a node may absorb */
  final case class JoinSource(pointer: KeyValueListPointer,
                              maximum: Option[Key],
                              right: Option[KeyValueListPointer],
                              contents: Map[Key, ValueState])

  /** Bytes of a node's stored slice that are not content entries, counted exactly as the store measures a slice
    * (`KVObjectState.encodedSizeIDA`): the mask byte, the minimum and maximum keys, the IDA-encoded right pointer
    * and the content-count varint, sized for `contentCount + 1` entries. The minimum is always counted, even for a
    * node that does not store one. Content pairs are sized with `KVObjectState.idaEncodedPairSize`, which also
    * matches the store, so a node whose pairs fit in `maxNodeSize - nodeOverhead(...)` has slices of at most
    * `maxNodeSize` bytes. This is what lets `NodeAllocator` compare `maxNodeSize` directly against a slice limit.
    */
  def nodeOverhead(ida: IDA,
                   minimum: Key,
                   maximum: Option[Key],
                   tail: Option[KeyValueListPointer],
                   contentCount: Int): Int =
    exactOverhead(ida, minimum, maximum, tail, contentCount + 1)

  /** As `nodeOverhead` for a node holding exactly `contentCount` entries */
  private def exactOverhead(ida: IDA,
                            minimum: Key,
                            maximum: Option[Key],
                            tail: Option[KeyValueListPointer],
                            contentCount: Int): Int =
    val withoutContent = KVObjectState.encodedSizeIDA(ida, Some(minimum), maximum, None,
      tail.map(p => Value(p.toArray)), Map())
    withoutContent - Varint.getUnsignedIntEncodingLength(0) + Varint.getUnsignedIntEncodingLength(contentCount)

  private def pairsSize(ida: IDA, contents: Map[Key, ValueState]): Int =
    contents.foldLeft(0)((sz, t) => sz + KVObjectState.idaEncodedPairSize(ida, t._1, t._2.value))

  /** Placeholder for an entry that is not stored yet. Only its value's size matters to the planning functions */
  private def pendingEntry(value: Value): ValueState = ValueState(value, ObjectRevision.Null, HLCTimestamp.now)

  /** The keys that move to the new right node when a node splits with `fullContent` (the node's content plus the
    * pending change). Starts from `SplitPlan.keysToMove`, which balances the pairs alone. Splitting also changes
    * the overhead of both nodes: the left node's stored maximum and right pointer become the new minimum and a
    * pointer carrying it, and the right node's minimum becomes the new minimum. When a long new minimum leaves
    * either node above `maxNodeSize`, the split point nearest the balanced one that fits both nodes is used.
    * Throws NodeSizeExceeded if no split point fits.
    */
  def planSplit(shape: NodeShape,
                fullContent: Map[Key, ValueState],
                maxNodeSize: Int): List[Key] = {
    val keys = fullContent.keysIterator.toArray

    scala.util.Sorting.quickSort(keys)(using shape.ordering)

    val sizes = keys.iterator.map { k =>
      (k, KVObjectState.idaEncodedPairSize(shape.ida, k, fullContent(k).value))
    }.toList

    // Both nodes must fit with the split at `index`: the left node keeps keys(0 until index)
    def fits(index: Int): Boolean = {
      val newMinimum = keys(index)
      val leftPairs = sizes.iterator.take(index).map(_._2).sum
      val rightPairs = sizes.iterator.drop(index).map(_._2).sum
      // The new node's pointer is not known yet. This node's is the same size for a node of the same pool
      val rightPtr = KeyValueListPointer(newMinimum, shape.pointer)
      val leftOverhead = nodeOverhead(shape.ida, shape.minimum, Some(newMinimum), Some(rightPtr), index)
      val rightOverhead = nodeOverhead(shape.ida, newMinimum, None, shape.tail, keys.length - index)
      leftPairs + leftOverhead <= maxNodeSize && rightPairs + rightOverhead <= maxNodeSize
    }

    val preferred = keys.length - SplitPlan.keysToMove(sizes).length

    if (keys.length > 1 && preferred >= 1 && fits(preferred))
      keys.iterator.drop(preferred).toList
    else {
      val candidates = (1 until keys.length).filter(fits)

      if (candidates.isEmpty)
        throw new NodeSizeExceeded

      keys.iterator.drop(candidates.minBy(i => math.abs(i - preferred))).toList
    }
  }

  /** Plans an insert of `key`/`value`. Throws NodeSizeExceeded if the pair cannot fit in any node and
    * KeyAlreadyExists if `requirement` demands the key be new and it is not
    */
  def planInsert(shape: NodeShape,
                 key: Key,
                 value: Value,
                 maxNodeSize: Int,
                 requirement: Option[Either[Boolean, ObjectRevision]]): ChangePlan = {
    val currentSize = pairsSize(shape.ida, shape.contents)
    val newPairSize = KVObjectState.idaEncodedPairSize(shape.ida, key, value)
    val maxSize = maxNodeSize - nodeOverhead(shape.ida, shape.minimum, shape.maximum, shape.tail, shape.contents.size)

    if (newPairSize > maxSize)
      throw new NodeSizeExceeded

    val reqs: List[KeyRequirement] = requirement match {
      case None => Nil
      case Some(Left(mustNotExist)) =>
        if (mustNotExist) {
          if (shape.contents.contains(key))
            throw new KeyAlreadyExists(key)
          DoesNotExist(key) :: Nil
        } else
          Nil
      case Some(Right(revision)) => KeyRevision(key, revision) :: Nil
    }

    // An overwrite replaces the existing pair rather than adding to it
    val replacedSize = shape.contents.get(key).map(vs => KVObjectState.idaEncodedPairSize(shape.ida, key, vs.value)).getOrElse(0)

    if (newPairSize + currentSize - replacedSize < maxSize)
      InPlace(WithinRange(key, shape.ordering) :: reqs, List(Insert(key, value.bytes)))
    else {
      val fullContent = shape.contents + (key -> pendingEntry(value))
      val moveList = planSplit(shape, fullContent, maxNodeSize)
      val oldOps = if (moveList.contains(key)) Nil else List(Insert(key, value.bytes))
      split(shape, reqs, fullContent, moveList, oldOps)
    }
  }

  /** Plans a rename of `oldKey`, which must be in the node, to `newKey` */
  def planRename(shape: NodeShape,
                 oldKey: Key,
                 newKey: Key,
                 maxNodeSize: Int): ChangePlan = {
    assert(shape.contents.contains(oldKey))

    val currentSize = pairsSize(shape.ida, shape.contents)
    val value = shape.contents(oldKey).value
    val oldPairSize = KVObjectState.idaEncodedPairSize(shape.ida, oldKey, value)
    val newPairSize = KVObjectState.idaEncodedPairSize(shape.ida, newKey, value)
    val maxSize = maxNodeSize - nodeOverhead(shape.ida, shape.minimum, shape.maximum, shape.tail, shape.contents.size)

    if (newPairSize > maxSize)
      throw new NodeSizeExceeded

    val reqs: List[KeyRequirement] = KeyRevision(oldKey, shape.contents(oldKey).revision) :: Nil

    // The old pair is removed, and a pair already stored under newKey is replaced
    val replacedSize = shape.contents.get(newKey).filter(_ => newKey != oldKey)
      .map(vs => KVObjectState.idaEncodedPairSize(shape.ida, newKey, vs.value)).getOrElse(0)

    if (newPairSize + currentSize - oldPairSize - replacedSize < maxSize)
      InPlace(WithinRange(newKey, shape.ordering) :: reqs, List(Delete(oldKey), Insert(newKey, value.bytes)))
    else {
      val fullContent = shape.contents - oldKey + (newKey -> pendingEntry(value))
      val moveList = planSplit(shape, fullContent, maxNodeSize)
      val oldOps =
        if (moveList.contains(newKey)) List(Delete(oldKey)) else List(Delete(oldKey), Insert(newKey, value.bytes))
      split(shape, reqs, fullContent, moveList, oldOps)
    }
  }

  private def split(shape: NodeShape,
                    reqs: List[KeyRequirement],
                    fullContent: Map[Key, ValueState],
                    moveList: List[Key],
                    oldOps: List[KeyValueOperation]): Split =
    Split(reqs,
      newMinimum = moveList.head,
      newContent = moveList.map(k => k -> fullContent(k).value).toMap,
      newTail = shape.tail.map(p => Value(p.toArray)),
      leftOps = oldOps ++ moveList.map(Delete(_)))

  /** Plans a delete of `key`. A node whose last key is deleted may absorb its right neighbor, which the caller
    * reads before calling `planJoin`.
    */
  def planDelete(shape: NodeShape, key: Key): DeletePlan =
    if (!shape.contents.contains(key))
      NothingToDelete
    else if (shape.contents.size > 1)
      RemoveKey(key)
    else
      shape.tail match {
        case None => RemoveKey(key)
        case Some(rp) => ConsiderJoin(rp)
      }

  /** Plans pointing `shape`, the predecessor of a node being removed from the list, at `newLink`, the removed node's
    * link. Only the stored maximum and right pointer change. Fits unless `newLink`'s minimum is longer than the
    * removed node's, which is not the case for a node whose join was declined but can be if its right neighbor changed
    * since. Then the node splits in the normal manner so that both halves, the second now pointing at `newLink`, fit.
    * Throws NodeSizeExceeded if no split point fits.
    */
  def planRelink(shape: NodeShape, newLink: Option[KeyValueListPointer], maxNodeSize: Int): ChangePlan = {
    val relinked = shape.copy(maximum = newLink.map(_.minimum), tail = newLink)
    val size = pairsSize(shape.ida, shape.contents) +
      exactOverhead(shape.ida, shape.minimum, relinked.maximum, relinked.tail, shape.contents.size)

    if (size <= maxNodeSize)
      InPlace(Nil, newLink match {
        case None => List(DeleteRight(), DeleteMax())
        case Some(l) => List(SetMax(l.minimum), SetRight(l.toArray))
      })
    else {
      val moveList = planSplit(relinked, shape.contents, maxNodeSize)
      split(relinked, Nil, shape.contents, moveList, Nil)
    }
  }

  /** Operations that delete `key` from the node and absorb the right node `source` into it, or None when the
    * joined node would be larger than `source` is, since only `source` is known to fit. That happens when the node's
    * minimum key is longer than the right node's. The node then simply loses the key.
    *
    * The node takes over the right node's right pointer and stored maximum so that later inserts into the joined
    * node pass the store's range check.
    */
  def planJoin(shape: NodeShape, key: Key, source: JoinSource): Option[List[KeyValueOperation]] = {
    val joinedOverhead = nodeOverhead(shape.ida, shape.minimum, source.maximum, source.right, source.contents.size)
    val sourceOverhead = nodeOverhead(shape.ida, source.pointer.minimum, source.maximum, source.right,
      source.contents.size)

    if (joinedOverhead > sourceOverhead)
      None
    else {
      val rightOp = source.right match {
        case None => DeleteRight()
        case Some(rp) => SetRight(rp.toArray)
      }
      val maxOp = source.maximum match {
        case None => DeleteMax()
        case Some(m) => SetMax(m)
      }
      val inserts = source.contents.iterator.map(t => Insert(t._1, t._2.value.bytes)).toList
      Some(maxOp :: rightOp :: Delete(key) :: inserts)
    }
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Executors. Apply a plan to a transaction. These only stage what the plan dictates (tx.update, allocation, reads
  // of the right neighbor, finalization callbacks); anything that decides what to do belongs in the planning section
  // above, where it can be tested without a Transaction.

  // Implemented as a non-class member to prevent accidental use of member variables instead of "node." attributes
  private[tkvl] def insert(node: KeyValueListNode,
                     ordering: KeyOrdering,
                     key: Key,
                     value: Value,
                     maxNodeSize: Int,
                     allocator: ObjectAllocator,
                     prepareForSplit: (Key, KeyValueObjectPointer) => Future[Unit] = (_,_) => Future.unit,
                     requirement: Option[Either[Boolean, ObjectRevision]] = None
                    )(using tx: Transaction, ec: ExecutionContext): Future[Unit] = {
    val plan =
      try planInsert(node.shape, key, value, maxNodeSize, requirement)
      catch case e: KeyAlreadyExists => tx.abortAndThrow(e)

    applyChange(node, plan, allocator, prepareForSplit)
  }

  // Implemented as a non-class member to prevent accidental use of member variables instead of "node." attributes
  private def rename(node: KeyValueListNode,
                     ordering: KeyOrdering,
                     oldKey: Key,
                     newKey: Key,
                     maxNodeSize: Int,
                     allocator: ObjectAllocator,
                     prepareForSplit: (Key, KeyValueObjectPointer) => Future[Unit] = (_,_) => Future.unit,
                    )(using tx: Transaction, ec: ExecutionContext): Future[Unit] =
    applyChange(node, planRename(node.shape, oldKey, newKey, maxNodeSize), allocator, prepareForSplit)

  private def applyChange(node: KeyValueListNode,
                          plan: ChangePlan,
                          allocator: ObjectAllocator,
                          prepareForSplit: (Key, KeyValueObjectPointer) => Future[Unit]
                         )(using tx: Transaction, ec: ExecutionContext): Future[Unit] = plan match {
    case InPlace(reqs, ops) =>
      tx.update(node.pointer, None, None, reqs, ops)
      Future.unit

    case Split(reqs, newMinimum, newContent, newTail, leftOps) =>
      allocator.allocateKeyValueObject(newContent, Some(newMinimum), None, None, newTail).flatMap { newObjectPointer =>
        val rightPtr = KeyValueListPointer(newMinimum, newObjectPointer)

        tx.update(node.pointer, Some(node.revision), Some(node.fullContentLock), reqs,
          SetMax(newMinimum) :: SetRight(rightPtr.toArray) :: leftOps)

        prepareForSplit(newMinimum, newObjectPointer)
      }
  }

  def delete(node: KeyValueListNode,
             key: Key,
             reader: ObjectReader,
             prepareForJoin: (Key, KeyValueObjectPointer) => Future[Unit] = (_,_) => Future.unit)(using tx: Transaction, ec: ExecutionContext): Future[Boolean] =
    delete(node, key, None, List(KeyValueUpdate.Exists(key)), reader, prepareForJoin)

  /** Deletes `key` from `node`. The result is true if the node is left empty: its last key was deleted and the
    * join with the right node was declined (see `planJoin`), or the right node was empty too. The caller removes
    * such a node from its list, see `TailRemovalFinalizationAction`.
    */
  def delete(node: KeyValueListNode,
             key: Key,
             requiredRevision: Option[ObjectRevision],
             requirements: List[KeyValueUpdate.KeyRequirement],
             reader: ObjectReader,
             prepareForJoin: (Key, KeyValueObjectPointer) => Future[Unit])(using tx: Transaction, ec: ExecutionContext): Future[Boolean] = {

    def remove(): Future[Boolean] = {
      tx.update(node.pointer, requiredRevision, None, requirements, List(Delete(key)))
      Future.successful(node.contents.size == 1)
    }

    planDelete(node.shape, key) match {
      case NothingToDelete => Future.successful(false)

      case RemoveKey(_) => remove()

      case ConsiderJoin(rp) =>
        reader.read(rp.pointer, s"Deleting key from KVListNode node ${rp.pointer.id}. Minimum: ${rp.minimum}. target: $key").flatMap { kvos =>
          val source = JoinSource(rp, kvos.maximum, kvos.right.map(v => KeyValueListPointer(v.bytes)), kvos.contents)

          planJoin(node.shape, key, source) match {
            case None => remove()

            case Some(ops) =>
              tx.update(node.pointer, Some(node.revision), None, requirements, ops)

              val rightContentLock = FullContentLock(kvos.contents.iterator.map(t => KeyRevision(t._1, t._2.revision)).toList)
              tx.update(rp.pointer, Some(kvos.revision), Some(rightContentLock), Nil, Nil)

              tx.setRefcount(rp.pointer, kvos.refcount, kvos.refcount.decrement())

              prepareForJoin(rp.minimum, rp.pointer).map(_ => kvos.contents.isEmpty)
          }
        }
    }
  }

  /** Points `node`, the predecessor of a node being removed from the list, at `newLink`, the removed node's link.
    * Splits `node` if the new link does not fit, calling `prepareForSplit` for the new node. See `planRelink`.
    */
  def relink(node: KeyValueListNode,
             newLink: Option[KeyValueListPointer],
             maxNodeSize: Int,
             allocator: ObjectAllocator,
             prepareForSplit: (Key, KeyValueObjectPointer) => Future[Unit]
            )(using tx: Transaction, ec: ExecutionContext): Future[Unit] =
    planRelink(node.shape, newLink, maxNodeSize) match {
      case InPlace(reqs, ops) =>
        tx.update(node.pointer, Some(node.revision), None, reqs, ops)
        Future.unit

      case split: Split =>
        applyChange(node, split, allocator, prepareForSplit)
    }

  def deleteList(client: AspenClient,
                 nodePointer: KeyValueListPointer,
                 ordering: KeyOrdering,
                 odeleteKVPair: Option[(Key, ValueState) => Future[Unit]])
                (implicit ec: ExecutionContext): Future[Unit] =

    def deleteNode(ptr: KeyValueListPointer,
                   nodeRevision: ObjectRevision,
                   optr: Option[KeyValueListPointer]): Future[Unit] =
      client.read(ptr.pointer).flatMap { kvos =>
        val tx = client.newTransaction()

        if kvos.contents.nonEmpty then
          tx.abortAndThrow(new Exception("Node is not empty"))

        val op = optr match
          case None => DeleteRight()
          case Some(ptr) => SetRight(ptr.toArray)

        // Lock full content to ensure nothing is inserted while we're trying to
        // delete the node
        tx.update(ptr.pointer, Some(kvos.revision), Some(FullContentLock(List())), Nil, Nil)
        tx.setRefcount(ptr.pointer, kvos.refcount, kvos.refcount.decrement())

        // Update the right pointer of the start node to point to the next node
        // in the chain
        tx.update(nodePointer.pointer, Some(nodeRevision), None, Nil, List(op))
        tx.commit().map(_ => ())
      }.recover:
        case _: InvalidObject => ()

    def deleteRight(ptr: KeyValueListPointer): Future[Option[KeyValueListPointer]] =
      client.retryStrategy.retryUntilSuccessful:
        for
          // reread the root node each time to ensure we have an accurate
          // object revision
          nodeKvos <- client.read(nodePointer.pointer).recover:
            case e:InvalidObject => throw StopRetrying(e)
          optr <- deleteContents(client, ptr, ordering, odeleteKVPair)
          _ <- deleteNode(ptr, nodeKvos.revision, optr)
        yield
          optr

    def deleteRootNode(): Future[Unit] =
      def tryDelete(kvos: KeyValueObjectState): Future[Unit] =
        val tx = client.newTransaction()
        if kvos.contents.nonEmpty then
          tx.abortAndThrow(new Exception("Node is not empty"))
        tx.update(kvos.pointer, Some(kvos.revision), Some(FullContentLock(List())), Nil, Nil)
        tx.setRefcount(kvos.pointer, kvos.refcount, kvos.refcount.decrement())
        tx.commit().map(_ => ())

      client.retryStrategy.retryUntilSuccessful:
        for
          _ <- deleteContents(client, nodePointer, ordering, odeleteKVPair)
          okvos <- client.readOptional(nodePointer.pointer)
          _ <- okvos match
            case None => Future.unit
            case Some(kvos) => tryDelete(kvos)
        yield
          ()

    val allRightNodesDeleted = Promise[Unit]()

    def recurse(optr: Option[KeyValueListPointer]): Unit =
      optr match
        case None => allRightNodesDeleted.success(())
        case Some(rptr) => deleteRight(rptr).map(recurse)

    for
      okvos <- client.readOptional(nodePointer.pointer)
      _ <- okvos match
        case None => Future.unit

        case Some(kvos) =>
          val node = KeyValueListNode(client, nodePointer, ordering, kvos)

          // Kick off potentially long recursion
          recurse(node.tail)

          allRightNodesDeleted.future.flatMap: _ =>
            deleteRootNode()
    yield
      ()

  def deleteContents(client: AspenClient,
                     nodePointer: KeyValueListPointer,
                     ordering: KeyOrdering,
                     odeleteKVPair: Option[(Key, ValueState) => Future[Unit]])
                    (implicit ec: ExecutionContext): Future[Option[KeyValueListPointer]] =

    def deleteOneShot(node: KeyValueListNode): Future[Unit] =
      val deleteOps = node.contents.keysIterator.map(Delete(_)).toList

      if deleteOps.isEmpty then
        Future.unit
      else
        val fullContentLock = FullContentLock(node.contents.iterator.map(t => KeyRevision(t._1, t._2.revision)).toList)
        val dtx = client.newTransaction()

        dtx.update(node.pointer, Some(node.revision), Some(fullContentLock), Nil, deleteOps)

        dtx.commit().map(_ => ())

    def deleteOneAtATime(node: KeyValueListNode,
                         deleteKVPair: (Key, ValueState) => Future[Unit]): Future[Unit] =
      val pdone = Promise[Unit]()

      def recurse(contents: List[(Key, ValueState)]): Unit =
        if contents.isEmpty then
          pdone.success(())
        else
          val (key, vs) = contents.head
          val fdeleted = Future.delegate(deleteKVPair(key, vs)).flatMap: _ =>
            val dtx = client.newTransaction()
            dtx.update(node.pointer, None, None, Nil, List(Delete(key)))
            dtx.commit()

          fdeleted.onComplete:
            case Failure(err) => pdone.failure(err)
            case Success(_) => recurse(contents.tail)

      recurse(node.contents.toList)

      pdone.future

    client.read(nodePointer.pointer).flatMap { kvos =>
      val node = KeyValueListNode(client, nodePointer, ordering, kvos)
      odeleteKVPair match
        case None => deleteOneShot(node).map(_ => node.tail)
        case Some(deleteKVPair) => deleteOneAtATime(node, deleteKVPair).map(_ => node.tail)
    } recover {
      case _: InvalidObject => None // already deleted
    } andThen:
      case Failure(err) =>
        logger.warn(s"Failed to delete KeyValueListNode content ${nodePointer.pointer.id}: $err")

  /**
   * Splits the list at the specified key. The original list node will have
   * its right pointer deleted and the newly allocated node will have its
   * minimum set to Key.AbsoluteMinimum. The nodes to the right of it will
   * be preserved as-is.
   *
   * Note:
   * There is a potential edge case where a full node is split with all
   * of the contents going into the right node. In this case, there is no
   * room left to insert the optional down pointer. To avoid this we can
   * always allocate 2 nodes. The optional down pointer will be always be
   * inserted into the new left node. If no optional pointer is provided,
   * the new left node will be empty. Minor performance issue but this
   * implementation is aimed at partitioning trees for the purpose of
   * slow deletion so it should be a non-issue.
   */
  private[tkvl] def splitAt(node: KeyValueListNode,
                      ordering: KeyOrdering,
                      splitAtKey: Key,
                      inclusive: Boolean,
                      optionalDownPointer: Option[KeyValueObjectPointer],
                      allocator: ObjectAllocator,
                     )(using tx: Transaction, ec: ExecutionContext): Future[KeyValueListPointer] = 

    require(ordering.compare(splitAtKey, Key.AbsoluteMinimum) > 0)

    def cmp(k: Key, v: ValueState): Boolean =
      if inclusive then
        ordering.compare(k, splitAtKey) <= 0
      else
        ordering.compare(k, splitAtKey) < 0
    
    val (leftContents, rightContents) = node.contents.partition(cmp)
    
    val newLeftContent = optionalDownPointer match
      case None => Map()
      case Some(dptr) => Map(Key.AbsoluteMinimum -> Value(dptr.toArray))

    tx.update(
      node.pointer,
      Some(node.revision),
      Some(node.fullContentLock),
      Nil,
      SetMax(splitAtKey) :: DeleteRight() :: rightContents.map((k,v) => Delete(k)).toList)
    
    for 
      newRightPtr <- allocator.allocateKeyValueObject(
        rightContents.map((k,vs) => (k -> vs.value)),
        Some(splitAtKey),
        None,
        None,
        node.tail.map(p => Value(p.toArray)))

      newLeftPtr <- allocator.allocateKeyValueObject(
        newLeftContent,
        None,
        Some(splitAtKey),
        None,
        Some(Value(KeyValueListPointer(splitAtKey,newRightPtr).toArray)))
    yield
      KeyValueListPointer(Key.AbsoluteMinimum, newLeftPtr)
  
}