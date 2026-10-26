package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.client.{AspenClient, ObjectAllocator, ObjectReader, Transaction}
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.objects.{Key, KeyOrdering, KeyValueObjectPointer, ObjectPointer, ObjectRevision, Value}

import scala.concurrent.{ExecutionContext, Future}

/** Nodes stored as KeyValue objects. Every method delegates to the `KeyValueListNode` statics. */
object KeyValueNodeEncoding extends NodeEncoding:
  def kind: TreeKind = TreeKind.KeyValueObjects

  private def kvNode(node: ListNode): KeyValueListNode = node match
    case n: KeyValueListNode => n
    case n => throw new IllegalArgumentException(
      s"KeyValueNodeEncoding requires a KeyValueListNode, got ${n.getClass.getName}")

  private def kvPointer(pointer: ObjectPointer): KeyValueObjectPointer = pointer match
    case p: KeyValueObjectPointer => p
    case p => throw new IllegalArgumentException(
      s"KeyValueNodeEncoding requires a KeyValueObjectPointer, got ${p.getClass.getName}")

  def read(reader: ObjectReader, pointer: ObjectPointer, minimum: Key, ordering: KeyOrdering,
           comment: String): Future[ListNode] =
    given ExecutionContext = reader.client.clientContext
    val ptr = kvPointer(pointer)
    reader.read(ptr, comment).map: kvos =>
      KeyValueListNode(reader, KeyValueListPointer(minimum, ptr), ordering, kvos)

  def insert(node: ListNode,
             key: Key,
             value: Value,
             maxNodeSize: Int,
             allocator: ObjectAllocator,
             prepareForSplit: (Key, ObjectPointer) => Future[Unit],
             requirement: Option[Either[Boolean, ObjectRevision]])(using Transaction): Future[Unit] =
    val n = kvNode(node)
    given ExecutionContext = n.reader.client.clientContext
    KeyValueListNode.insert(n, n.ordering, key, value, maxNodeSize, allocator, prepareForSplit, requirement)

  /** `maxNodeSize` is not needed: `KeyValueListNode.planJoin` only joins when the result is no larger than the right node */
  def delete(node: ListNode, key: Key, maxNodeSize: Int, prepareForJoin: (Key, ObjectPointer) => Future[Unit])
            (using Transaction): Future[Boolean] =
    val n = kvNode(node)
    given ExecutionContext = n.reader.client.clientContext
    KeyValueListNode.delete(n, key, n.reader, prepareForJoin)

  def applyBatch(node: ListNode,
                 ops: List[BatchOp],
                 maxNodeSize: Int,
                 allocator: ObjectAllocator,
                 joinSource: Option[ListNode],
                 prepareForSplit: (Key, ObjectPointer) => Future[Unit],
                 prepareForJoin: (Key, ObjectPointer) => Future[Unit])(using Transaction): Future[Boolean] =
    val n = kvNode(node)
    given ExecutionContext = n.reader.client.clientContext
    KeyValueListNode.applyBatch(n, ops, maxNodeSize, allocator, joinSource.map(kvNode), prepareForSplit, prepareForJoin)

  def relink(node: ListNode,
             newLink: Option[NodeLink],
             maxNodeSize: Int,
             allocator: ObjectAllocator,
             prepareForSplit: (Key, ObjectPointer) => Future[Unit])(using Transaction): Future[Unit] =
    val n = kvNode(node)
    given ExecutionContext = n.reader.client.clientContext
    KeyValueListNode.relink(n, newLink.map(l => KeyValueListPointer(l.minimum, kvPointer(l.pointer))), maxNodeSize,
      allocator, prepareForSplit)

  def splitAt(node: ListNode,
              splitAtKey: Key,
              inclusive: Boolean,
              maxNodeSize: Int,
              optionalDownPointer: Option[ObjectPointer],
              allocator: ObjectAllocator)(using Transaction): Future[NodeLink] =
    val n = kvNode(node)
    given ExecutionContext = n.reader.client.clientContext
    KeyValueListNode.splitAt(n, n.ordering, splitAtKey, inclusive, optionalDownPointer.map(kvPointer), allocator)
      .map(p => NodeLink(p.minimum, p.pointer))

  def allocate(allocator: ObjectAllocator, ordering: KeyOrdering, entries: Map[Key, Value])
              (using Transaction): Future[ObjectPointer] =
    allocator.allocateKeyValueObject(entries)

  def allocateChain(allocator: ObjectAllocator,
                    ordering: KeyOrdering,
                    entries: Map[Key, Value],
                    maxNodeSize: Int,
                    prepareForSplit: (Key, ObjectPointer) => Future[Unit])(using Transaction): Future[ObjectPointer] =
    given ExecutionContext = allocator.client.clientContext
    KeyValueListNode.allocateChain(allocator, ordering, entries, maxNodeSize, prepareForSplit)

  def deleteList(client: AspenClient,
                 head: NodeLink,
                 ordering: KeyOrdering,
                 odeleteEntry: Option[(Key, Value) => Future[Unit]]): Future[Unit] =
    given ExecutionContext = client.clientContext
    KeyValueListNode.deleteList(client, KeyValueListPointer(head.minimum, kvPointer(head.pointer)), ordering,
      odeleteEntry.map(fn => (k, vs) => fn(k, vs.value)))

  def nodeSizeLimit(ida: IDA, maxSliceSize: Option[Int]): Option[Int] =
    NodeAllocator.nodeSizeLimit(ida, maxSliceSize, TreeKind.KeyValueObjects)
