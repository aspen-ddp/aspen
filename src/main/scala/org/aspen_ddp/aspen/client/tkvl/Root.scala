package org.aspen_ddp.aspen.client.tkvl

import java.nio.{ByteBuffer, ByteOrder}

import org.aspen_ddp.aspen.client.{AspenClient, Transaction}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Key, KeyOrdering, KeyValueObjectPointer, ObjectPointer, Value}

import scala.concurrent.{ExecutionContext, Future}

case class Root(tier: Int,
                ordering: KeyOrdering,
                orootObject: Option[ObjectPointer],
                nodeAllocator: NodeAllocator,
                kind: TreeKind = TreeKind.KeyValueObjects) {

  require(orootObject.forall(Root.pointerMatchesKind(kind, _)),
    s"Root pointer ${orootObject.get} does not match tree kind $kind")

  /** The root node pointer for callers that only deal with key-value object trees */
  def rootKeyValuePointer: Option[KeyValueObjectPointer] = orootObject.map:
    case kvp: KeyValueObjectPointer => kvp
    case other => throw new InvalidRoot(new IllegalStateException(s"Root pointer is not a KeyValueObjectPointer: $other"))

  def encodedSize: Int = 1 + 1 + 1 + nodeAllocator.encodedSize + 1 + orootObject.map(_.encodedSize).getOrElse(0)

  def encode(): Array[Byte] = {
    val arr = new Array[Byte](encodedSize)
    val bb = ByteBuffer.wrap(arr)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.put(tier.toByte)
    bb.put(ordering.code)
    bb.put(kind.code)
    nodeAllocator.encodeInto(bb)
    orootObject match {
      case None => bb.put(0.toByte)
      case Some(rp) =>
        bb.put(1.toByte)
        rp.encodeInto(bb)
    }
    arr
  }

  def encodeInto(bb: ByteBuffer): Unit = {
    bb.put(this.encode())
  }
}

object Root {

  def pointerMatchesKind(kind: TreeKind, ptr: ObjectPointer): Boolean = (kind, ptr) match
    case (TreeKind.KeyValueObjects, _: KeyValueObjectPointer) => true
    case (TreeKind.DataObjects, _: DataObjectPointer) => true
    case _ => false

  def create(client: AspenClient,
             ordering: KeyOrdering,
             nodeAllocator: NodeAllocator,
             initialContent: Map[Key, Value] = Map(),
             kind: TreeKind = TreeKind.KeyValueObjects)(using t: Transaction): Future[Root] = {

    given ExecutionContext = client.clientContext

    // Every tier is checked up front, not only the tier-0 node allocated here
    nodeAllocator.validate().flatMap: _ =>
      if initialContent.isEmpty then
        Future.successful(new Root(0, ordering, None, nodeAllocator, kind))
      else
        for
          alloc <- nodeAllocator.getAllocatorForTier(0)
          rp <- alloc.allocateKeyValueObject(initialContent)
        yield
          new Root(0, ordering, Some(rp), nodeAllocator, kind)
  }

  /** Decodes a Root. Synchronous, so the node allocator is not validated here: every use that writes a node goes
    * through `NodeAllocator.getAllocatorForTier`, which checks the tier against its allocator's current limit.
    */
  def apply(client: AspenClient, arr: Array[Byte]): Root = {
    val bb = ByteBuffer.wrap(arr)
    bb.order(ByteOrder.BIG_ENDIAN)
    Root(client, bb)
  }

  def apply(client: AspenClient, bb: ByteBuffer): Root = {
    bb.order(ByteOrder.BIG_ENDIAN)
    val tier = bb.get()
    val ordering = KeyOrdering.fromCode(bb.get())
    val kind = TreeKind.fromCode(bb.get())
    val nodeAllocator = NodeAllocator(client, bb)

    val oroot = bb.get() match {
      case 1 => Some(ObjectPointer.fromByteBuffer(bb))
      case _ => None
    }

    Root(tier, ordering, oroot, nodeAllocator, kind)
  }
}
