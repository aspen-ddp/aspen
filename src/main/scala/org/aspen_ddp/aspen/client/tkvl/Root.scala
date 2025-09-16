package org.aspen_ddp.aspen.client.tkvl

import java.nio.{ByteBuffer, ByteOrder}

import org.aspen_ddp.aspen.client.{AspenClient, Transaction}
import org.aspen_ddp.aspen.common.objects.{AllocationRevisionGuard, Key, KeyOrdering, KeyValueObjectPointer, Value}

import scala.concurrent.{ExecutionContext, Future}

case class Root(tier: Int,
                ordering: KeyOrdering,
                orootObject: Option[KeyValueObjectPointer],
                nodeAllocator: NodeAllocator) {

  def encodedSize: Int = 1 + 1 + nodeAllocator.encodedSize + 1 + orootObject.map(_.encodedSize).getOrElse(0)

  def encode(): Array[Byte] = {
    val arr = new Array[Byte](encodedSize)
    val bb = ByteBuffer.wrap(arr)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.put(tier.toByte)
    bb.put(ordering.code)
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

  def create(client: AspenClient,
             guard: AllocationRevisionGuard,
             ordering: KeyOrdering,
             nodeAllocator: NodeAllocator,
             initialContent: Map[Key, Value] = Map())(using t: Transaction): Future[Root] = {

    given ExecutionContext = client.clientContext

    if (initialContent.isEmpty) {
      Future.successful(new Root(0, ordering, None, nodeAllocator))
    } else {
      for {
        alloc <- nodeAllocator.getAllocatorForTier(0)
        rp <- alloc.allocateKeyValueObject(guard, initialContent)
      } yield {
        new Root(0, ordering, Some(rp), nodeAllocator)
      }
    }
  }

  def apply(client: AspenClient, arr: Array[Byte]): Root = {
    val bb = ByteBuffer.wrap(arr)
    bb.order(ByteOrder.BIG_ENDIAN)
    Root(client, bb)
  }

  def apply(client: AspenClient, bb: ByteBuffer): Root = {
    bb.order(ByteOrder.BIG_ENDIAN)
    val tier = bb.get()
    val ordering = KeyOrdering.fromCode(bb.get())
    val nodeAllocator = NodeAllocator(client, bb)

    val oroot = bb.get() match {
      case 1 => Some(KeyValueObjectPointer(bb))
      case _ => None
    }

    Root(tier, ordering, oroot, nodeAllocator)
  }
}
