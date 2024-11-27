package org.aspen_ddp.aspen.client.tkvl

import java.nio.ByteBuffer
import java.util.UUID

import org.aspen_ddp.aspen.client.{AspenClient, ObjectAllocator}
import org.aspen_ddp.aspen.client.internal.allocation.SinglePoolObjectAllocator
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.pool.PoolId

import scala.concurrent.{ExecutionContext, Future}

sealed abstract class NodeAllocator {
  def getAllocatorForTier(tier: Int): Future[ObjectAllocator]

  def getMaxNodeSize(tier: Int): Int

  val code: Int
  def encodedSize: Int
  def encodeInto(bb: ByteBuffer): Unit
}

object NodeAllocator {
  def apply(client: AspenClient, bb: ByteBuffer): NodeAllocator = {
    bb.get() match {
      case 0 =>
        val msb = bb.getLong()
        val lsb = bb.getLong()
        val poolId = PoolId(new UUID(msb, lsb))
        new SinglePoolNodeAllocator(client, poolId)
      case _ =>
        println("HERE!!!")
        throw new Exception("Invalid Node Allocator")
    }
  }
}

object BootstrapPoolNodeAllocator extends NodeAllocator {

  val poolId: PoolId = Radicle.poolId

  override val code: Int = 0
  override val encodedSize = 17

  override def getAllocatorForTier(tier: Int): Future[ObjectAllocator] = null

  override def getMaxNodeSize(tier: Int): Int = 1 * 1024 * 1024

  override def encodeInto(bb:ByteBuffer): Unit = {
    bb.put(code.asInstanceOf[Byte])
    bb.putLong(poolId.uuid.getMostSignificantBits)
    bb.putLong(poolId.uuid.getLeastSignificantBits)
  }
}


class SinglePoolNodeAllocator(val client:AspenClient, val poolId: PoolId) extends NodeAllocator {

  implicit val ec: ExecutionContext = client.clientContext

  private var allocator: Option[SinglePoolObjectAllocator] = None

  override val code: Int = 0
  override val encodedSize = 17

  override def getAllocatorForTier(tier: Int): Future[ObjectAllocator] = allocator match {
    case Some(alloc) => Future.successful(alloc)
    case None => client.getStoragePool(poolId).map { opool =>
      val alloc = new SinglePoolObjectAllocator(client, opool.get, opool.get.defaultIDA, None)
      // Note, there's a race condition here if multiple getAllocatorForTier calls are made
      // simultaneously. It's harmless though so we'll ignore it and just keep the last one.
      allocator = Some(alloc)
      alloc
    }
  }

  override def getMaxNodeSize(tier: Int): Int = 1 * 1024 * 1024

  override def encodeInto(bb:ByteBuffer): Unit = {
    bb.put(code.asInstanceOf[Byte])
    bb.putLong(poolId.uuid.getMostSignificantBits)
    bb.putLong(poolId.uuid.getLeastSignificantBits)
  }
}