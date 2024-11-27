package org.aspen_ddp.aspen.common.store

import org.aspen_ddp.aspen.common.pool.PoolId

import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID

object StoreId:
  def apply(arr: Array[Byte]): StoreId =
    assert(arr.length == 17)
    val bb = ByteBuffer.wrap(arr)
    bb.order(ByteOrder.BIG_ENDIAN)
    val msb = bb.getLong
    val lsb = bb.getLong
    val idx = bb.get
    StoreId(PoolId(new UUID(msb, lsb)), idx)

  def apply(name: String): StoreId =
    val parts = name.split(':')
    val poolId = PoolId(UUID.fromString(parts(0)))
    val idx = Integer.valueOf(parts(1)).toByte
    StoreId(poolId, idx)
    
    
case class StoreId(poolId: PoolId, poolIndex: Byte):
  
  def directoryName: String = s"${poolId.uuid}:$poolIndex"
  
  def toBytes: Array[Byte] =
    val arr = new Array[Byte](17)
    val bb = ByteBuffer.wrap(arr)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.putLong(poolId.uuid.getMostSignificantBits)
    bb.putLong(poolId.uuid.getLeastSignificantBits)
    bb.put(poolIndex)
    arr
    
