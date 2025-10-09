package org.aspen_ddp.aspen.client.internal.pool

import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID
import org.aspen_ddp.aspen.client.internal.allocation.SinglePoolObjectAllocator
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.client.{AspenClient, HostId, KeyValueObjectState, ObjectAllocator, StoragePool}
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.pool.PoolId

object SimpleStoragePool {

  def encode(poolId: PoolId,
             name: String,
             numberOfStores: Int,
             defaultIDA: IDA,
             storeHosts: Array[HostId],
             maxObjectSize: Option[Int]): Array[Byte] = {
    StoragePool.Config(poolId, name, numberOfStores, defaultIDA, maxObjectSize, storeHosts).encode()

  }

  def apply(client: AspenClient, kvos: KeyValueObjectState): SimpleStoragePool = {

    val cfg = StoragePool.Config(kvos)

    val allocTree = new TieredKeyValueList(client,
      new KVObjectRootManager(client, StoragePool.AllocationTreeKey, kvos.pointer))

    val errorTree = new TieredKeyValueList(client,
      new KVObjectRootManager(client, StoragePool.ErrorTreeKey, kvos.pointer))

    new SimpleStoragePool(client, cfg.poolId, cfg.name, cfg.numberOfStores, cfg.defaultIDA, cfg.storeHosts, cfg.maxObjectSize,
      allocTree, errorTree)
  }
}

class SimpleStoragePool(val client: AspenClient,
                        val poolId: PoolId,
                        val name: String,
                        val numberOfStores: Int,
                        val defaultIDA: IDA,
                        val storeHosts: Array[HostId],
                        val maxObjectSize: Option[Int],
                        val allocationTree: TieredKeyValueList,
                        val errorTree: TieredKeyValueList) extends StoragePool {

  override def supportsIDA(ida: IDA): Boolean = numberOfStores >= ida.width

  override def createAllocator(ida: IDA): ObjectAllocator = new SinglePoolObjectAllocator(client,
    this, ida, maxObjectSize)

  override def selectStoresForAllocation(ida: IDA): Array[Int] = {
    val arr = new Array[Int](ida.width)
    for (i <- 0 until ida.width)
      arr(i) = i
    arr
  }
}
