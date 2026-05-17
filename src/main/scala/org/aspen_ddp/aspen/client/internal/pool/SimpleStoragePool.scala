package org.aspen_ddp.aspen.client.internal.pool

import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID
import org.aspen_ddp.aspen.client.internal.allocation.SinglePoolObjectAllocator
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState, ObjectAllocator, StoragePool}
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.metadata.{HostId, StoragePoolState}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.server.store.backend.BackendConfig

object SimpleStoragePool:
  
  def apply(client: AspenClient, kvos: KeyValueObjectState): SimpleStoragePool =

    val cfg = StoragePoolState(kvos)

    val allocTree = new TieredKeyValueList(client,
      new KVObjectRootManager(client, StoragePoolState.AllocationTreeKey, kvos.pointer))

    val errorTree = new TieredKeyValueList(client,
      new KVObjectRootManager(client, StoragePoolState.ErrorTreeKey, kvos.pointer))

    new SimpleStoragePool(client, cfg.poolId, cfg.name, cfg.numberOfStores, cfg.defaultIDA, cfg.stores,
      cfg.backendConfig, cfg.maxObjectSize,
      allocTree, errorTree)


class SimpleStoragePool(val client: AspenClient,
                        val poolId: PoolId,
                        val name: String,
                        val numberOfStores: Int,
                        val defaultIDA: IDA,
                        val stores: Array[StoragePoolState.StoreEntry],
                        val backendConfig: BackendConfig,
                        val maxObjectSize: Option[Int],
                        val allocationTree: TieredKeyValueList,
                        val errorTree: TieredKeyValueList) extends StoragePool:

  override def supportsIDA(ida: IDA): Boolean = numberOfStores >= ida.width

  override def createAllocator(ida: IDA): ObjectAllocator = new SinglePoolObjectAllocator(client,
    this, ida, maxObjectSize)

  override def selectStoresForAllocation(ida: IDA): Array[Int] = 
    val arr = new Array[Int](ida.width)
    for (i <- 0 until ida.width)
      arr(i) = i
    arr

