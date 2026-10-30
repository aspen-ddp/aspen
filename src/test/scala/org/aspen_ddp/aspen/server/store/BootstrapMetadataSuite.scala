package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.PoolObjectAllocatorId
import org.aspen_ddp.aspen.client.tkvl.Root
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.allocation_group.AllocationGroupId
import org.aspen_ddp.aspen.common.ida.{IDA, Replication}
import org.aspen_ddp.aspen.common.metadata.{IDAPolicy, StorageDeviceId, StorageDeviceSetId, StoragePoolState}
import org.aspen_ddp.aspen.common.pool.PoolId

import scala.concurrent.ExecutionContext

class BootstrapMetadataSuite extends IntegrationTestSuite:

  atest("bootstrap registers the initial metadata pool"):
    given ExecutionContext = executionContext
    for
      poolId <- client.getStoragePoolId(PoolId.InitialMetadataPoolName)
      ps <- client.getStoragePoolState(PoolId.InitialMetadataPoolId)
    yield
      poolId should be(PoolId.InitialMetadataPoolId)
      ps.isMetadataPool should be(true)
      ps.ida should be(net.ida)
      ps.stores.length should be(net.ida.width)
      ps.stores.foreach(_.storageDeviceId should be(StorageDeviceId.BootstrapStorageDeviceId))
      ps.allocationGroups should be(List(AllocationGroupId.DefaultMetadataGroupId.uuid))

  atest("bootstrap registers the default metadata group in the Radicle"):
    given ExecutionContext = executionContext
    for
      defaultId <- client.getDefaultMetadataGroupId()
      namedId <- client.getAllocationGroupId(AllocationGroupId.DefaultMetadataGroupName)
      ags <- client.getAllocationGroupState(AllocationGroupId.DefaultMetadataGroupId)
    yield
      defaultId should be(AllocationGroupId.DefaultMetadataGroupId)
      namedId should be(AllocationGroupId.DefaultMetadataGroupId)
      ags.isMetadataGroup should be(true)
      ags.members.map(_.uuid) should be(List(PoolId.InitialMetadataPoolId.uuid))

  atest("the initial metadata pool's trees live in the bootstrap pool"):
    given ExecutionContext = executionContext
    for
      ptr <- client.getStoragePoolPointer(PoolId.InitialMetadataPoolId)
      kvos <- client.read(ptr)
    yield
      for key <- List(StoragePoolState.ErrorTreeKey, StoragePoolState.AllocationTreeKey) do
        val root = Root(client, kvos.contents(key).value.bytes)
        root.rootKeyValuePointer.get.poolId should be(Radicle.poolId)
        root.nodeAllocator.tiers.head._1 should be(PoolObjectAllocatorId(Radicle.poolId))
      succeed

  atest("the bootstrap device set lists the initial metadata pool"):
    given ExecutionContext = executionContext
    for
      sds <- client.getStorageDeviceSetState(StorageDeviceSetId.BootstrapStorageDeviceSetId)
    yield
      sds.assignedPools should contain theSameElementsAs List(PoolId.BootstrapPoolId, PoolId.InitialMetadataPoolId)

class LimitedBootstrapMetadataSuite extends IntegrationTestSuite:

  // Replication's minSliceSize is 1, well below the 4096 limit
  override def testIda: IDA = Replication(3, 2)
  override def testPoolMaxSliceSize: Option[Int] = Some(4096)

  atest("a limited bootstrap makes the default group ExactMatch with the same limit"):
    given ExecutionContext = executionContext
    for
      ags <- client.getAllocationGroupState(AllocationGroupId.DefaultMetadataGroupId)
    yield
      ags.idaPolicy should be(IDAPolicy.ExactMatch)
      ags.ida should be(Some(net.ida))
      ags.maxSliceSize should be(Some(4096))
