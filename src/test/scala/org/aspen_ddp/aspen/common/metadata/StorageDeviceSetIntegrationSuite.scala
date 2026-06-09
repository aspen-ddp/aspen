package org.aspen_ddp.aspen.common.metadata

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.internal.MetadataTree
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.objects.DataObjectPointer
import org.aspen_ddp.aspen.common.pool.PoolId

import scala.concurrent.ExecutionContext

class StorageDeviceSetIntegrationSuite extends IntegrationTestSuite:

  atest("bootstrap creates the 'bootstrap' storage device set"):
    given ExecutionContext = executionContext
    val tree = new MetadataTree(client, radicle, Radicle.StorageDeviceSetsTreeKey)
    for
      ptr <- tree.get(StorageDeviceSetId.BootstrapStorageDeviceSetId.uuid)
      dos <- client.read(ptr.asInstanceOf[DataObjectPointer])
    yield
      val sds = StorageDeviceSetState(dos)
      sds.setId should be(StorageDeviceSetId.BootstrapStorageDeviceSetId)
      sds.name should be("bootstrap")
      sds.level should be(0)
      sds.parent should be(None)
      sds.memberDevices should contain(StorageDeviceId.BootstrapStorageDeviceId)
      sds.memberSets should be(Nil)
      sds.assignedPools should contain(PoolId.BootstrapPoolId)

  atest("bootstrap pool and device reference the bootstrap set"):
    given ExecutionContext = executionContext
    for
      ps <- client.getStoragePoolState(PoolId.BootstrapPoolId)
      ds <- client.getStorageDeviceState(StorageDeviceId.BootstrapStorageDeviceId)
    yield
      ps.storageDeviceSet should be(StorageDeviceSetId.BootstrapStorageDeviceSetId)
      ds.storageDeviceSet should be(StorageDeviceSetId.BootstrapStorageDeviceSetId)
