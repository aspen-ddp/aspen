package org.aspen_ddp.aspen.common.metadata

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.internal.MetadataTree
import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.objects.DataObjectPointer
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.server.store.backend.RocksDBConfig
import org.aspen_ddp.aspen.common.DataBuffer

import scala.concurrent.{ExecutionContext, Future}

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

  atest("createStorageDeviceSet round-trips via the client"):
    given ExecutionContext = executionContext
    for
      setId <- client.createStorageDeviceSet("test-set", level = 0, parent = None)
      _ <- waitForTransactionsToComplete()
      sds <- client.getStorageDeviceSetState(setId)
    yield
      sds.setId should be(setId)
      sds.name should be("test-set")
      sds.level should be(0)
      sds.parent should be(None)
      sds.memberDevices should be(Nil)
      sds.memberSets should be(Nil)
      sds.assignedPools should be(Nil)

  atest("getStorageDeviceSetState reads the bootstrap set"):
    given ExecutionContext = executionContext
    client.getStorageDeviceSetState(StorageDeviceSetId.BootstrapStorageDeviceSetId).map: sds =>
      sds.name should be("bootstrap")

  atest("getStorageDeviceSetId resolves a created set by name"):
    given ExecutionContext = executionContext
    for
      setId <- client.createStorageDeviceSet("named-set", level = 0, parent = None)
      _ <- waitForTransactionsToComplete()
      resolved <- client.getStorageDeviceSetId("named-set")
    yield
      resolved should be(setId)

  atest("createStorageDeviceSet links the new set into its parent"):
    given ExecutionContext = executionContext
    for
      parentId <- client.createStorageDeviceSet("parent-set", level = 1, parent = None)
      _ <- waitForTransactionsToComplete()
      childId <- client.createStorageDeviceSet("child-set", level = 0, parent = Some(parentId))
      _ <- waitForTransactionsToComplete()
      parent <- client.getStorageDeviceSetState(parentId)
      child <- client.getStorageDeviceSetState(childId)
    yield
      child.parent should be(Some(parentId))
      parent.memberSets should contain(childId)
      parent.memberSets.count(_ == childId) should be(1)

  atest("createNewStoragePool selects devices from the set and records the pool in assignedPools"):
    given ExecutionContext = executionContext
    val setId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    for
      poolId <- client.createNewStoragePool(
                  "rework-pool",
                  Replication(1, 1),
                  None,
                  RocksDBConfig(),
                  setId,
                  0L)
      _ <- waitForTransactionsToComplete()
      poolState <- client.getStoragePoolState(poolId)
      setState <- client.getStorageDeviceSetState(setId)
    yield
      poolState.storageDeviceSet should be(setId)
      poolState.stores.length should be(1)
      poolState.stores(0).storageDeviceId should be(StorageDeviceId.BootstrapStorageDeviceId)
      setState.assignedPools should contain(poolId)
      setState.assignedPools should contain(PoolId.BootstrapPoolId)

  atest("createStorageDeviceSet rejects a parent at an equal or lower level"):
    given ExecutionContext = executionContext
    for
      parentId <- client.createStorageDeviceSet("bad-parent", level = 0, parent = None)
      _ <- waitForTransactionsToComplete()
      _ <- recoverToSucceededIf[AspenClient.InvalidDeviceSetLevel](
             client.createStorageDeviceSet("bad-child-equal", level = 0, parent = Some(parentId))
           )
      _ <- recoverToSucceededIf[AspenClient.InvalidDeviceSetLevel](
             client.createStorageDeviceSet("bad-child-higher", level = 1, parent = Some(parentId))
           )
    yield succeed

  atest("moveDeviceToSet moves the device and updates both sets"):
    given ExecutionContext = executionContext
    val deviceId = StorageDeviceId.BootstrapStorageDeviceId
    val oldSetId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    for
      destId <- client.createStorageDeviceSet("dest-set", level = 0, parent = None)
      _ <- waitForTransactionsToComplete()

      _ <- client.moveDeviceToSet(deviceId, destId)
      _ <- waitForTransactionsToComplete()

      device <- client.getStorageDeviceState(deviceId)
      dest <- client.getStorageDeviceSetState(destId)
      old <- client.getStorageDeviceSetState(oldSetId)
    yield
      device.storageDeviceSet should be(destId)
      dest.memberDevices should contain(deviceId)
      old.memberDevices should not contain deviceId

  atest("moveDeviceToSet is a no-op when the device is already in the target set"):
    given ExecutionContext = executionContext
    val deviceId = StorageDeviceId.BootstrapStorageDeviceId
    val setId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    for
      _ <- client.moveDeviceToSet(deviceId, setId)
      _ <- waitForTransactionsToComplete()

      device <- client.getStorageDeviceState(deviceId)
      set <- client.getStorageDeviceSetState(setId)
    yield
      device.storageDeviceSet should be(setId)
      set.memberDevices should contain(deviceId)
      set.memberDevices.count(_ == deviceId) should be(1)

  atest("moveDeviceToSet fails with NotLevelZero when the target set is not level 0"):
    given ExecutionContext = executionContext
    val deviceId = StorageDeviceId.BootstrapStorageDeviceId
    for
      level1Id <- client.createStorageDeviceSet("higher-set", level = 1, parent = None)
      _ <- waitForTransactionsToComplete()

      err <- client.moveDeviceToSet(deviceId, level1Id).failed
    yield
      err shouldBe a[StorageDeviceSetState.NotLevelZero]

  atest("moveDeviceToSet fails with NoSuchElementException for an unknown device"):
    given ExecutionContext = executionContext
    val unknownDevice = StorageDeviceId(java.util.UUID.randomUUID())
    val setId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    recoverToSucceededIf[NoSuchElementException](
      client.moveDeviceToSet(unknownDevice, setId)
    )

  atest("moveDeviceToSet self-heals when the old set does not list the device"):
    given ExecutionContext = executionContext
    val deviceId = StorageDeviceId.BootstrapStorageDeviceId
    val oldSetId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    for
      destId <- client.createStorageDeviceSet("selfheal-dest", level = 0, parent = None)
      _ <- waitForTransactionsToComplete()

      // Corrupt state: remove the device from the old set's memberDevices while the
      // device's storageDeviceSet still points at the old set.
      oldPtr <- client.getStorageDeviceSetPointer(oldSetId)
      oldDos <- client.read(oldPtr)
      oldSet = StorageDeviceSetState(oldDos)
      _ <- client.transactUntilSuccessful: tx =>
             tx.overwrite(oldPtr, oldDos.revision,
               DataBuffer(oldSet.copy(memberDevices = oldSet.memberDevices.filter(_ != deviceId)).toBytes))
             Future.unit
      _ <- waitForTransactionsToComplete()

      _ <- client.moveDeviceToSet(deviceId, destId)
      _ <- waitForTransactionsToComplete()

      device <- client.getStorageDeviceState(deviceId)
      dest <- client.getStorageDeviceSetState(destId)
      old <- client.getStorageDeviceSetState(oldSetId)
    yield
      device.storageDeviceSet should be(destId)
      dest.memberDevices should contain(deviceId)
      old.memberDevices should not contain deviceId
