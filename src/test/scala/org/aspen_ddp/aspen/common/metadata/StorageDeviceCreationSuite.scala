package org.aspen_ddp.aspen.common.metadata

import org.aspen_ddp.aspen.IntegrationTestSuite

import java.util.UUID
import scala.concurrent.ExecutionContext

class StorageDeviceCreationSuite extends IntegrationTestSuite:

  private val bootstrapHost = HostId.BootstrapHostId
  private val bootstrapSet  = StorageDeviceSetId.BootstrapStorageDeviceSetId

  atest("createStorageDevice registers a device with empty usage and no stores"):
    given ExecutionContext = executionContext
    for
      deviceId <- client.createStorageDevice(bootstrapHost, bootstrapSet)
      _        <- waitForTransactionsToComplete()
      ds       <- client.getStorageDeviceState(deviceId)
    yield
      ds.storageDeviceId should be(deviceId)
      ds.hostId should be(bootstrapHost)
      ds.storageDeviceSet should be(bootstrapSet)
      ds.stores should be(Map.empty)
      ds.currentUsage should be(0L)
      ds.totalSize should be(0L)

  atest("createStorageDevice adds the device to the host"):
    given ExecutionContext = executionContext
    for
      deviceId <- client.createStorageDevice(bootstrapHost, bootstrapSet)
      _        <- waitForTransactionsToComplete()
      hs       <- client.getHostState(bootstrapHost)
    yield
      hs.storageDevices should contain(deviceId)

  atest("createStorageDevice adds the device to the target set"):
    given ExecutionContext = executionContext
    for
      setId    <- client.createStorageDeviceSet("dev-target-set", level = 0, parent = None)
      _        <- waitForTransactionsToComplete()
      deviceId <- client.createStorageDevice(bootstrapHost, setId)
      _        <- waitForTransactionsToComplete()
      sds      <- client.getStorageDeviceSetState(setId)
    yield
      sds.memberDevices should be(List(deviceId))

  atest("two calls produce distinct devices, both registered on the host and set"):
    given ExecutionContext = executionContext
    for
      first  <- client.createStorageDevice(bootstrapHost, bootstrapSet)
      _      <- waitForTransactionsToComplete()
      second <- client.createStorageDevice(bootstrapHost, bootstrapSet)
      _      <- waitForTransactionsToComplete()
      hs     <- client.getHostState(bootstrapHost)
      sds    <- client.getStorageDeviceSetState(bootstrapSet)
    yield
      first should not be second
      hs.storageDevices should contain(first)
      hs.storageDevices should contain(second)
      sds.memberDevices should contain(first)
      sds.memberDevices should contain(second)

  atest("createStorageDevice fails with NotLevelZero when the set is not level 0"):
    given ExecutionContext = executionContext
    for
      level1Id  <- client.createStorageDeviceSet("higher-set", level = 1, parent = None)
      _         <- waitForTransactionsToComplete()

      hsBefore  <- client.getHostState(bootstrapHost)
      setBefore <- client.getStorageDeviceSetState(bootstrapSet)

      err       <- client.createStorageDevice(bootstrapHost, level1Id).failed
      _         <- waitForTransactionsToComplete()

      hsAfter   <- client.getHostState(bootstrapHost)
      setAfter  <- client.getStorageDeviceSetState(bootstrapSet)
      l1After   <- client.getStorageDeviceSetState(level1Id)
    yield
      err shouldBe a[StorageDeviceSetState.NotLevelZero]
      // The rejected attempt must leave no residue. The allocation and tree insert are
      // already staged when the level check throws, so this holds because transact
      // invalidates the transaction on failure, not because nothing was staged.
      hsAfter.storageDevices should be(hsBefore.storageDevices)
      setAfter.memberDevices should be(setBefore.memberDevices)
      l1After.memberDevices should be(Nil)

  atest("createStorageDevice fails with NoSuchElementException for an unknown host"):
    given ExecutionContext = executionContext
    recoverToSucceededIf[NoSuchElementException](
      client.createStorageDevice(HostId(UUID.randomUUID()), bootstrapSet))
