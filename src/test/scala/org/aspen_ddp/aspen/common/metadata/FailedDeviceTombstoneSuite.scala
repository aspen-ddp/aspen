package org.aspen_ddp.aspen.common.metadata

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class FailedDeviceTombstoneSuite extends AnyFunSuite with Matchers:

  private val liveHost = HostId(UUID.randomUUID())
  private val liveDevice = StorageDeviceId(UUID.randomUUID())
  private val setId = StorageDeviceSetId(UUID.randomUUID())

  private def state(hostId: HostId,
                    deviceId: StorageDeviceId,
                    status: StorageDeviceState.DeviceStatus = StorageDeviceState.DeviceStatus.Operational)
      : StorageDeviceState =
    StorageDeviceState(deviceId, hostId, 0L, 1024L, Map.empty, setId, status)

  test("the tombstone constants are the zero UUID"):
    fixed_ids.FailedHostId should be(HostId(new UUID(0, 0)))
    fixed_ids.FailedStorageDeviceId should be(StorageDeviceId(new UUID(0, 0)))

  test("a healthy device does not read as failed"):
    state(liveHost, liveDevice).isFailed should be(false)

  test("a fully tombstoned device reads as failed"):
    state(fixed_ids.FailedHostId, fixed_ids.FailedStorageDeviceId,
          StorageDeviceState.DeviceStatus.Failed).isFailed should be(true)

  // The status is authoritative: zeroed ids alone do not make a device failed.
  test("zeroed ids on an Operational device do not read as failed"):
    state(fixed_ids.FailedHostId, liveDevice).isFailed should be(false)
    state(liveHost, fixed_ids.FailedStorageDeviceId).isFailed should be(false)
