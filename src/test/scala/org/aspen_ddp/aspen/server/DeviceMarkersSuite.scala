package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.common.metadata.HostId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class DeviceMarkersSuite extends AnyFunSuite with Matchers:

  private val hostA = HostId(UUID.fromString("aaaaaaaa-0000-0000-0000-000000000001"))
  private val hostB = HostId(UUID.fromString("bbbbbbbb-0000-0000-0000-000000000002"))

  private def withDir(f: os.Path => Unit): Unit =
    val dir = os.temp.dir(prefix = "aspen-markers", deleteOnExit = false)
    try f(dir) finally os.remove.all(dir)

  test("owning-host round-trips and is absent by default"):
    withDir: dir =>
      DeviceMarkers.readOwningHost(dir) should be(None)
      DeviceMarkers.writeOwningHost(dir, hostA)
      DeviceMarkers.readOwningHost(dir) should be(Some(hostA))
      os.exists(dir / DeviceMarkers.OwningHostFile) should be(true)

  test("shutdown state round-trips, is absent by default, and can be removed"):
    withDir: dir =>
      DeviceMarkers.readShutdownState(dir) should be(None)
      DeviceMarkers.writeShutdownState(dir, hostA)
      DeviceMarkers.readShutdownState(dir) should be(Some(hostA))
      DeviceMarkers.removeShutdownState(dir)
      DeviceMarkers.readShutdownState(dir) should be(None)
      os.exists(dir / DeviceMarkers.ShutdownStateFile) should be(false)

  test("removing an absent shutdown state is a no-op"):
    withDir: dir =>
      DeviceMarkers.removeShutdownState(dir)

  test("the two files are independent"):
    withDir: dir =>
      DeviceMarkers.writeOwningHost(dir, hostA)
      DeviceMarkers.writeShutdownState(dir, hostB)
      DeviceMarkers.readOwningHost(dir) should be(Some(hostA))
      DeviceMarkers.readShutdownState(dir) should be(Some(hostB))

  test("malformed content throws"):
    withDir: dir =>
      os.write(dir / DeviceMarkers.OwningHostFile, "not-a-uuid")
      an[IllegalArgumentException] should be thrownBy DeviceMarkers.readOwningHost(dir)
      os.write(dir / DeviceMarkers.ShutdownStateFile, "")
      an[IllegalArgumentException] should be thrownBy DeviceMarkers.readShutdownState(dir)

  test("a write replaces the previous value and leaves no temp file behind"):
    withDir: dir =>
      DeviceMarkers.writeOwningHost(dir, hostA)
      DeviceMarkers.writeOwningHost(dir, hostB)
      DeviceMarkers.writeShutdownState(dir, hostA)
      DeviceMarkers.writeShutdownState(dir, hostB)
      DeviceMarkers.readOwningHost(dir) should be(Some(hostB))
      DeviceMarkers.readShutdownState(dir) should be(Some(hostB))
      os.list(dir).map(_.last).toSet should be(
        Set(DeviceMarkers.OwningHostFile, DeviceMarkers.ShutdownStateFile))
