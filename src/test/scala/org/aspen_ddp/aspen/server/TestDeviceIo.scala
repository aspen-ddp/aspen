package org.aspen_ddp.aspen.server

import java.util.concurrent.ConcurrentHashMap

import org.aspen_ddp.aspen.server.host.{DeviceIo, ThreadedDeviceIo}

/** A real-threaded DeviceIo per path for tests that drive a BackendOpener or a RebuildingStore without a host
 *  (a host owns its own, `DeviceControl.deviceIos`). One per path for the JVM, its daemon threads started lazily. */
object TestDeviceIo:
  private val ios = new ConcurrentHashMap[os.Path, DeviceIo]()

  def of(path: os.Path): DeviceIo =
    ios.computeIfAbsent(path, p => new ThreadedDeviceIo(p, DeviceIo.Config()))
