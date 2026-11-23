package org.aspen_ddp.aspen.server.host

import java.util.concurrent.ConcurrentHashMap

import scala.jdk.CollectionConverters.*

/** The DeviceIo of every device directory a host has seen, plus `hostRoot` for the host's own directory. Owned by the
 *  control thread (`forPath`, `forget`); `all` and `closeAll` may be read from any thread. A path keeps its DeviceIo
 *  from first sight, before the device is loaded, so a hung device's lanes exist and are isolated from the start,
 *  and a reload reuses them (spec section 7). */
final class DeviceIos(threading: HostThreading, rootDir: os.Path):
  private val ios = new ConcurrentHashMap[os.Path, DeviceIo]()

  /** The host root directory's lane, for host-level files such as the join ceiling. */
  val hostRoot: DeviceIo = threading.newDeviceIo(rootDir)

  /** The device's DeviceIo, created on first sight, else the existing one. */
  def forPath(path: os.Path): DeviceIo =
    ios.computeIfAbsent(path, p => threading.newDeviceIo(p))

  def get(path: os.Path): Option[DeviceIo] = Option(ios.get(path))

  /** Closes and drops the path's DeviceIo, unless its observer shows work in flight, in which case it is kept. */
  def forget(path: os.Path): Unit =
    val io = ios.get(path)
    if io != null && io.observer.inFlight.isEmpty then
      ios.remove(path)
      io.close()

  def all: Map[os.Path, DeviceIo] = ios.asScala.toMap

  /** Host shutdown: closes every DeviceIo, hostRoot included, without waiting for any of them. */
  def closeAll(): Unit =
    ios.values.asScala.foreach(_.close())
    hostRoot.close()
