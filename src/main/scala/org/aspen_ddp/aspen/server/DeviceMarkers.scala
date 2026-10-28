package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.common.metadata.HostId

import java.util.UUID

/** Small text files kept in a storage device's root directory so that a host can decide
 *  whether to load a device without consulting the network.
 *
 *  - `owning-host`: the HostId that owns the device. Written at creation and rewritten
 *    atomically at adoption.
 *  - `shutdown.state`: present only while the device is shut down. Names the host that shut it down.
 */
object DeviceMarkers:

  val OwningHostFile = "owning-host"
  val ShutdownStateFile = "shutdown.state"

  /** None if the file is absent. Throws IllegalArgumentException on malformed content. */
  def readOwningHost(devicePath: os.Path): Option[HostId] = readHostId(devicePath / OwningHostFile)

  def writeOwningHost(devicePath: os.Path, hostId: HostId): Unit =
    writeHostId(devicePath, OwningHostFile, hostId)

  /** None if the file is absent. Throws IllegalArgumentException on malformed content. */
  def readShutdownState(devicePath: os.Path): Option[HostId] = readHostId(devicePath / ShutdownStateFile)

  def writeShutdownState(devicePath: os.Path, hostId: HostId): Unit =
    writeHostId(devicePath, ShutdownStateFile, hostId)

  def removeShutdownState(devicePath: os.Path): Unit =
    DurableFiles.remove(devicePath / ShutdownStateFile)

  private def readHostId(file: os.Path): Option[HostId] =
    if os.exists(file) then
      Some(HostId(UUID.fromString(os.read(file).trim)))
    else
      None

  // Atomic and forced to disk before returning (DurableFiles.write): the shutdown reports a
  // device safe to remove only after shutdown.state is written, and a lost owning-host would
  // leave the device refusing to load on its own host.
  private def writeHostId(devicePath: os.Path, name: String, hostId: HostId): Unit =
    DurableFiles.write(devicePath / name, hostId.uuid.toString)
