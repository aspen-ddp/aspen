package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.common.metadata.HostId

import java.util.UUID

/** Small text files kept in a storage device's root directory so that a host can decide
 *  whether to load a device without consulting the network.
 *
 *  - `owning-host`: the HostId that owns the device. Written at creation and rewritten
 *    atomically at adoption.
 *  - `shutdown.state`: present only while the device is shut down. Names the host that shut it down.
 *  - `failed`: written when the storage-devices tree records the device Failed. A device carrying it is never
 *    loaded or claimed again: its stores are rebuilt elsewhere, and a copy that served again would be a second
 *    replica of each of them. Its contents are left for manual recovery.
 */
object DeviceMarkers:

  val OwningHostFile = "owning-host"
  val ShutdownStateFile = "shutdown.state"
  val FailedFile = "failed"

  def isFailed(devicePath: os.Path): Boolean = os.exists(devicePath / FailedFile)

  /** Durable before returning, so a restart never loads the device */
  def writeFailed(devicePath: os.Path): Unit = DurableFiles.write(devicePath / FailedFile, "")

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
