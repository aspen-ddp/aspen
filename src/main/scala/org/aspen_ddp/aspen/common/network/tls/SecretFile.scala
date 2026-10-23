package org.aspen_ddp.aspen.common.network.tls

import java.nio.file.FileAlreadyExistsException

/** Writes files that contain private keys so they are never readable by anyone but the owner. */
object SecretFile:
  private val ownerOnly = os.PermSet.fromString("rw-------")

  /** Writes bytes to path with mode 0600. The file is created with that mode from the start,
   *  so there is no window in which it is more widely readable. When createNew is true an
   *  existing file is never clobbered (FileAlreadyExistsException); otherwise it is replaced
   *  and its mode is tightened to 0600.
   */
  def write(path: os.Path, bytes: Array[Byte], createNew: Boolean = false): Unit =
    try
      os.write(path, bytes, perms = ownerOnly)
    catch
      case _: FileAlreadyExistsException if !createNew =>
        os.perms.set(path, ownerOnly)
        os.write.over(path, bytes)

  /** Copies source to target as a 0600 file, replacing any existing target. */
  def copy(source: os.Path, target: os.Path): Unit =
    write(target, os.read.bytes(source))
