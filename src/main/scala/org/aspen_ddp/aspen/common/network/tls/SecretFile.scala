package org.aspen_ddp.aspen.common.network.tls

import java.nio.file.attribute.PosixFilePermissions
import java.nio.file.{FileAlreadyExistsException, Files, Path, StandardOpenOption}

/** Writes files that contain private keys so they are never readable by anyone but the owner. */
object SecretFile:
  private val ownerOnly = PosixFilePermissions.fromString("rw-------")

  /** Writes bytes to path with mode 0600. The file is created with that mode from the start,
   *  so there is no window in which it is more widely readable. When createNew is true an
   *  existing file is never clobbered (FileAlreadyExistsException); otherwise it is replaced
   *  and its mode is tightened to 0600.
   */
  def write(path: Path, bytes: Array[Byte], createNew: Boolean = false): Unit =
    try
      Files.createFile(path, PosixFilePermissions.asFileAttribute(ownerOnly))
    catch
      case _: FileAlreadyExistsException if !createNew =>
        Files.setPosixFilePermissions(path, ownerOnly)
    Files.write(path, bytes, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)

  /** Copies source to target as a 0600 file, replacing any existing target. */
  def copy(source: Path, target: Path): Unit =
    write(target, Files.readAllBytes(source))
