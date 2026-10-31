package org.aspen_ddp.aspen.vdisk

/** Errors reported by the virtual disk engine */
sealed abstract class VDiskError(msg: String) extends Exception(msg)

object VDiskError:
  /** Malformed request, e.g. unaligned offset or length */
  final case class InvalidRequest(msg: String) extends VDiskError(msg)
  /** Request extends past the end of the disk */
  final case class OutOfRange(msg: String) extends VDiskError(msg)
  /** The disk is now owned by a newer writer */
  case object Fenced extends VDiskError("disk is fenced by a newer writer")
  final case class DiskFailed(reason: String) extends VDiskError(s"disk failed: $reason")
  final case class DiskNotFound(name: String) extends VDiskError(s"disk not found: $name")
  final case class DiskAlreadyExists(name: String) extends VDiskError(s"disk already exists: $name")
  final case class InvalidGeometry(reason: String) extends VDiskError(s"invalid geometry: $reason")
  case object NotOwner extends VDiskError("this handle does not own the disk")
