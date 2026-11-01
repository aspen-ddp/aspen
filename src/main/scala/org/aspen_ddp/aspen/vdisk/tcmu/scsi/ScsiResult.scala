package org.aspen_ddp.aspen.vdisk.tcmu.scsi

import org.aspen_ddp.aspen.vdisk.VDiskError

/** Outcome of one SCSI command. `readLen` is the number of bytes returned to the initiator for a read. */
final case class ScsiResult(status: Int, sense: Array[Byte] = Array.empty, readLen: Option[Int] = None)

object ScsiResult:
  val Good: ScsiResult = ScsiResult(ScsiStatus.Good)
  def good(readLen: Int): ScsiResult = ScsiResult(ScsiStatus.Good, readLen = Some(readLen))

object ScsiStatus:
  val Good = 0x00
  val CheckCondition = 0x02
  val Busy = 0x08
  val TaskSetFull = 0x28

/** Sense data builders. Values follow tcmu-runner's tcmu_sts_to_scsi. */
object Sense:
  /** 18-byte fixed-format sense as laid out by tcmu_sense_set_data. `info` sets VALID and bytes 3-6. */
  def fixed(key: Int, ascAscq: Int, info: Option[Long] = None): Array[Byte] =
    val s = new Array[Byte](18)
    s(0) = 0x70.toByte
    s(2) = key.toByte
    s(7) = 10
    s(12) = ((ascAscq >> 8) & 0xff).toByte
    s(13) = (ascAscq & 0xff).toByte
    info.foreach: i =>
      s(0) = (s(0) | 0x80).toByte
      s(3) = ((i >> 24) & 0xff).toByte
      s(4) = ((i >> 16) & 0xff).toByte
      s(5) = ((i >> 8) & 0xff).toByte
      s(6) = (i & 0xff).toByte
    s

  def check(key: Int, ascAscq: Int): ScsiResult =
    ScsiResult(ScsiStatus.CheckCondition, fixed(key, ascAscq))

  val LbaOutOfRange: ScsiResult = check(5, 0x2100)
  val InvalidFieldInCdb: ScsiResult = check(5, 0x2400)
  val InvalidFieldInParamList: ScsiResult = check(5, 0x2600)
  val ParamListLength: ScsiResult = check(5, 0x1A00)
  val InvalidOpcode: ScsiResult = check(5, 0x2000)
  val HardwareError: ScsiResult = check(4, 0x4400)
  val ReadError: ScsiResult = check(3, 0x1100)
  val WriteError: ScsiResult = check(3, 0x0C00)
  val Fenced: ScsiResult = check(2, 0x040B)
  val CapacityChanged: ScsiResult = check(6, 0x2A09)
  val NotSuppSaveParams: ScsiResult = check(5, 0x3900)

  def Miscompare(offset: Long): ScsiResult =
    ScsiResult(ScsiStatus.CheckCondition, fixed(0x0E, 0x1D00, Some(offset)))

object ErrorMap:
  def result(err: Throwable, isWrite: Boolean): ScsiResult =
    err match
      case _: VDiskError.InvalidRequest | _: VDiskError.InvalidGeometry => Sense.InvalidFieldInCdb
      case _: VDiskError.OutOfRange => Sense.LbaOutOfRange
      case VDiskError.Fenced | VDiskError.NotOwner => Sense.Fenced
      case _: VDiskError.DiskFailed => if isWrite then Sense.WriteError else Sense.ReadError
      case _ => Sense.HardwareError
