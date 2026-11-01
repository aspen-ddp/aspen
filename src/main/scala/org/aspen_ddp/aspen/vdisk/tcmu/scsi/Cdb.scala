package org.aspen_ddp.aspen.vdisk.tcmu.scsi

/** SCSI opcodes, named as in tcmu-runner's scsi_defs.h */
object Op:
  val TestUnitReady = 0x00
  val RequestSense = 0x03
  val FormatUnit = 0x04
  val Read6 = 0x08
  val Write6 = 0x0A
  val Inquiry = 0x12
  val ModeSelect6 = 0x15
  val ModeSense6 = 0x1A
  val StartStop = 0x1B
  val ReadCapacity10 = 0x25
  val Read10 = 0x28
  val Write10 = 0x2A
  val WriteVerify10 = 0x2E
  val SyncCache10 = 0x35
  val WriteSame10 = 0x41
  val Unmap = 0x42
  val ModeSelect10 = 0x55
  val ModeSense10 = 0x5A
  val Read16 = 0x88
  val CompareAndWrite = 0x89
  val Write16 = 0x8A
  val WriteVerify16 = 0x8E
  val SyncCache16 = 0x91
  val WriteSame16 = 0x93
  val ServiceActionIn16 = 0x9E
  val MaintenanceIn = 0xA3
  val Read12 = 0xA8
  val Write12 = 0xAA

/** Field decoding for the READ/WRITE style CDB layouts, selected by the CDB's group code */
object Cdb:
  /** CDB length implied by the opcode's group code */
  def length(firstByte: Byte): Option[Int] =
    (firstByte >> 5) & 0x07 match
      case 0 => Some(6)
      case 1 | 2 => Some(10)
      case 4 => Some(16)
      case 5 => Some(12)
      case _ => None

  private def be(cdb: Array[Byte], offset: Int, len: Int): Long =
    var v = 0L
    for i <- 0 until len do
      v = (v << 8) | (cdb(offset + i) & 0xffL)
    v

  def lba(cdb: Array[Byte]): Long =
    length(cdb(0)) match
      case Some(6) => be(cdb, 1, 3) & 0x1FFFFFL
      case Some(10) | Some(12) => be(cdb, 2, 4)
      case Some(16) => be(cdb, 2, 8)
      case _ => throw new IllegalArgumentException(f"unsupported CDB opcode 0x${cdb(0) & 0xff}%02x")

  def transferLength(cdb: Array[Byte]): Long =
    length(cdb(0)) match
      case Some(6) =>
        val n = cdb(4) & 0xff
        if n == 0 then 256L else n.toLong
      case Some(10) => be(cdb, 7, 2)
      case Some(12) => be(cdb, 6, 4)
      case Some(16) => be(cdb, 10, 4)
      case _ => throw new IllegalArgumentException(f"unsupported CDB opcode 0x${cdb(0) & 0xff}%02x")

  /** Force Unit Access; meaningful only for WRITE 10/12/16 and WRITE AND VERIFY */
  def fua(cdb: Array[Byte]): Boolean =
    (cdb(0) & 0xff) match
      case Op.Write10 | Op.Write12 | Op.Write16 | Op.WriteVerify10 | Op.WriteVerify16 => (cdb(1) & 0x08) != 0
      case _ => false
