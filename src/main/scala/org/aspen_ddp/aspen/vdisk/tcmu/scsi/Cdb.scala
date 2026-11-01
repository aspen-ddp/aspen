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

  private val dataOps = Set(
    Op.Read6, Op.Read10, Op.Read12, Op.Read16,
    Op.Write6, Op.Write10, Op.Write12, Op.Write16,
    Op.WriteSame10, Op.WriteSame16, Op.WriteVerify10, Op.WriteVerify16,
    Op.SyncCache10, Op.SyncCache16, Op.CompareAndWrite)

  /** Returns the opcode, after checking that the array holds a whole CDB */
  private def checked(cdb: Array[Byte]): Int =
    if cdb.isEmpty then
      throw new IllegalArgumentException("empty CDB")
    val op = cdb(0) & 0xff
    length(cdb(0)).foreach: len =>
      if cdb.length < len then
        throw new IllegalArgumentException(f"CDB for opcode 0x$op%02x is ${cdb.length} bytes, needs $len")
    op

  private def dataOp(cdb: Array[Byte]): Int =
    val op = checked(cdb)
    if !dataOps.contains(op) then
      throw new IllegalArgumentException(f"opcode 0x$op%02x has no LBA/transfer length fields")
    op

  def lba(cdb: Array[Byte]): Long =
    dataOp(cdb) match
      case Op.Read6 | Op.Write6 => be(cdb, 1, 3) & 0x1FFFFFL
      case Op.Read10 | Op.Write10 | Op.WriteSame10 | Op.WriteVerify10 | Op.SyncCache10 |
           Op.Read12 | Op.Write12 => be(cdb, 2, 4)
      case _ => be(cdb, 2, 8)

  /** Number of logical blocks; 0 in a 6-byte CDB means 256 */
  def transferLength(cdb: Array[Byte]): Long =
    dataOp(cdb) match
      case Op.Read6 | Op.Write6 =>
        val n = cdb(4) & 0xff
        if n == 0 then 256L else n.toLong
      case Op.Read10 | Op.Write10 | Op.WriteSame10 | Op.WriteVerify10 | Op.SyncCache10 => be(cdb, 7, 2)
      case Op.Read12 | Op.Write12 => be(cdb, 6, 4)
      case Op.CompareAndWrite => cdb(13) & 0xffL
      case _ => be(cdb, 10, 4)

  /** Force Unit Access; meaningful only for WRITE 10/12/16 and WRITE AND VERIFY */
  def fua(cdb: Array[Byte]): Boolean =
    checked(cdb) match
      case Op.Write10 | Op.Write12 | Op.Write16 | Op.WriteVerify10 | Op.WriteVerify16 => (cdb(1) & 0x08) != 0
      case _ => false
