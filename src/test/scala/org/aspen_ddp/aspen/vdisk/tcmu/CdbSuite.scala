package org.aspen_ddp.aspen.vdisk.tcmu

import org.aspen_ddp.aspen.vdisk.tcmu.scsi.{Cdb, Op}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CdbSuite extends AnyFunSuite with Matchers:

  test("group codes give CDB lengths"):
    Cdb.length(0x28.toByte) should be (Some(10))
    Cdb.length(0x88.toByte) should be (Some(16))
    Cdb.length(0xA8.toByte) should be (Some(12))
    Cdb.length(0x08.toByte) should be (Some(6))
    Cdb.length(0xC0.toByte) should be (None)

  test("6-byte transfer length 0 is 256 blocks"):
    val cdb = new Array[Byte](6)
    cdb(0) = Op.Read6.toByte
    Cdb.transferLength(cdb) should be (256L)
    cdb(4) = 7
    Cdb.transferLength(cdb) should be (7L)

  test("READ 16 LBA and length decode big-endian"):
    val cdb = new Array[Byte](16)
    cdb(0) = Op.Read16.toByte
    for i <- 0 until 8 do cdb(2 + i) = (i + 1).toByte
    cdb(10) = 0x0A; cdb(11) = 0x0B; cdb(12) = 0x0C; cdb(13) = 0x0D
    Cdb.lba(cdb) should be (0x0102030405060708L)
    Cdb.transferLength(cdb) should be (0x0A0B0C0DL)

  test("READ 10 and 12 decode LBA and length"):
    val c10 = new Array[Byte](10)
    c10(0) = Op.Read10.toByte
    c10(2) = 0xFF.toByte; c10(3) = 2; c10(4) = 3; c10(5) = 4; c10(7) = 5; c10(8) = 6
    Cdb.lba(c10) should be (0xFF020304L)
    Cdb.transferLength(c10) should be (0x0506L)
    val c12 = new Array[Byte](12)
    c12(0) = Op.Read12.toByte
    c12(2) = 1; c12(3) = 2; c12(4) = 3; c12(5) = 4
    c12(6) = 0x80.toByte; c12(7) = 0; c12(8) = 0; c12(9) = 1
    Cdb.lba(c12) should be (0x01020304L)
    Cdb.transferLength(c12) should be (0x80000001L)

  test("FUA bit only on writes"):
    val w = new Array[Byte](10)
    w(0) = Op.Write10.toByte
    w(1) = 0x08
    Cdb.fua(w) should be (true)
    w(1) = 0
    Cdb.fua(w) should be (false)
    val r = new Array[Byte](10)
    r(0) = Op.Read10.toByte
    r(1) = 0x08
    Cdb.fua(r) should be (false)
    val wv = new Array[Byte](16)
    wv(0) = Op.WriteVerify16.toByte
    wv(1) = 0x08
    Cdb.fua(wv) should be (true)

  test("READ 6 uses the 21-bit LBA"):
    val cdb = new Array[Byte](6)
    cdb(0) = Op.Read6.toByte
    cdb(1) = 0xFF.toByte
    cdb(2) = 0x12; cdb(3) = 0x34
    Cdb.lba(cdb) should be (0x1F1234L)

  private def cdbOf(op: Int, n: Int): Array[Byte] =
    val c = new Array[Byte](n)
    c(0) = op.toByte
    c

  test("COMPARE AND WRITE count is byte 13 only"):
    val c = cdbOf(Op.CompareAndWrite, 16)
    for i <- 2 until 10 do c(i) = (i - 1).toByte
    c(10) = 0x7F; c(11) = 0x7F; c(12) = 0x7F; c(13) = 3
    Cdb.lba(c) should be (0x0102030405060708L)
    Cdb.transferLength(c) should be (3L)

  test("SYNCHRONIZE CACHE 16 and WRITE SAME decode"):
    val c = cdbOf(Op.SyncCache16, 16)
    c(9) = 9; c(13) = 4
    Cdb.lba(c) should be (9L)
    Cdb.transferLength(c) should be (4L)
    val ws = cdbOf(Op.WriteSame10, 10)
    ws(5) = 6; ws(8) = 2
    Cdb.lba(ws) should be (6L)
    Cdb.transferLength(ws) should be (2L)

  test("opcodes without LBA or length fields are rejected"):
    for op <- Seq(Op.Unmap, Op.ReadCapacity10, Op.ModeSense10, Op.MaintenanceIn, Op.ServiceActionIn16) do
      val c = cdbOf(op, 16)
      an [IllegalArgumentException] should be thrownBy Cdb.lba(c)
      an [IllegalArgumentException] should be thrownBy Cdb.transferLength(c)

  test("empty and truncated CDBs are rejected"):
    val empty = Array.empty[Byte]
    an [IllegalArgumentException] should be thrownBy Cdb.lba(empty)
    an [IllegalArgumentException] should be thrownBy Cdb.transferLength(empty)
    an [IllegalArgumentException] should be thrownBy Cdb.fua(empty)
    val short = cdbOf(Op.Write16, 10)
    an [IllegalArgumentException] should be thrownBy Cdb.lba(short)
    an [IllegalArgumentException] should be thrownBy Cdb.transferLength(short)
    an [IllegalArgumentException] should be thrownBy Cdb.fua(short)

  test("FUA across write opcodes"):
    for (op, n) <- Seq((Op.Write12, 12), (Op.Write16, 16), (Op.WriteVerify10, 10)) do
      val c = cdbOf(op, n)
      c(1) = 0x08
      Cdb.fua(c) should be (true)
      c(1) = 0x00
      Cdb.fua(c) should be (false)
    val w6 = cdbOf(Op.Write6, 6)
    w6(1) = 0x08
    Cdb.fua(w6) should be (false)
    val w16 = cdbOf(Op.Write16, 16)
    w16(1) = 0xF7.toByte
    Cdb.fua(w16) should be (false)

