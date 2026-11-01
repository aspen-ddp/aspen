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
