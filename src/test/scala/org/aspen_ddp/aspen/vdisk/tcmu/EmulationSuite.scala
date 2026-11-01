package org.aspen_ddp.aspen.vdisk.tcmu

import java.nio.charset.StandardCharsets.US_ASCII

import org.aspen_ddp.aspen.vdisk.tcmu.scsi.{Emulation, Op, ScsiResult, ScsiStatus, Sense}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class EmulationSuite extends AnyFunSuite with Matchers:

  private val Wwn = "a5d1e2f3-4a5b-46c7-8d9e-0f1a2b3c4d5e"
  private val settings = DeviceSettings(8L * 1024 * 1024, 4096, 1024, 16384, "vd1", Wwn)

  private def hex(s: String): Array[Byte] =
    s.split("\\s+").filter(_.nonEmpty).map(b => Integer.parseInt(b, 16).toByte)

  private def ascii(s: String): Array[Byte] = s.getBytes(US_ASCII)

  private def zeros(n: Int): Array[Byte] = new Array[Byte](n)

  private def cdb(bytes: Int*): Array[Byte] = bytes.map(_.toByte).toArray

  private def run(c: Array[Byte], dataOut: Array[Byte] = Array.empty): (ScsiResult, Array[Byte]) =
    Emulation.handle(c, settings, () => dataOut).getOrElse(fail(f"opcode 0x${c(0)}%02x not handled"))

  private def ok(c: Array[Byte]): Array[Byte] =
    val (r, data) = run(c)
    r.status should be (ScsiStatus.Good)
    r.readLen should be (Some(data.length))
    data

  private def inquiry(evpd: Boolean, page: Int, alloc: Int = 0xffff): Array[Byte] =
    cdb(Op.Inquiry, if evpd then 1 else 0, page, alloc >> 8, alloc & 0xff, 0)

  private def modeSense6(page: Int, alloc: Int = 255, subpage: Int = 0): Array[Byte] =
    cdb(Op.ModeSense6, 0, page, subpage, alloc, 0)

  private def modeSense10(page: Int, alloc: Int = 0xffff): Array[Byte] =
    cdb(Op.ModeSense10, 0, page, 0, 0, 0, 0, alloc >> 8, alloc & 0xff, 0)

  private def modeSelect6(byte1: Int, len: Int): Array[Byte] =
    cdb(Op.ModeSelect6, byte1, 0, 0, len, 0)

  private def u32(a: Array[Byte], off: Int): Long =
    java.nio.ByteBuffer.wrap(a, off, 4).getInt & 0xffffffffL

  private def sameSense(r: ScsiResult, expected: ScsiResult): Unit =
    r.status should be (expected.status)
    r.sense.toSeq should be (expected.sense.toSeq)

  // Golden vectors. Each is checked field by field against the tcmu-runner function named beside it.

  // scsi.c:tcmu_emulate_std_inquiry. Differences, by design: VERSION (byte 2) is 06h (SPC-4) where tcmu-runner
  // reports 05h, and 3PC (byte 5 bit 3) is clear because EXTENDED COPY is not implemented. TPGS is 0 as in
  // tcmu-runner with no ALUA port.
  private val GoldenInquiry = Array.concat(
    hex("00 00 06 02 1f 00 00 02"),
    ascii("LIO-ORG "),
    ascii("TCMU device     "),
    ascii("0002"))

  // scsi.c:tcmu_emulate_evpd_inquiry, case 0x83, with no ALUA port (its `goto finish_page83` path). Designator
  // 1: T10 vendor id, ASCII, "LIO-ORG " + wwn + NUL, length 8 + len + 1. Designator 2: NAA type 6, OpenFabrics
  // IEEE id 00 14 05, then one nibble per hex digit of the wwn, as the kernel builds it. Difference, by design:
  // tcmu-runner's third (vendor-specific) designator carries the handler cfgstring; it is omitted here.
  private val GoldenVpd83 = Array.concat(
    hex("00 83 00 45"),
    hex("02 01 00 2d"), ascii("LIO-ORG "), ascii(Wwn), hex("00"),
    hex("01 03 00 10 60 01 40 5a 5d 1e 2f 34 a5 b4 6c 78 d9 e0 f1 a2"))

  // scsi.c:tcmu_emulate_evpd_inquiry, case 0xb0, with unmap enabled and main.c's defaults: WSNZ = 1,
  // MAX_CAW_LENGTH = 1, max and optimal transfer = hw_max_sectors (1024), MAXIMUM UNMAP LBA COUNT =
  // VPD_MAX_UNMAP_LBA_COUNT (32 Mi), MAXIMUM UNMAP BLOCK DESCRIPTOR COUNT = VPD_MAX_UNMAP_BLOCK_DESC_COUNT (4),
  // MAXIMUM WRITE SAME LENGTH = VPD_MAX_WRITE_SAME_LENGTH. Difference, by design: OPTIMAL UNMAP GRANULARITY and
  // UNMAP GRANULARITY ALIGNMENT are one block object (16384 / 4096 = 4 blocks), where main.c uses
  // hw_max_sectors and 0. An alignment equal to the granularity is the same as 0 modulo the granularity.
  private val GoldenVpdB0 = Array.concat(
    hex("00 b0 00 3c 01 01 00 00"),
    hex("00 00 04 00"),
    hex("00 00 04 00"),
    hex("00 00 00 00"),
    hex("02 00 00 00"),
    hex("00 00 00 04"),
    hex("00 00 00 04"),
    hex("80 00 00 04"),
    hex("00 00 00 00 ff ff ff ff"),
    zeros(20))

  // scsi.c:tcmu_emulate_mode_sense over modesense_handlers (handle_rwrecovery_page, handle_cache_page,
  // handle_control_page), no block descriptors. Differences, by design: the header's device-specific parameter
  // has DPOFUA (10h) set, and the caching page has WCE (byte 2 bit 2) set because the disk is always write-back.
  private val GoldenModeSense6All = Array.concat(
    hex("2f 00 10 00"),
    hex("01 0a"), zeros(10),
    hex("08 12 04"), zeros(17),
    hex("0a 0a 02 00 00 40 00 00 ff ff 00 00"))

  test("DeviceSettings lbaCount"):
    settings.lbaCount should be (2048L)

  test("standard INQUIRY"):
    val full = ok(inquiry(evpd = false, 0))
    full.length should be (36)
    (full(0) & 0x1f) should be (0)
    full(2) should be (6.toByte)
    new String(full, 8, 8, US_ASCII) should be ("LIO-ORG ")
    new String(full, 16, 16, US_ASCII).trim should not be empty
    val short = ok(inquiry(evpd = false, 0, alloc = 20))
    short.toSeq should be (full.take(20).toSeq)
    ok(inquiry(evpd = false, 0, alloc = 36)).length should be (36)

  test("standard INQUIRY golden vector"):
    ok(inquiry(evpd = false, 0)).toSeq should be (GoldenInquiry.toSeq)

  test("standard INQUIRY with a page code is INVALID FIELD IN CDB"):
    sameSense(run(inquiry(evpd = false, 0x80))._1, Sense.InvalidFieldInCdb)

  test("VPD 0x00 lists 0x00 0x80 0x83 0xB0 0xB2"):
    val d = ok(inquiry(evpd = true, 0x00))
    d(1) should be (0.toByte)
    val n = d(3) & 0xff
    d.slice(4, 4 + n).map(_ & 0xff).toSeq should be (Seq(0x00, 0x80, 0x83, 0xB0, 0xB2))

  test("VPD 0x80 returns the wwn"):
    val d = ok(inquiry(evpd = true, 0x80))
    d(1) should be (0x80.toByte)
    // scsi.c: PAGE LENGTH counts the serial plus its NUL
    (d(3) & 0xff) should be (Wwn.length + 1)
    new String(d, 4, Wwn.length, US_ASCII) should be (Wwn)
    d(4 + Wwn.length) should be (0.toByte)

  test("a long wwn is cut so the one-byte lengths do not wrap"):
    val long = settings.copy(wwn = "f" * 300)
    def vpd(page: Int) = Emulation.handle(inquiry(evpd = true, page), long, () => Array.empty).get._2
    (vpd(0x80)(3) & 0xff) should be (255)
    val d83 = vpd(0x83)
    (d83(7) & 0xff) should be (255)
    java.nio.ByteBuffer.wrap(d83, 2, 2).getShort should be ((d83.length - 4).toShort)
    d83.length should be (4 + 4 + 255 + 20)

  test("VPD 0x83 has an NAA or T10 designator built as tcmu-runner builds it"):
    val d = ok(inquiry(evpd = true, 0x83))
    d.toSeq should be (GoldenVpd83.toSeq)
    java.nio.ByteBuffer.wrap(d, 2, 2).getShort should be ((d.length - 4).toShort)

  test("VPD 0xB0 unmap granularity is one block object"):
    val d = ok(inquiry(evpd = true, 0xB0))
    u32(d, 8) should be (1024L)
    u32(d, 28) should be (4L)
    (d(32) & 0x80) should be (0x80)
    (u32(d, 32) & 0x7fffffffL) should be (4L)
    d.toSeq should be (GoldenVpdB0.toSeq)

  test("VPD 0xB2 advertises UNMAP and WRITE SAME provisioning"):
    val d = ok(inquiry(evpd = true, 0xB2))
    d(1) should be (0xB2.toByte)
    d(3) should be (4.toByte)
    // LBPU 80h, LBPWS 40h, LBPWS10 20h, LBPRZ 04h
    (d(5) & 0xff) should be (0xE4)

  test("VPD responses are truncated to the allocation length"):
    val d = ok(inquiry(evpd = true, 0x83, alloc = 10))
    d.toSeq should be (GoldenVpd83.take(10).toSeq)

  test("unknown VPD page is INVALID FIELD IN CDB"):
    val (r, data) = run(inquiry(evpd = true, 0xB1))
    sameSense(r, Sense.InvalidFieldInCdb)
    data shouldBe empty

  test("READ CAPACITY 10 gives last LBA and block size"):
    val d = ok(cdb(Op.ReadCapacity10, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    d.length should be (8)
    u32(d, 0) should be (2047L)
    u32(d, 4) should be (4096L)

  test("READ CAPACITY 10 reports FFFFFFFFh beyond 32 bits"):
    val big = settings.copy(size = 4096L * 0x100000001L)
    val (r, d) = Emulation.handle(cdb(Op.ReadCapacity10, 0, 0, 0, 0, 0, 0, 0, 0, 0), big, () => Array.empty).get
    r.status should be (ScsiStatus.Good)
    u32(d, 0) should be (0xffffffffL)
    val lastFit = settings.copy(size = 4096L * 0x100000000L)
    val (_, d2) = Emulation.handle(cdb(Op.ReadCapacity10, 0, 0, 0, 0, 0, 0, 0, 0, 0), lastFit, () => Array.empty).get
    u32(d2, 0) should be (0xffffffffL)

  test("READ CAPACITY 10 with PMI is INVALID FIELD IN CDB"):
    sameSense(run(cdb(Op.ReadCapacity10, 0, 0, 0, 0, 0, 0, 0, 1, 0))._1, Sense.InvalidFieldInCdb)

  test("READ CAPACITY 16 sets LBPME"):
    // scsi.c:tcmu_emulate_read_capacity_16: last LBA, block size, LBPME | LBPRZ in byte 14
    val c = cdb(Op.ServiceActionIn16, 0x10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 32, 0, 0)
    val d = ok(c)
    d.toSeq should be (Array.concat(
      hex("00 00 00 00 00 00 07 ff 00 00 10 00 00 00 c0 00"), zeros(16)).toSeq)
    val short = cdb(Op.ServiceActionIn16, 0x10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 12, 0, 0)
    ok(short).toSeq should be (d.take(12).toSeq)

  test("other SERVICE ACTION IN 16 actions are not owned"):
    Emulation.handle(cdb(Op.ServiceActionIn16, 0x11, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 32, 0, 0),
      settings, () => Array.empty) should be (None)

  test("MODE SENSE 6 caching page has WCE and the header has DPOFUA"):
    val d = ok(modeSense6(0x08))
    (d(2) & 0x10) should not be (0)
    d.length should be (4 + 20)
    (d(0) & 0xff) should be (d.length - 1)
    d(4) should be (0x08.toByte)
    (d(4 + 2) & 0x04) should not be (0)

  test("MODE SENSE 6 all pages golden vector"):
    ok(modeSense6(0x3f)).toSeq should be (GoldenModeSense6All.toSeq)

  test("MODE SENSE 6 is truncated to the allocation length without changing the data length"):
    val d = ok(modeSense6(0x3f, alloc = 10))
    d.toSeq should be (GoldenModeSense6All.take(10).toSeq)
    ok(modeSense6(0x3f, alloc = 0)) shouldBe empty

  test("MODE SENSE 10 all pages contains caching and control"):
    val d = ok(modeSense10(0x3f))
    java.nio.ByteBuffer.wrap(d, 0, 2).getShort should be ((d.length - 2).toShort)
    (d(3) & 0x10) should not be (0)
    d.slice(8, d.length).toSeq should be (GoldenModeSense6All.drop(4).toSeq)
    d.length should be (8 + 44)

  test("MODE SENSE changeable values are all zero"):
    val d = ok(modeSense6(0x40 | 0x08))
    d.length should be (24)
    d(4) should be (0x08.toByte)
    d(5) should be (0x12.toByte)
    d.drop(6).forall(_ == 0) should be (true)

  test("MODE SENSE saved values are not supported"):
    sameSense(run(modeSense6(0xC0 | 0x08))._1, Sense.NotSuppSaveParams)

  test("MODE SENSE of an unknown page or subpage is INVALID FIELD IN CDB"):
    sameSense(run(modeSense6(0x1c))._1, Sense.InvalidFieldInCdb)
    sameSense(run(modeSense6(0x08, subpage = 1))._1, Sense.InvalidFieldInCdb)

  private val cachingPage = GoldenModeSense6All.slice(16, 36)

  test("MODE SELECT of the current caching page is GOOD"):
    val list = Array.concat(hex("00 00 00 00"), cachingPage)
    val (r, d) = run(modeSelect6(0x10, list.length), list)
    r should be (ScsiResult.Good)
    d shouldBe empty

  test("MODE SELECT 10 of the current pages is GOOD"):
    val list = Array.concat(zeros(8), GoldenModeSense6All.drop(4))
    val c = cdb(Op.ModeSelect10, 0x10, 0, 0, 0, 0, 0, 0, list.length, 0)
    run(c, list)._1 should be (ScsiResult.Good)

  test("MODE SELECT that clears WCE is INVALID FIELD IN PARAMETER LIST"):
    val page = cachingPage.clone()
    page(2) = 0
    val list = Array.concat(hex("00 00 00 00"), page)
    sameSense(run(modeSelect6(0x10, list.length), list)._1, Sense.InvalidFieldInParamList)

  test("MODE SELECT with SP set is rejected"):
    val list = Array.concat(hex("00 00 00 00"), cachingPage)
    sameSense(run(modeSelect6(0x11, list.length), list)._1, Sense.NotSuppSaveParams)

  test("MODE SELECT without PF is INVALID FIELD IN CDB"):
    val list = Array.concat(hex("00 00 00 00"), cachingPage)
    sameSense(run(modeSelect6(0x00, list.length), list)._1, Sense.InvalidFieldInCdb)

  test("MODE SELECT with a truncated page is PARAMETER LIST LENGTH ERROR"):
    val list = Array.concat(hex("00 00 00 00"), cachingPage.take(10))
    sameSense(run(modeSelect6(0x10, list.length), list)._1, Sense.ParamListLength)
    sameSense(run(modeSelect6(0x10, 2), list.take(2))._1, Sense.ParamListLength)

  test("MODE SELECT of an unknown page or with block descriptors is INVALID FIELD IN PARAMETER LIST"):
    val unknown = Array.concat(hex("00 00 00 00 1c 02 00 00"))
    sameSense(run(modeSelect6(0x10, unknown.length), unknown)._1, Sense.InvalidFieldInParamList)
    val withBd = Array.concat(hex("00 00 00 08"), zeros(8), cachingPage)
    sameSense(run(modeSelect6(0x10, withBd.length), withBd)._1, Sense.InvalidFieldInParamList)

  test("MODE SELECT with a zero parameter list length is GOOD"):
    run(modeSelect6(0x10, 0))._1 should be (ScsiResult.Good)

  test("TEST UNIT READY is GOOD"):
    val (r, d) = run(cdb(Op.TestUnitReady, 0, 0, 0, 0, 0))
    r should be (ScsiResult.Good)
    d shouldBe empty

  test("REQUEST SENSE reports no sense"):
    val d = ok(cdb(Op.RequestSense, 0, 0, 0, 252, 0))
    d.toSeq should be (Sense.fixed(0, 0).toSeq)
    ok(cdb(Op.RequestSense, 0, 0, 0, 8, 0)).length should be (8)
    ok(cdb(Op.RequestSense, 1, 0, 0, 252, 0)).toSeq should be (hex("72 00 00 00 00 00 00 00").toSeq)

  test("START STOP UNIT"):
    // scsi.c:tcmu_emulate_start_stop: START is GOOD; a STOP or a power condition is INVALID FIELD IN CDB
    run(cdb(Op.StartStop, 0, 0, 0, 0x01, 0))._1 should be (ScsiResult.Good)
    sameSense(run(cdb(Op.StartStop, 0, 0, 0, 0x00, 0))._1, Sense.InvalidFieldInCdb)
    sameSense(run(cdb(Op.StartStop, 0, 0, 0, 0x11, 0))._1, Sense.InvalidFieldInCdb)

  test("a data opcode returns None"):
    Emulation.handle(cdb(Op.Read10, 0, 0, 0, 0, 0, 0, 0, 1, 0), settings, () => Array.empty) should be (None)
    Emulation.handle(cdb(Op.MaintenanceIn, 0x0c, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), settings,
      () => Array.empty) should be (None)

  test("a short CDB for an owned opcode is INVALID FIELD IN CDB"):
    sameSense(run(cdb(Op.Inquiry, 0, 0))._1, Sense.InvalidFieldInCdb)
