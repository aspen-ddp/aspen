package org.aspen_ddp.aspen.vdisk.tcmu

import java.nio.ByteBuffer

import org.aspen_ddp.aspen.vdisk.{DiskGeometry, VDiskError}
import org.aspen_ddp.aspen.vdisk.tcmu.scsi.{Op, ScsiCommands, ScsiResult, ScsiStatus, Sense}
import org.scalatest.Assertion
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future

class ScsiCommandsSuite extends AsyncFunSuite with Matchers:

  private val Mi = 1024L * 1024
  private val Bs = 4096
  private val settings = DeviceSettings(8 * Mi, Bs, 1024, 16384, "vd1", "wwn")

  private def fixture(geo: DiskGeometry = DiskGeometry(8 * Mi, Bs, 16384)): (FakeBlockDevice, ScsiCommands) =
    val dev = new FakeBlockDevice(geo)
    (dev, new ScsiCommands(dev, settings))

  private def cdb(bytes: Int*): Array[Byte] = bytes.map(_.toByte).toArray

  private def be(v: Long, n: Int): Seq[Int] = (n - 1 to 0 by -1).map(i => ((v >> (8 * i)) & 0xff).toInt)

  private def read6(lba: Int, len: Int) = cdb(Op.Read6, (lba >> 16) & 0x1f, (lba >> 8) & 0xff, lba & 0xff, len, 0)
  private def read10(lba: Long, len: Int) = cdb((Seq(Op.Read10, 0) ++ be(lba, 4) ++ Seq(0) ++ be(len, 2) :+ 0)*)
  private def read16(lba: Long, len: Long) = cdb((Seq(Op.Read16, 0) ++ be(lba, 8) ++ be(len, 4) ++ Seq(0, 0))*)
  private def write10(lba: Long, len: Int) = cdb((Seq(Op.Write10, 0) ++ be(lba, 4) ++ Seq(0) ++ be(len, 2) :+ 0)*)
  private def write16(lba: Long, len: Long, fua: Boolean) =
    cdb((Seq(Op.Write16, if fua then 0x08 else 0) ++ be(lba, 8) ++ be(len, 4) ++ Seq(0, 0))*)
  private def writeVerify10(lba: Long, len: Int) =
    cdb((Seq(Op.WriteVerify10, 0) ++ be(lba, 4) ++ Seq(0) ++ be(len, 2) :+ 0)*)
  private def writeSame16(lba: Long, len: Long, unmap: Boolean) =
    cdb((Seq(Op.WriteSame16, if unmap then 0x08 else 0) ++ be(lba, 8) ++ be(len, 4) ++ Seq(0, 0))*)
  private def caw(lba: Long, n: Int) = cdb((Seq(Op.CompareAndWrite, 0) ++ be(lba, 8) ++ Seq(0, 0, 0, n, 0, 0))*)
  private def unmapCdb(paramLen: Int) = cdb((Seq(Op.Unmap, 0, 0, 0, 0, 0, 0) ++ be(paramLen, 2) :+ 0)*)
  private val syncCache10 = cdb(Op.SyncCache10, 0, 0, 0, 0, 0, 0, 0, 0, 0)
  private val testUnitReady = cdb(0, 0, 0, 0, 0, 0)
  private val readCapacity10 = cdb(Op.ReadCapacity10, 0, 0, 0, 0, 0, 0, 0, 0, 0)
  private val inquiry = cdb(Op.Inquiry, 0, 0, 0, 36, 0)

  /** UNMAP parameter list of (lba, blocks) descriptors; `bddl` overrides the descriptor data length */
  private def unmapList(descs: List[(Long, Int)], bddl: Option[Int] = None): Array[Byte] =
    val bb = ByteBuffer.allocate(8 + 16 * descs.length)
    bb.putShort(0, (6 + 16 * descs.length).toShort)
    bb.putShort(2, bddl.getOrElse(16 * descs.length).toShort)
    descs.zipWithIndex.foreach: (d, i) =>
      bb.putLong(8 + 16 * i, d._1)
      bb.putInt(16 + 16 * i, d._2)
    bb.array()

  private def run(
      cmds: ScsiCommands,
      c: Array[Byte],
      data: Array[Byte] = Array.empty,
      capacity: Long = 1 << 24): Future[(ScsiResult, Array[Byte])] =
    cmds.handle(c, () => data, capacity)

  private def senseOf(r: ScsiResult): (Int, Int) =
    (r.sense(2) & 0x0f, ((r.sense(12) & 0xff) << 8) | (r.sense(13) & 0xff))

  private def expectSense(r: ScsiResult, expected: ScsiResult): Assertion =
    r.status should be (ScsiStatus.CheckCondition)
    senseOf(r) should be (senseOf(expected))

  private def good(r: ScsiResult): Assertion = r.status should be (ScsiStatus.Good)

  private def pattern(n: Int, seed: Int): Array[Byte] = Array.tabulate(n)(i => ((i * 7 + seed) % 256).toByte)

  test("READ 10 maps to device.read"):
    val (dev, cmds) = fixture()
    run(cmds, read10(2, 3), capacity = 3 * Bs).map: (r, data) =>
      good(r)
      r.readLen should be (Some(12288))
      dev.logged should be (List(("read", List(8192L, 12288))))
      data.toList should be (dev.readData(8192L, 12288).toList)

  test("WRITE 16 with FUA passes fua = true"):
    val (dev, cmds) = fixture()
    val data = pattern(2 * Bs, 1)
    for
      (r1, _) <- run(cmds, write16(4, 2, fua = true), data)
      (r2, _) <- run(cmds, write16(4, 2, fua = false), data)
    yield
      good(r1)
      good(r2)
      dev.logged should be (List(
        ("write", List(4L * Bs, data.toList, true)),
        ("write", List(4L * Bs, data.toList, false))))

  test("zero-length READ 10 completes without a device call"):
    val (dev, cmds) = fixture()
    for
      (r1, d1) <- run(cmds, read10(5, 0), capacity = 0)
      (r2, _) <- run(cmds, write10(5, 0))
    yield
      good(r1)
      d1 should be (empty)
      good(r2)
      dev.calls should be (empty)

  test("READ 6 length 0 reads 256 blocks"):
    val (dev, cmds) = fixture()
    run(cmds, read6(1, 0), capacity = 256 * Bs).map: (r, data) =>
      good(r)
      data.length should be (256 * Bs)
      dev.logged should be (List(("read", List(Bs.toLong, 256 * Bs))))

  test("LBA plus length overflow is out of range"):
    val (dev, cmds) = fixture()
    run(cmds, read16(0xFFFFFFFFFFFFFFF0L, 32), capacity = 32 * Bs).map: (r, _) =>
      expectSense(r, Sense.LbaOutOfRange)
      dev.calls should be (empty)

  test("range checked against the kernel size, not the Aspen size"):
    val (dev, cmds) = fixture(DiskGeometry(16 * Mi, Bs, 16384))
    for
      (r1, _) <- run(cmds, read10(2048, 1), capacity = Bs)
      (r2, _) <- run(cmds, read10(2047, 1), capacity = Bs)
    yield
      expectSense(r1, Sense.LbaOutOfRange)
      good(r2)
      dev.logged should be (List(("read", List(2047L * Bs, Bs))))

  test("payload length mismatch is INVALID FIELD IN CDB"):
    val (dev, cmds) = fixture()
    for
      (r1, _) <- run(cmds, write10(0, 2), pattern(Bs, 0))
      (r2, _) <- run(cmds, read10(0, 2), capacity = Bs)
    yield
      expectSense(r1, Sense.InvalidFieldInCdb)
      expectSense(r2, Sense.InvalidFieldInCdb)
      dev.calls should be (empty)

  test("SYNCHRONIZE CACHE flushes"):
    val (dev, cmds) = fixture()
    run(cmds, syncCache10).map: (r, _) =>
      good(r)
      dev.logged should be (List(("flush", Nil)))

  test("UNMAP sends every descriptor in one call"):
    val (dev, cmds) = fixture()
    val list = unmapList(List((0L, 4), (100L, 0), (8L, 12), (2040L, 8)))
    run(cmds, unmapCdb(list.length), list).map: (r, _) =>
      good(r)
      dev.logged should be (List(("unmap", List(List((0L, 4L * Bs), (8L * Bs, 12L * Bs), (2040L * Bs, 8L * Bs))))))

  test("UNMAP with inconsistent lengths is rejected whole"):
    val (dev, cmds) = fixture()
    // The descriptor data length claims two descriptors but the list holds one
    val short = unmapList(List((0L, 4)), bddl = Some(32))
    val pastEnd = unmapList(List((0L, 4), (2047L, 2)))
    for
      (r1, _) <- run(cmds, unmapCdb(short.length), short)
      (r2, _) <- run(cmds, unmapCdb(pastEnd.length), pastEnd)
    yield
      expectSense(r1, Sense.ParamListLength)
      expectSense(r2, Sense.LbaOutOfRange)
      dev.calls should be (empty)

  test("UNMAP enforces the advertised block limits"):
    val (dev, cmds) = fixture(DiskGeometry(1L << 40, Bs, 16384))
    val big = new ScsiCommands(dev, settings.copy(size = 1L << 40))
    val five = unmapList(List.tabulate(5)(i => (i * 8L, 4)))
    val tooLong = unmapList(List((0L, 32 * 1024 * 1024 + 1)))
    val maxLong = unmapList(List((0L, 32 * 1024 * 1024)))
    for
      (r1, _) <- run(big, unmapCdb(five.length), five)
      (r2, _) <- run(big, unmapCdb(tooLong.length), tooLong)
      _ = dev.calls should be (empty)
      (r3, _) <- run(big, unmapCdb(maxLong.length), maxLong)
    yield
      expectSense(r1, Sense.InvalidFieldInParamList)
      expectSense(r2, Sense.InvalidFieldInParamList)
      good(r3)
      dev.logged should be (List(("unmap", List(List((0L, 32L * 1024 * 1024 * Bs))))))

  test("WRITE SAME with UNMAP bit unmaps"):
    val (dev, cmds) = fixture()
    run(cmds, writeSame16(8, 16, unmap = true), new Array[Byte](Bs)).map: (r, _) =>
      good(r)
      dev.logged should be (List(("unmap", List(List((8L * Bs, 16L * Bs))))))

  test("WRITE SAME with UNMAP bit writes when unaligned or not zero"):
    val (dev, cmds) = fixture()
    val p = pattern(Bs, 3)
    for
      (r1, _) <- run(cmds, writeSame16(9, 2, unmap = true), new Array[Byte](Bs))
      (r2, _) <- run(cmds, writeSame16(8, 4, unmap = true), p)
    yield
      good(r1)
      good(r2)
      dev.logged should be (List(
        ("write", List(9L * Bs, new Array[Byte](2 * Bs).toList, false)),
        ("write", List(8L * Bs, (p ++ p ++ p ++ p).toList, false))))

  test("WRITE SAME without UNMAP writes the repeated block"):
    val (dev, cmds) = fixture()
    val p = pattern(Bs, 5)
    run(cmds, writeSame16(3, 3, unmap = false), p).map: (r, _) =>
      good(r)
      dev.logged should be (List(("write", List(3L * Bs, (p ++ p ++ p).toList, false))))

  test("WRITE SAME is written in chunks of at most maxSectors"):
    val dev = new FakeBlockDevice()
    val cmds = new ScsiCommands(dev, settings.copy(maxSectors = 2))
    val p = pattern(Bs, 9)
    run(cmds, writeSame16(0, 5, unmap = false), p).map: (r, _) =>
      good(r)
      dev.logged.map(c => (c._2(0), c._2(1).asInstanceOf[List[Byte]].length)) should be (List(
        (0L, 2 * Bs), (2L * Bs, 2 * Bs), (4L * Bs, Bs)))

  test("WRITE SAME rejects a zero count, a bad payload and an out-of-range block"):
    val (dev, cmds) = fixture()
    for
      (r1, _) <- run(cmds, writeSame16(0, 0, unmap = false), pattern(Bs, 0))
      (r2, _) <- run(cmds, writeSame16(0, 1, unmap = false), pattern(2 * Bs, 0))
      (r3, _) <- run(cmds, writeSame16(2047, 2, unmap = true), new Array[Byte](Bs))
    yield
      expectSense(r1, Sense.InvalidFieldInCdb)
      expectSense(r2, Sense.InvalidFieldInCdb)
      expectSense(r3, Sense.LbaOutOfRange)
      dev.calls should be (empty)

  test("WRITE AND VERIFY writes then flushes"):
    val (dev, cmds) = fixture()
    val data = pattern(Bs, 2)
    run(cmds, writeVerify10(7, 1), data).map: (r, _) =>
      good(r)
      dev.logged should be (List(("write", List(7L * Bs, data.toList, false)), ("flush", Nil)))

  test("COMPARE AND WRITE writes on match"):
    val (dev, cmds) = fixture()
    val current = dev.readData(5L * Bs, Bs)
    val next = pattern(Bs, 11)
    run(cmds, caw(5, 1), current ++ next).map: (r, _) =>
      good(r)
      dev.logged should be (List(("read", List(5L * Bs, Bs)), ("write", List(5L * Bs, next.toList, false))))

  test("COMPARE AND WRITE miscompare reports the offset and writes nothing"):
    val (dev, cmds) = fixture()
    val expected = dev.readData(5L * Bs, Bs).clone()
    expected(100) = (expected(100) + 1).toByte
    run(cmds, caw(5, 1), expected ++ pattern(Bs, 11)).map: (r, _) =>
      r.status should be (ScsiStatus.CheckCondition)
      r.sense.toList should be (Sense.Miscompare(100).sense.toList)
      dev.names should be (List("read"))

  test("COMPARE AND WRITE rejects more than one block"):
    val (dev, cmds) = fixture()
    for
      (r1, _) <- run(cmds, caw(5, 2), new Array[Byte](4 * Bs))
      (r2, _) <- run(cmds, caw(5, 0))
    yield
      expectSense(r1, Sense.InvalidFieldInCdb)
      good(r2)
      dev.calls should be (empty)

  test("COMPARE AND WRITE serializes against an overlapping write"):
    val (dev, cmds) = fixture()
    dev.auto = false
    val current = dev.readData(5L * Bs, Bs)
    val cawData = pattern(Bs, 11)
    val other = pattern(Bs, 13)
    val cawDone = run(cmds, caw(5, 1), current ++ cawData)
    dev.call(0).flatMap: readCall =>
      val writeDone = run(cmds, write10(5, 1), other)
      // The WRITE must wait for the CAW, so only the CAW's read has reached the device
      dev.names should be (List("read"))
      dev.auto = true
      readCall.succeed(current)
      for
        (r1, _) <- cawDone
        (r2, _) <- writeDone
      yield
        good(r1)
        good(r2)
        dev.logged should be (List(
          ("read", List(5L * Bs, Bs)),
          ("write", List(5L * Bs, cawData.toList, false)),
          ("write", List(5L * Bs, other.toList, false))))

  test("COMPARE AND WRITE waits for an overlapping in-flight write"):
    val (dev, cmds) = fixture()
    dev.auto = false
    val current = dev.readData(5L * Bs, Bs)
    val writeDone = run(cmds, write10(4, 2), pattern(2 * Bs, 1))
    dev.call(0).flatMap: writeCall =>
      val cawDone = run(cmds, caw(5, 1), current ++ pattern(Bs, 2))
      // A non-overlapping write is not held up
      val apartDone = run(cmds, write10(100, 1), pattern(Bs, 3))
      dev.call(1).flatMap: apartCall =>
        dev.names should be (List("write", "write"))
        apartCall.args.head should be (100L * Bs)
        dev.auto = true
        apartCall.succeed()
        writeCall.succeed()
        for
          (r1, _) <- writeDone
          (r2, _) <- cawDone
          (r3, _) <- apartDone
        yield
          List(r1, r2, r3).foreach(good)
          dev.names should be (List("write", "write", "read", "write"))

  test("device failures map through ErrorMap"):
    val (dev, cmds) = fixture()
    dev.failures = Map("read" -> VDiskError.Fenced)
    for
      (fenced, _) <- run(cmds, read10(0, 1), capacity = Bs)
      _ = dev.failures = Map("read" -> VDiskError.DiskFailed("x"), "write" -> VDiskError.DiskFailed("y"))
      (readFail, _) <- run(cmds, read10(0, 1), capacity = Bs)
      (writeFail, _) <- run(cmds, write10(0, 1), pattern(Bs, 0))
    yield
      expectSense(fenced, Sense.Fenced)
      expectSense(readFail, Sense.ReadError)
      expectSense(writeFail, Sense.WriteError)

  test("a synchronous throw from the device becomes HARDWARE ERROR"):
    val (dev, cmds) = fixture()
    dev.throws = Map("read" -> new RuntimeException("boom"), "flush" -> new IllegalStateException("bang"))
    for
      (r1, d1) <- run(cmds, read10(0, 1), capacity = Bs)
      (r2, _) <- run(cmds, syncCache10)
    yield
      expectSense(r1, Sense.HardwareError)
      d1 should be (empty)
      expectSense(r2, Sense.HardwareError)

  test("a dataOut that throws becomes HARDWARE ERROR"):
    val (_, cmds) = fixture()
    cmds.handle(write10(0, 1), () => throw new RuntimeException("gather"), 0).map: (r, _) =>
      expectSense(r, Sense.HardwareError)

  test("capacity change unit attention is reported once"):
    val (_, cmds) = fixture()
    cmds.resized(16 * Mi)
    for
      (r1, _) <- run(cmds, testUnitReady)
      (r2, _) <- run(cmds, testUnitReady)
      (r3, cap) <- run(cmds, readCapacity10)
    yield
      cmds.settings.size should be (16 * Mi)
      expectSense(r1, Sense.CapacityChanged)
      good(r2)
      good(r3)
      ByteBuffer.wrap(cap).getInt(0) should be (4095)

  test("INQUIRY does not consume the unit attention"):
    val (_, cmds) = fixture()
    cmds.resized(16 * Mi)
    for
      (r1, d1) <- run(cmds, inquiry)
      (r2, _) <- run(cmds, cdb(Op.RequestSense, 0, 0, 0, 18, 0))
      (r3, _) <- run(cmds, cdb(0xA0, 0, 0, 0, 0, 0, 0, 0, 0, 16, 0, 0))
      (r4, _) <- run(cmds, testUnitReady)
    yield
      good(r1)
      r1.readLen should be (Some(36))
      d1.length should be (36)
      good(r2)
      expectSense(r3, Sense.InvalidOpcode)
      expectSense(r4, Sense.CapacityChanged)

  test("emulated data-in is limited to the data-in capacity"):
    val (_, cmds) = fixture()
    run(cmds, inquiry, capacity = 8).map: (r, d) =>
      good(r)
      r.readLen should be (Some(8))
      d.length should be (8)

  test("FORMAT UNIT unmaps the whole device"):
    val (dev, cmds) = fixture(DiskGeometry(16 * Mi, Bs, 16384))
    run(cmds, cdb(Op.FormatUnit, 0, 0, 0, 0, 0)).map: (r, _) =>
      good(r)
      dev.logged should be (List(("unmap", List(List((0L, 8 * Mi))))))

  test("unknown opcode is INVALID OPCODE"):
    val (dev, cmds) = fixture()
    for
      (r1, _) <- run(cmds, cdb(0xC5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
      (r2, _) <- run(cmds, cdb(Op.MaintenanceIn, 0x0C, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
      (r3, _) <- run(cmds, Array.emptyByteArray)
    yield
      expectSense(r1, Sense.InvalidOpcode)
      expectSense(r2, Sense.InvalidOpcode)
      expectSense(r3, Sense.InvalidOpcode)
      dev.calls should be (empty)

  test("a short data CDB is INVALID FIELD IN CDB"):
    val (dev, cmds) = fixture()
    run(cmds, cdb(Op.Read10, 0, 0, 0)).map: (r, _) =>
      expectSense(r, Sense.InvalidFieldInCdb)
      dev.calls should be (empty)
