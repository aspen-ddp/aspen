package org.aspen_ddp.aspen.vdisk.tcmu

import org.aspen_ddp.aspen.vdisk.VDiskError
import org.aspen_ddp.aspen.vdisk.tcmu.scsi.{ErrorMap, ScsiResult, ScsiStatus, Sense}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SenseSuite extends AnyFunSuite with Matchers:

  test("fixed sense layout"):
    val s = Sense.fixed(5, 0x2100)
    s.length should be (18)
    s(0) should be (0x70.toByte)
    s(2) should be (5.toByte)
    s(7) should be (10.toByte)
    s(12) should be (0x21.toByte)
    s(13) should be (0.toByte)

  test("miscompare carries the offset"):
    val r = Sense.Miscompare(1234)
    r.status should be (ScsiStatus.CheckCondition)
    val s = r.sense
    (s(0) & 0x80) should be (0x80)
    s(2) should be (0x0E.toByte)
    s(12) should be (0x1D.toByte)
    s(13) should be (0.toByte)
    java.nio.ByteBuffer.wrap(s, 3, 4).getInt should be (1234)

  test("result constants"):
    def chk(r: ScsiResult, key: Int, asc: Int, ascq: Int): Unit =
      r.status should be (ScsiStatus.CheckCondition)
      r.sense.toSeq should be (Sense.fixed(key, (asc << 8) | ascq).toSeq)
    chk(Sense.LbaOutOfRange, 5, 0x21, 0)
    chk(Sense.InvalidFieldInCdb, 5, 0x24, 0)
    chk(Sense.InvalidFieldInParamList, 5, 0x26, 0)
    chk(Sense.ParamListLength, 5, 0x1A, 0)
    chk(Sense.InvalidOpcode, 5, 0x20, 0)
    chk(Sense.HardwareError, 4, 0x44, 0)
    chk(Sense.ReadError, 3, 0x11, 0)
    chk(Sense.WriteError, 3, 0x0C, 0)
    chk(Sense.Fenced, 2, 0x04, 0x0B)
    chk(Sense.CapacityChanged, 6, 0x2A, 0x09)
    chk(Sense.NotSuppSaveParams, 5, 0x39, 0)
    ScsiResult.Good.status should be (ScsiStatus.Good)
    ScsiResult.good(512).readLen should be (Some(512))

  private def same(actual: ScsiResult, expected: ScsiResult): Unit =
    actual.status should be (expected.status)
    actual.sense.toSeq should be (expected.sense.toSeq)

  test("error map"):
    same(ErrorMap.result(VDiskError.InvalidRequest("x"), false), Sense.InvalidFieldInCdb)
    same(ErrorMap.result(VDiskError.InvalidGeometry("x"), true), Sense.InvalidFieldInCdb)
    same(ErrorMap.result(VDiskError.OutOfRange("x"), true), Sense.LbaOutOfRange)
    same(ErrorMap.result(VDiskError.Fenced, true), Sense.Fenced)
    same(ErrorMap.result(VDiskError.NotOwner, false), Sense.Fenced)
    same(ErrorMap.result(VDiskError.DiskFailed("x"), true), Sense.WriteError)
    same(ErrorMap.result(VDiskError.DiskFailed("x"), false), Sense.ReadError)
    same(ErrorMap.result(new RuntimeException("boom"), false), Sense.HardwareError)
