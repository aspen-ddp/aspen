package org.aspen_ddp.aspen.common.ida

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Allocation groups decide IDA membership with `==`, so it must compare the concrete type and every parameter */
class IDAEqualitySuite extends AnyFunSuite with Matchers:

  test("identically constructed IDAs are equal"):
    (Replication(3, 2) == Replication(3, 2)) shouldBe true
    (ReedSolomon(5, 3, 4) == ReedSolomon(5, 3, 4)) shouldBe true
    (RandomAccessReedSolomon(5, 3, 4, 4096) == RandomAccessReedSolomon(5, 3, 4, 4096)) shouldBe true

  test("IDAs differing in any parameter are not equal"):
    (Replication(3, 2) == Replication(3, 3)) shouldBe false
    (Replication(3, 2) == Replication(5, 3)) shouldBe false
    (ReedSolomon(5, 3, 4) == ReedSolomon(5, 3, 5)) shouldBe false
    (ReedSolomon(5, 3, 4) == ReedSolomon(5, 4, 4)) shouldBe false
    (ReedSolomon(5, 3, 4) == ReedSolomon(7, 4, 5)) shouldBe false
    (RandomAccessReedSolomon(5, 3, 4, 4096) == RandomAccessReedSolomon(5, 3, 4, 8192)) shouldBe false
    (RandomAccessReedSolomon(5, 3, 4, 4096) == RandomAccessReedSolomon(5, 3, 5, 4096)) shouldBe false

  test("IDAs of different types are not equal even with matching width and thresholds"):
    val rep: IDA = Replication(3, 2)
    val rs: IDA = ReedSolomon(3, 2, 2)
    val rars: IDA = RandomAccessReedSolomon(3, 2, 2)
    (rep == rs) shouldBe false
    (rs == rars) shouldBe false
    (rep == rars) shouldBe false

  test("compare is not an equality test"):
    // Ordering looks only at failureTolerance, so unequal IDAs can compare as 0. Membership checks must use ==.
    val a: IDA = Replication(3, 3)
    val b: IDA = Replication(5, 5)
    a.compare(b) shouldBe 0
    (a == b) shouldBe false
