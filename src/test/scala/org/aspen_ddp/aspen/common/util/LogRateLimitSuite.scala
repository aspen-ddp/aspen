package org.aspen_ddp.aspen.common.util

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class LogRateLimitSuite extends AnyFunSuite with Matchers:

  test("the first line passes, then at most one per period, with the number held back since the last"):
    var now = 1_000L
    val limit = new LogRateLimit(10_000, () => now)
    limit.admit() shouldBe Some(0L)
    limit.admit() shouldBe None
    now += 9_999
    limit.admit() shouldBe None
    now += 1
    limit.admit() shouldBe Some(2L)
    limit.admit() shouldBe None
    now += 20_000
    limit.admit() shouldBe Some(1L)

  test("a clock stepped backward lets the next line through rather than holding lines back for good"):
    var now = 1_000_000L
    val limit = new LogRateLimit(10_000, () => now)
    limit.admit() shouldBe Some(0L)
    now -= 500_000
    limit.admit() shouldBe Some(0L)
    limit.admit() shouldBe None

  test("heldText names the held-back lines only when there are some"):
    LogRateLimit.heldText(0) shouldBe ""
    LogRateLimit.heldText(3) should include ("3")
