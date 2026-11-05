package org.aspen_ddp.aspen.common.network.implementations.quic

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class QuicConfigSuite extends AnyFunSuite with Matchers:
  private val MiB = 1024L * 1024
  private val GiB = 1024 * MiB

  test("send buffering scales with the heap") {
    QuicConfig.defaultSendBudget(16 * GiB) shouldBe 4 * GiB
    QuicConfig.defaultConnectionCeiling(16 * GiB) shouldBe GiB
  }

  test("a small heap keeps the minimums") {
    QuicConfig.defaultSendBudget(GiB) shouldBe 256 * MiB
    QuicConfig.defaultConnectionCeiling(GiB) shouldBe 64 * MiB
  }
