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

  test("a GSO send stays within the kernel's 64 segments and 64 KiB") {
    QuicConfig().gsoSegments shouldBe 44
    QuicConfig(maxUdpPayloadBytes = 1200).gsoSegments shouldBe 54
    QuicConfig(maxUdpPayloadBytes = 9000).gsoSegments shouldBe 7
    QuicConfig(maxUdpPayloadBytes = 65000).gsoSegments shouldBe 1
  }

  test("payload sizes below QUIC's minimum or above one GSO send are refused") {
    an[IllegalArgumentException] should be thrownBy QuicConfig(maxUdpPayloadBytes = 1199)
    an[IllegalArgumentException] should be thrownBy QuicConfig(maxUdpPayloadBytes = 65001)
  }
