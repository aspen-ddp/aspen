package org.aspen_ddp.aspen.common.network.implementations.quic

import io.netty.handler.codec.quic.Quic
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class QuicNativeSuite extends AnyFunSuite with Matchers:

  test("native QUIC library loads"):
    Quic.ensureAvailability()
    Quic.isAvailable shouldBe true
