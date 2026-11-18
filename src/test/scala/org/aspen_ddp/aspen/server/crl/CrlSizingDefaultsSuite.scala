package org.aspen_ddp.aspen.server.crl

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.server.StorageDeviceConfig
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class CrlSizingDefaultsSuite extends AnyFunSuite with Matchers:

  private val zero = new UUID(0, 0)
  private val base = StorageDeviceConfig(StorageDeviceId(zero), zero)

  test("a 1 TiB device resolves to 4 streams of 512 MiB, capped at 20, 5 percent free"):
    CrlSizingDefaults.resolve(base, 1L << 40) should be(CrlSizing(4, 512L << 20, 20, 5))

  test("a small device takes the 64 MiB floor"):
    CrlSizingDefaults.resolve(base, 10L << 30).streamBytes should be(64L << 20)

  test("the default stream size is a 4 KiB multiple no larger than capacity / 2048"):
    val capacity = (1L << 40) + 12345
    val bytes = CrlSizingDefaults.resolve(base, capacity).streamBytes
    (bytes % 4096) should be(0L)
    bytes should be <= (capacity / 2048)

  test("an explicit file size is honored whatever the capacity"):
    CrlSizingDefaults.resolve(base.copy(crlFileSizeMb = Some(2)), 1L << 40).streamBytes should be(2L << 20)

  test("fewer than 4 streams is refused"):
    val e = the[IllegalArgumentException] thrownBy
      CrlSizingDefaults.resolve(base.copy(crlNumStreams = Some(3)), 1L << 40)
    e.getMessage should include("at least 4")

  test("a maximum below the stream count is refused"):
    an[IllegalArgumentException] should be thrownBy
      CrlSizingDefaults.resolve(base.copy(crlMaxStreams = Some(3)), 1L << 40)

  test("a free percentage above 100 is refused"):
    an[IllegalArgumentException] should be thrownBy
      CrlSizingDefaults.resolve(base.copy(crlMinFreePercent = Some(101)), 1L << 40)

  test("an existing CRL's config never throws: invalid growth bounds disable growth"):
    val bad = base.copy(crlNumStreams = Some(3), crlMaxStreams = Some(2), crlMinFreePercent = Some(101))
    val s = CrlSizingDefaults.resolveExisting(bad, 1L << 40)
    s.maxStreams should be(0)
    s.minFreePercent should be(100)
    CrlSizingDefaults.resolveExisting(base, 1L << 40).maxStreams should be(20)
