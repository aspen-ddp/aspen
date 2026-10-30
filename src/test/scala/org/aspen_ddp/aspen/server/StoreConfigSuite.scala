package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.YamlFormat
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.lang.management.ManagementFactory
import java.util.UUID

class StoreConfigSuite extends AnyFunSuite with Matchers:

  private val zero = UUID.fromString("00000000-0000-0000-0000-000000000000")

  private val sample = StoreConfig(
    StoreId(PoolId(UUID.fromString("44444444-4444-4444-4444-444444444444")), 3.asInstanceOf[Byte]),
    StoreConfig.RocksDB(None), None)

  test("configFilename is aspen-store-config.yaml"):
    StoreConfig.configFilename should be("aspen-store-config.yaml")

  test("yamlConfig emits a bare store id"):
    sample.yamlConfig should include(
      "store-id: 44444444-4444-4444-4444-444444444444:3")
    sample.yamlConfig should not include "StoreId("

  test("loadStoreConfig round-trips yamlConfig"):
    val f = os.temp(prefix = "aspen-store-config", suffix = ".yaml")
    try
      os.write.over(f, sample.yamlConfig)
      StoreConfig.loadStoreConfig(f) should be(sample)
    finally
      os.remove(f)

  test("doc-comment example config parses correctly"):
    val docExampleYaml = """store-id: 00000000-0000-0000-0000-000000000000:0
backend:
  storage-engine: rocksdb
"""

    val f = os.temp(prefix = "aspen-store-docexample", suffix = ".yaml")
    try
      os.write.over(f, docExampleYaml)
      val cfg = StoreConfig.loadStoreConfig(f)

      cfg.storeId should be(StoreId(PoolId(zero), 0.asInstanceOf[Byte]))
      cfg.backend should be(StoreConfig.RocksDB(None))
      cfg.poolMaxSliceSize should be(None)

      StoreConfig(StoreId(PoolId(zero), 0.asInstanceOf[Byte]),
                  StoreConfig.RocksDB(None), None).yamlConfig should be(docExampleYaml)
    finally
      os.remove(f)

  private val storeId0 = StoreId(PoolId(zero), 0.asInstanceOf[Byte])

  private def roundTrip(cfg: StoreConfig): StoreConfig =
    val f = os.temp(prefix = "aspen-store-limits", suffix = ".yaml")
    try
      os.write.over(f, cfg.yamlConfig)
      StoreConfig.loadStoreConfig(f)
    finally
      os.remove(f)

  test("yaml round trip with both limits"):
    val cfg = StoreConfig(storeId0, StoreConfig.RocksDB(Some(2048)), Some(4096))
    cfg.yamlConfig should include("pool-max-slice-size: 4096")
    cfg.yamlConfig should include("max-slice-size: 2048")
    roundTrip(cfg) should be(cfg)

  test("yaml round trip with only the pool limit"):
    val cfg = StoreConfig(storeId0, StoreConfig.RocksDB(None), Some(4096))
    roundTrip(cfg) should be(cfg)

  test("yaml round trip with only the backend limit"):
    val cfg = StoreConfig(storeId0, StoreConfig.RocksDB(Some(2048)), None)
    roundTrip(cfg) should be(cfg)

  test("yaml round trip with neither limit"):
    val cfg = StoreConfig(storeId0, StoreConfig.RocksDB(None), None)
    cfg.yamlConfig should not include "max-slice-size"
    roundTrip(cfg) should be(cfg)

  test("yaml round trip of an FSIF backend"):
    val cfg = StoreConfig(storeId0, StoreConfig.Fsif(4097, 1000), Some(8192))
    cfg.yamlConfig should include("storage-engine: fsif")
    cfg.yamlConfig should include("segment-payload-size: 4097")
    cfg.yamlConfig should include("segment-count: 1000")
    roundTrip(cfg) should be(cfg)
    cfg.effectiveMaxSliceSize should be(Some(4097))
    StoreConfig(storeId0, StoreConfig.Fsif(4097, 1000), None).effectiveMaxSliceSize should be(Some(4097))
    roundTrip(StoreConfig(storeId0, StoreConfig.Fsif(4097, 1000), None)) should be(
      StoreConfig(storeId0, StoreConfig.Fsif(4097, 1000), None))

  test("an FSIF backend needs a positive segment payload size and count"):
    def load(backend: String): StoreConfig =
      val f = os.temp(prefix = "aspen-store-fsif", suffix = ".yaml")
      try
        os.write.over(f,
          s"""store-id: 00000000-0000-0000-0000-000000000000:0
             |backend:
             |  storage-engine: fsif
             |$backend""".stripMargin)
        StoreConfig.loadStoreConfig(f)
      finally
        os.remove(f)

    load("  segment-payload-size: 10\n  segment-count: 2\n").backend should be(StoreConfig.Fsif(10, 2))
    a[YamlFormat.FormatError] should be thrownBy load("  segment-count: 2\n")
    a[YamlFormat.FormatError] should be thrownBy load("  segment-payload-size: 10\n")
    a[YamlFormat.FormatError] should be thrownBy load("  segment-payload-size: 0\n  segment-count: 2\n")
    a[YamlFormat.FormatError] should be thrownBy load("  segment-payload-size: 10\n  segment-count: 0\n")

  test("effectiveMaxSliceSize is the smaller, the only one, or None"):
    StoreConfig(storeId0, StoreConfig.RocksDB(Some(2048)), Some(4096)).effectiveMaxSliceSize should be(Some(2048))
    StoreConfig(storeId0, StoreConfig.RocksDB(Some(8192)), Some(4096)).effectiveMaxSliceSize should be(Some(4096))
    StoreConfig(storeId0, StoreConfig.RocksDB(None), Some(4096)).effectiveMaxSliceSize should be(Some(4096))
    StoreConfig(storeId0, StoreConfig.RocksDB(Some(2048)), None).effectiveMaxSliceSize should be(Some(2048))
    StoreConfig(storeId0, StoreConfig.RocksDB(None), None).effectiveMaxSliceSize should be(None)

  test("zero or negative slice limits are reported as a FormatError"):
    def load(backendLimit: String, poolLimit: String): StoreConfig =
      val f = os.temp(prefix = "aspen-store-limit", suffix = ".yaml")
      try
        os.write.over(f,
          s"""store-id: 00000000-0000-0000-0000-000000000000:0
             |backend:
             |  storage-engine: rocksdb$backendLimit$poolLimit
             |""".stripMargin)
        StoreConfig.loadStoreConfig(f)
      finally
        os.remove(f)

    List("0", "-5").foreach: v =>
      val e1 = the[YamlFormat.FormatError] thrownBy load(s"\n  max-slice-size: $v", "")
      e1.msg should include("backend.max-slice-size")
      val e2 = the[YamlFormat.FormatError] thrownBy load("", s"\npool-max-slice-size: $v")
      e2.msg should include("pool-max-slice-size")
    load("\n  max-slice-size: 1", "\npool-max-slice-size: 1").effectiveMaxSliceSize should be(Some(1))

  test("an empty config file is reported as a FormatError rather than an NPE"):
    val f = os.temp(prefix = "aspen-store-empty", suffix = ".yaml")
    try
      a[YamlFormat.FormatError] should be thrownBy StoreConfig.loadStoreConfig(f)
    finally
      os.remove(f)

  /** Open descriptors of this process that refer to `f`, from /proc/self/fd. Entries closing while being listed are
    * skipped.
    */
  private def fdsOpenOn(f: os.Path): Int =
    val target = os.Path(f.toNIO.toRealPath())
    os.list(os.root / "proc" / "self" / "fd").count: fd =>
      try os.readLink.absolute(fd) == target
      catch case _: Exception => false

  test("repeated loads do not leak file descriptors"):
    if os.isDir(os.root / "proc" / "self" / "fd") then
      // Linux: only descriptors on this test's file count, so other suites opening sockets and files in parallel
      // cannot disturb the result
      val f = os.temp(prefix = "aspen-store-fdcheck", suffix = ".yaml")
      try
        os.write.over(f, sample.yamlConfig)
        for _ <- 1 to 500 do StoreConfig.loadStoreConfig(f)
        fdsOpenOn(f) shouldBe 0
      finally
        os.remove(f)
    else
      fdCountCheck()

  private def fdCountCheck(): Unit =
    ManagementFactory.getOperatingSystemMXBean match
      case osBean: com.sun.management.UnixOperatingSystemMXBean =>
        val f = os.temp(prefix = "aspen-store-fdcheck", suffix = ".yaml")
        try
          os.write.over(f, sample.yamlConfig)

          // Prime any lazily-initialized state so it isn't counted as a leak
          StoreConfig.loadStoreConfig(f)

          val before = osBean.getOpenFileDescriptorCount
          for _ <- 1 to 500 do StoreConfig.loadStoreConfig(f)
          (osBean.getOpenFileDescriptorCount - before) should be < 50L
        finally
          os.remove(f)

      case _ =>
        cancel("open file descriptor count is unavailable on this platform")
