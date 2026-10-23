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
    StoreConfig.RocksDB())

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
      cfg.backend should be(StoreConfig.RocksDB())

      StoreConfig(StoreId(PoolId(zero), 0.asInstanceOf[Byte]),
                  StoreConfig.RocksDB()).yamlConfig should be(docExampleYaml)
    finally
      os.remove(f)

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
