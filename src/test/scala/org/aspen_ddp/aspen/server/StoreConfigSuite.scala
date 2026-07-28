package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.YamlFormat
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.lang.management.ManagementFactory
import java.nio.charset.StandardCharsets
import java.nio.file.Files
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
    val f = Files.createTempFile("aspen-store-config", ".yaml")
    try
      Files.write(f, sample.yamlConfig.getBytes(StandardCharsets.UTF_8))
      StoreConfig.loadStoreConfig(f.toFile) should be(sample)
    finally
      Files.deleteIfExists(f)

  test("doc-comment example config parses correctly"):
    val docExampleYaml = """store-id: 00000000-0000-0000-0000-000000000000:0
backend:
  storage-engine: rocksdb
"""

    val f = Files.createTempFile("aspen-store-docexample", ".yaml")
    try
      Files.write(f, docExampleYaml.getBytes(StandardCharsets.UTF_8))
      val cfg = StoreConfig.loadStoreConfig(f.toFile)

      cfg.storeId should be(StoreId(PoolId(zero), 0.asInstanceOf[Byte]))
      cfg.backend should be(StoreConfig.RocksDB())

      StoreConfig(StoreId(PoolId(zero), 0.asInstanceOf[Byte]),
                  StoreConfig.RocksDB()).yamlConfig should be(docExampleYaml)
    finally
      Files.deleteIfExists(f)

  test("an empty config file is reported as a FormatError rather than an NPE"):
    val f = Files.createTempFile("aspen-store-empty", ".yaml")
    try
      a[YamlFormat.FormatError] should be thrownBy StoreConfig.loadStoreConfig(f.toFile)
    finally
      Files.deleteIfExists(f)

  test("repeated loads do not leak file descriptors"):
    ManagementFactory.getOperatingSystemMXBean match
      case os: com.sun.management.UnixOperatingSystemMXBean =>
        val f = Files.createTempFile("aspen-store-fdcheck", ".yaml")
        try
          Files.write(f, sample.yamlConfig.getBytes(StandardCharsets.UTF_8))

          // Prime any lazily-initialized state so it isn't counted as a leak
          StoreConfig.loadStoreConfig(f.toFile)

          val before = os.getOpenFileDescriptorCount
          for _ <- 1 to 500 do StoreConfig.loadStoreConfig(f.toFile)
          (os.getOpenFileDescriptorCount - before) should be < 50L
        finally
          Files.deleteIfExists(f)

      case _ =>
        cancel("open file descriptor count is unavailable on this platform")
