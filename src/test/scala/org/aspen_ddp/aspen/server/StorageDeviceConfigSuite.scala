package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.UUID

class StorageDeviceConfigSuite extends AnyFunSuite with Matchers:

  private val zero = UUID.fromString("00000000-0000-0000-0000-000000000000")

  private val sample = StorageDeviceConfig(
    StorageDeviceId(UUID.fromString("33333333-3333-3333-3333-333333333333")),
    UUID.fromString("22222222-2222-2222-2222-222222222222"))

  test("configFilename is aspen-storage-device-config.yaml"):
    StorageDeviceConfig.configFilename should be("aspen-storage-device-config.yaml")

  test("yamlConfig emits a bare device UUID"):
    sample.yamlConfig should include(
      "storage-device-id: 33333333-3333-3333-3333-333333333333")
    sample.yamlConfig should not include "StorageDeviceId("

  test("loadStorageDeviceConfig round-trips yamlConfig"):
    val f = Files.createTempFile("aspen-storage-device-config", ".yaml")
    try
      Files.write(f, sample.yamlConfig.getBytes(StandardCharsets.UTF_8))
      StorageDeviceConfig.loadStorageDeviceConfig(f.toFile) should be(sample)
    finally
      Files.deleteIfExists(f)

  test("doc-comment example config parses correctly"):
    val docExampleYaml = """storage-device-id: 00000000-0000-0000-0000-000000000000
aspen-system-id: 00000000-0000-0000-0000-000000000000
"""

    val f = Files.createTempFile("aspen-storage-device-docexample", ".yaml")
    try
      Files.write(f, docExampleYaml.getBytes(StandardCharsets.UTF_8))
      val cfg = StorageDeviceConfig.loadStorageDeviceConfig(f.toFile)

      cfg.storageDeviceId should be(StorageDeviceId(zero))
      cfg.aspenSystemId should be(zero)

      StorageDeviceConfig(StorageDeviceId(zero), zero).yamlConfig should be(docExampleYaml)
    finally
      Files.deleteIfExists(f)
