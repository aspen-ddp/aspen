package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

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
    val f = os.temp(prefix = "aspen-storage-device-config", suffix = ".yaml")
    try
      os.write.over(f, sample.yamlConfig)
      StorageDeviceConfig.loadStorageDeviceConfig(f) should be(sample)
    finally
      os.remove(f)

  test("doc-comment example config parses correctly"):
    val docExampleYaml = """storage-device-id: 00000000-0000-0000-0000-000000000000
aspen-system-id: 00000000-0000-0000-0000-000000000000
"""

    val f = os.temp(prefix = "aspen-storage-device-docexample", suffix = ".yaml")
    try
      os.write.over(f, docExampleYaml)
      val cfg = StorageDeviceConfig.loadStorageDeviceConfig(f)

      cfg.storageDeviceId should be(StorageDeviceId(zero))
      cfg.aspenSystemId should be(zero)

      StorageDeviceConfig(StorageDeviceId(zero), zero).yamlConfig should be(docExampleYaml)
    finally
      os.remove(f)
