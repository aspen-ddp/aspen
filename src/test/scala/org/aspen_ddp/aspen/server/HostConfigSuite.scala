package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.common.metadata.HostId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.UUID

class HostConfigSuite extends AnyFunSuite with Matchers:

  private val zero = UUID.fromString("00000000-0000-0000-0000-000000000000")

  private val sample = HostConfig(
    HostId(UUID.fromString("11111111-1111-1111-1111-111111111111")),
    UUID.fromString("22222222-2222-2222-2222-222222222222"),
    "node_a",
    "127.0.0.1",
    4750,
    4751,
    4752,
    HostConfig.SimpleCRL(numStreams = 3, fileSizeMb = 300))

  test("configFilename is aspen-host-config.yaml"):
    HostConfig.configFilename should be("aspen-host-config.yaml")

  test("yamlConfig uses the host-id key and a bare UUID"):
    sample.yamlConfig should include("host-id: 11111111-1111-1111-1111-111111111111")
    sample.yamlConfig should not include "hostState"

  test("loadHostConfig round-trips yamlConfig"):
    val f = Files.createTempFile("aspen-host-config", ".yaml")
    try
      Files.write(f, sample.yamlConfig.getBytes(StandardCharsets.UTF_8))
      HostConfig.loadHostConfig(f.toFile) should be(sample)
    finally
      Files.deleteIfExists(f)

  test("doc-comment example config parses correctly"):
    val docExampleYaml = """host-id: 00000000-0000-0000-0000-000000000000
aspen-system-id: 00000000-0000-0000-0000-000000000000
name: bootstrap-host
address: 127.0.0.1
data-port: 4750
cnc-port: 4751
store-transfer-port: 4752
crl:
  storage-engine: simple-crl
  num-streams: 3
  max-file-size-mb: 300
"""

    val f = Files.createTempFile("aspen-host-docexample", ".yaml")
    try
      Files.write(f, docExampleYaml.getBytes(StandardCharsets.UTF_8))
      val cfg = HostConfig.loadHostConfig(f.toFile)

      cfg.hostId should be(HostId(zero))
      cfg.aspenSystemId should be(zero)
      cfg.name should be("bootstrap-host")
      cfg.address should be("127.0.0.1")
      cfg.dataPort should be(4750)
      cfg.cncPort should be(4751)
      cfg.storeTransferPort should be(4752)
      cfg.crl should be(HostConfig.SimpleCRL(numStreams = 3, fileSizeMb = 300))

      cfg.yamlConfig should be(docExampleYaml)
    finally
      Files.deleteIfExists(f)
