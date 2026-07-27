package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.common.metadata.HostId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.UUID

class HostConfigSuite extends AnyFunSuite with Matchers:

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
