package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.TestTLS
import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.network.tls.{CertificateVerifier, Pem}
import org.aspen_ddp.aspen.common.util.YamlFormat.FormatError
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.UUID

class HostConfigSuite extends AnyFunSuite with Matchers:

  private val zero = UUID.fromString("00000000-0000-0000-0000-000000000000")

  private val hostUuid = UUID.fromString("11111111-1111-1111-1111-111111111111")
  private val (sampleCert, sampleKey) = TestTLS.credentials(hostUuid)

  private val sample = HostConfig(
    HostId(hostUuid),
    UUID.fromString("22222222-2222-2222-2222-222222222222"),
    "node_a",
    "127.0.0.1",
    4750,
    4751,
    4752,
    HostConfig.SimpleCRL(numStreams = 3, fileSizeMb = 300),
    sampleCert,
    sampleKey)

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

  test("round trip preserves the TLS certificate and private key exactly"):
    val f = Files.createTempFile("aspen-host-config-tls", ".yaml")
    try
      Files.write(f, sample.yamlConfig.getBytes(StandardCharsets.UTF_8))
      val loaded = HostConfig.loadHostConfig(f.toFile)
      loaded.tlsCertificate shouldBe sample.tlsCertificate
      loaded.tlsPrivateKey shouldBe sample.tlsPrivateKey
      CertificateVerifier.verify(Pem.decodeCertificate(loaded.tlsCertificate), TestTLS.ca.certificate) shouldBe sample.hostId.uuid
    finally
      Files.deleteIfExists(f)

  test("loadHostConfig rejects a config without tls-certificate"):
    val yaml = sample.yamlConfig
    val start = yaml.indexOf("tls-certificate: |")
    val end = yaml.indexOf("tls-private-key:")
    val f = Files.createTempFile("aspen-host-config-notls", ".yaml")
    try
      Files.write(f, (yaml.substring(0, start) + yaml.substring(end)).getBytes(StandardCharsets.UTF_8))
      a [FormatError] should be thrownBy HostConfig.loadHostConfig(f.toFile)
    finally
      Files.deleteIfExists(f)

  test("doc-comment example config parses correctly"):
    val docExampleYaml = """host-id: 00000000-0000-0000-0000-000000000000
aspen-system-id: 00000000-0000-0000-0000-000000000000
name: bootstrap-host
address: 127.0.0.1
data-port: 4750
http-port: 4751
store-transfer-port: 4752
crl:
  storage-engine: simple-crl
  num-streams: 3
  max-file-size-mb: 300
tls-certificate: |
  -----BEGIN CERTIFICATE-----
  AAAA
  -----END CERTIFICATE-----
tls-private-key: |
  -----BEGIN PRIVATE KEY-----
  BBBB
  -----END PRIVATE KEY-----
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
      cfg.httpPort should be(4751)
      cfg.storeTransferPort should be(4752)
      cfg.crl should be(HostConfig.SimpleCRL(numStreams = 3, fileSizeMb = 300))

      cfg.tlsCertificate should be("-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n")
      cfg.tlsPrivateKey should be("-----BEGIN PRIVATE KEY-----\nBBBB\n-----END PRIVATE KEY-----\n")

      cfg.yamlConfig should be(docExampleYaml)
    finally
      Files.deleteIfExists(f)

  test("rocksdb resource limits round-trip, including partially specified ones"):
    val full = sample.copy(rocksdb = HostConfig.RocksDBResources(Some(4096), Some(1024), Some(8)))
    val partial = sample.copy(rocksdb = HostConfig.RocksDBResources(maxBackgroundThreads = Some(6)))
    for cfg <- List(full, partial) do
      val f = Files.createTempFile("aspen-host-config-rocksdb", ".yaml")
      try
        Files.write(f, cfg.yamlConfig.getBytes(StandardCharsets.UTF_8))
        HostConfig.loadHostConfig(f.toFile) should be(cfg)
      finally
        Files.deleteIfExists(f)

  test("rocksdb section is omitted when no limits are set"):
    sample.yamlConfig should not include "rocksdb:"

  test("rocksdb resource limits convert megabytes to bytes and default the rest"):
    val rc = HostConfig.RocksDBResources(memoryMb = Some(4096), maxBackgroundThreads = Some(8)).resourceConfig
    rc.memoryBytes should be(4096L * 1024 * 1024)
    rc.writeBufferBytes should be(1024L * 1024 * 1024)
    rc.maxBackgroundThreads should be(8)

  test("object cache size round-trips and converts megabytes to bytes"):
    val cfg = sample.copy(objectCacheMb = Some(2048))
    cfg.objectCacheBytes should be(2048L * 1024 * 1024)
    val f = Files.createTempFile("aspen-host-config-objcache", ".yaml")
    try
      Files.write(f, cfg.yamlConfig.getBytes(StandardCharsets.UTF_8))
      HostConfig.loadHostConfig(f.toFile) should be(cfg)
    finally
      Files.deleteIfExists(f)

  test("object cache defaults to a quarter of physical RAM and is omitted from the yaml"):
    sample.yamlConfig should not include "object-cache-mb"
    sample.objectCacheBytes should be((org.aspen_ddp.aspen.common.util.physicalMemory() * 0.25).toLong)
