package org.aspen_ddp.aspen.common.metadata

import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.YamlFormat.FormatError
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.yaml.snakeyaml.error.YAMLException
import org.yaml.snakeyaml.parser.ParserException

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.UUID
import scala.util.control.NonFatal

class BootstrapConfigSuite extends AnyFunSuite with Matchers:

  private val systemId = UUID.fromString("22222222-2222-2222-2222-222222222222")
  private val hostId   = HostId(UUID.fromString("11111111-1111-1111-1111-111111111111"))
  private val poolId   = PoolId(UUID.fromString("44444444-4444-4444-4444-444444444444"))
  private val storeId  = StoreId(poolId, 0.toByte)

  private val hostState =
    HostState(hostId, "node_a", "127.0.0.1", 5000, 5001, 5002, Set())

  private def generated: String =
    BootstrapConfig.generateBootstrapConfig(
      systemId,
      Replication(1, 1),
      List(hostState),
      List(storeId -> hostId))

  private def assertMatchesGenerated(cfg: BootstrapConfig.Config): Unit =
    cfg.aspenSystemId should be(systemId)
    cfg.bootstrapIDA should be(Replication(1, 1))
    cfg.hosts.length should be(1)

    val h = cfg.hosts.head
    h.hostId should be(hostId)
    h.name should be("node_a")
    h.address should be("127.0.0.1")
    h.dataPort should be(5000)
    h.cncPort should be(5001)
    h.storeTransferPort should be(5002)
    h.stores should be(List(storeId))

  test("generateBootstrapConfig uses the bootstrap-hosts and host-id keys"):
    generated should include("bootstrap-hosts:")
    generated should include("- host-id: 11111111-1111-1111-1111-111111111111")
    generated should not include "hostState"

  test("loadBootstrapConfig round-trips generateBootstrapConfig"):
    val f = Files.createTempFile("aspen-bootstrap-config", ".yaml")
    try
      Files.write(f, generated.getBytes(StandardCharsets.UTF_8))
      val cfg = BootstrapConfig.loadBootstrapConfig(f.toFile)
      assertMatchesGenerated(cfg)
    finally
      Files.deleteIfExists(f)

  test("loadBootstrapConfig round-trips multi-host configuration"):
    val hostId1 = HostId(UUID.fromString("aaaa0001-0001-0001-0001-000000000001"))
    val hostId2 = HostId(UUID.fromString("aaaa0002-0002-0002-0002-000000000002"))
    val hostId3 = HostId(UUID.fromString("aaaa0003-0003-0003-0003-000000000003"))

    val multiHostPoolId = PoolId(UUID.fromString("bbbb0000-0000-0000-0000-000000000000"))
    val store0 = StoreId(multiHostPoolId, 0.toByte)
    val store1 = StoreId(multiHostPoolId, 1.toByte)
    val store2 = StoreId(multiHostPoolId, 2.toByte)
    val store3 = StoreId(multiHostPoolId, 3.toByte)

    val host1 = HostState(hostId1, "node_1", "10.0.0.1", 5000, 5001, 5002, Set())
    val host2 = HostState(hostId2, "node_2", "10.0.0.2", 5010, 5011, 5012, Set())
    val host3 = HostState(hostId3, "node_3", "10.0.0.3", 5020, 5021, 5022, Set())

    val storeMap = List(
      store0 -> hostId1,
      store1 -> hostId2,
      store2 -> hostId2,
      store3 -> hostId3
    )

    // Four stores across three hosts: the store count matches the width, the host count
    // deliberately does not.
    val yaml = BootstrapConfig.generateBootstrapConfig(
      systemId,
      Replication(4, 2),
      List(host1, host2, host3),
      storeMap)

    val f = Files.createTempFile("aspen-bootstrap-multi-host", ".yaml")
    try
      Files.write(f, yaml.getBytes(StandardCharsets.UTF_8))
      val cfg = BootstrapConfig.loadBootstrapConfig(f.toFile)

      cfg.aspenSystemId should be(systemId)
      cfg.bootstrapIDA should be(Replication(4, 2))
      cfg.hosts.length should be(3)

      val h1 = cfg.hosts(0)
      h1.hostId should be(hostId1)
      h1.name should be("node_1")
      h1.address should be("10.0.0.1")
      h1.dataPort should be(5000)
      h1.cncPort should be(5001)
      h1.storeTransferPort should be(5002)
      h1.stores should be(List(store0))

      val h2 = cfg.hosts(1)
      h2.hostId should be(hostId2)
      h2.name should be("node_2")
      h2.address should be("10.0.0.2")
      h2.dataPort should be(5010)
      h2.cncPort should be(5011)
      h2.storeTransferPort should be(5012)
      h2.stores should be(List(store1, store2))

      val h3 = cfg.hosts(2)
      h3.hostId should be(hostId3)
      h3.name should be("node_3")
      h3.address should be("10.0.0.3")
      h3.dataPort should be(5020)
      h3.cncPort should be(5021)
      h3.storeTransferPort should be(5022)
      h3.stores should be(List(store3))
    finally
      Files.deleteIfExists(f)

  test("doc-comment example config parses correctly"):
    val docExampleYaml = """aspen-system-id:  BF1049AD-D2A8-4D17-8080-E01A4678C8B3
bootstrap-ida:
  type: replication
  write-threshold: 1
  width: 1
bootstrap-hosts:
  - host-id: AA1049AD-D2A8-4D17-8080-E01A4678C8B3
    name: node_a
    address: 127.0.0.1
    data-port: 5000
    cnc-port: 5001
    store-transfer-port: 5002
    stores:
      - 00000000-0000-0000-0000-000000000000:0
"""

    val f = Files.createTempFile("aspen-bootstrap-docexample", ".yaml")
    try
      Files.write(f, docExampleYaml.getBytes(StandardCharsets.UTF_8))
      val cfg = BootstrapConfig.loadBootstrapConfig(f.toFile)

      cfg.aspenSystemId should be(UUID.fromString("BF1049AD-D2A8-4D17-8080-E01A4678C8B3"))
      cfg.bootstrapIDA should be(Replication(1, 1))
      cfg.hosts.length should be(1)

      val h = cfg.hosts.head
      h.hostId should be(HostId(UUID.fromString("AA1049AD-D2A8-4D17-8080-E01A4678C8B3")))
      h.name should be("node_a")
      h.address should be("127.0.0.1")
      h.dataPort should be(5000)
      h.cncPort should be(5001)
      h.storeTransferPort should be(5002)
      h.stores should be(List(StoreId(PoolId(UUID.fromString("00000000-0000-0000-0000-000000000000")), 0.toByte)))
    finally
      Files.deleteIfExists(f)

  test("loadBootstrapConfig accepts every bootstrap store on a single host"):
    // This is what "./t bootstrap <dir> replication 2 3 3" produces: one host, three stores.
    val singleHostYaml = """aspen-system-id: 22222222-2222-2222-2222-222222222222
bootstrap-ida:
  type: replication
  write-threshold: 2
  width: 3
bootstrap-hosts:
  - host-id: 11111111-1111-1111-1111-111111111111
    name: bootstrap-host
    address: 127.0.0.1
    data-port: 4750
    cnc-port: 4751
    store-transfer-port: 4752
    stores:
      - 00000000-0000-0000-0000-000000000000:0
      - 00000000-0000-0000-0000-000000000000:1
      - 00000000-0000-0000-0000-000000000000:2
"""

    val f = Files.createTempFile("aspen-bootstrap-single-host", ".yaml")
    try
      Files.write(f, singleHostYaml.getBytes(StandardCharsets.UTF_8))
      val cfg = BootstrapConfig.loadBootstrapConfig(f.toFile)

      cfg.hosts.length should be(1)
      cfg.hosts.head.stores.length should be(3)
    finally
      Files.deleteIfExists(f)

  test("loadBootstrapConfig rejects config with store count mismatch"):
    val mismatchYaml = """aspen-system-id: 22222222-2222-2222-2222-222222222222
bootstrap-ida:
  type: replication
  write-threshold: 2
  width: 3
bootstrap-hosts:
  - host-id: 11111111-1111-1111-1111-111111111111
    name: node_a
    address: 127.0.0.1
    data-port: 5000
    cnc-port: 5001
    store-transfer-port: 5002
    stores:
      - 44444444-4444-4444-4444-444444444444:0
  - host-id: 22222222-2222-2222-2222-222222222222
    name: node_b
    address: 127.0.0.2
    data-port: 5003
    cnc-port: 5004
    store-transfer-port: 5005
    stores:
      - 44444444-4444-4444-4444-444444444444:1
"""

    val f = Files.createTempFile("aspen-bootstrap-mismatch", ".yaml")
    try
      Files.write(f, mismatchYaml.getBytes(StandardCharsets.UTF_8))
      val ex = intercept[FormatError]:
        BootstrapConfig.loadBootstrapConfig(f.toFile)
      ex.getMessage should include("Number of bootstrap stores (2) must exactly match the Bootstrap IDA width (3)")
    finally
      Files.deleteIfExists(f)

  test("parseBootstrapConfig round-trips generateBootstrapConfig"):
    val cfg = BootstrapConfig.parseBootstrapConfig(generated)
    assertMatchesGenerated(cfg)

  test("parseBootstrapConfig rejects an empty document"):
    val ex = intercept[FormatError]:
      BootstrapConfig.parseBootstrapConfig("")
    ex.getMessage should include("Empty YAML document")

  test("parseBootstrapConfig rejects a document that is not a mapping"):
    // The shape a truncated or wrong-endpoint network response takes. It must surface as a
    // FormatError like every other bad config, not as a ClassCastException from the parse.
    val ex = intercept[FormatError]:
      BootstrapConfig.parseBootstrapConfig("<html><body>404</body></html>")
    ex.getMessage should include("Object Required")

  test("parseBootstrapConfig lets SnakeYAML's own parse failure out"):
    // Malformed YAML never reaches Config's validation at all: SnakeYAML throws first, and what
    // it throws is one of its YAMLException subclasses -- ParserException here, ScannerException
    // for a tab indent, ComposerException for an undefined alias. All three are NonFatal and
    // none is a FormatError, which is why a caller guarding this call has to catch NonFatal.
    // MetadataManager.refreshBootstrapConfig is that caller.
    val ex = intercept[ParserException]:
      BootstrapConfig.parseBootstrapConfig("bootstrap-hosts: [unclosed\n")
    ex shouldBe a[YAMLException]
    NonFatal(ex) should be(true)

  test("parseBootstrapConfig applies the store count validation"):
    // Config's own validation, reached through the string entry point rather than the file one.
    val mismatched = BootstrapConfig.generateBootstrapConfig(
      systemId,
      Replication(1, 1),
      List(hostState),
      List(storeId -> hostId, StoreId(poolId, 1.toByte) -> hostId))

    val ex = intercept[FormatError]:
      BootstrapConfig.parseBootstrapConfig(mismatched)
    ex.getMessage should include("Number of bootstrap stores (2) must exactly match")
