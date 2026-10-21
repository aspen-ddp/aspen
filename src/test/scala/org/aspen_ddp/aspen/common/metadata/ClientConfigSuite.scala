package org.aspen_ddp.aspen.common.metadata

import org.aspen_ddp.aspen.TestTLS
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.metadata.ClientConfig.{BootstrapEndpoint, CacheUpdate}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.YamlFormat.FormatError
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.file.attribute.PosixFilePermissions
import java.nio.file.{Files, Path}
import java.util.UUID

class ClientConfigSuite extends AnyFunSuite with Matchers:

  private val systemId = UUID.fromString("22222222-2222-2222-2222-222222222222")

  private val ep1 = BootstrapEndpoint("10.0.0.5", 4751)
  private val ep2 = BootstrapEndpoint("[fd00::5]", 4800)

  private val cfg = ClientConfig.Config(systemId, TestTLS.client, List(ep1, ep2))

  // generateBootstrapConfig does not quote host addresses, so the fetched configs below use a
  // host name rather than a bracketed IPv6 literal
  private val ep3 = BootstrapEndpoint("node-b", 4800)
  private val cached = ClientConfig.Config(systemId, TestTLS.client, List(ep1, ep3))

  /** A bootstrap config whose hosts expose the given (address, httpPort) pairs, in order */
  private def fetched(hosts: List[(String, Int)]): BootstrapConfig.Config =
    val poolId = PoolId(UUID.fromString("44444444-4444-4444-4444-444444444444"))
    val states = hosts.zipWithIndex.map: (h, i) =>
      HostState(HostId(new UUID(0, i + 1)), s"node_$i", h._1, 5000, h._2, 5002, Set())
    val storeMap = states.zipWithIndex.map: (hs, i) =>
      StoreId(poolId, i.toByte) -> hs.hostId
    BootstrapConfig.parseBootstrapConfig(
      BootstrapConfig.generateBootstrapConfig(
        systemId, Replication(hosts.length, 1), states, storeMap, TestTLS.caCertificatePem))

  private def tempFile(): Path =
    val dir = Files.createTempDirectory("client-config-suite")
    dir.resolve(ClientConfig.configFilename)

  test("generate/parse round-trips and keeps host order"):
    ClientConfig.parse(ClientConfig.generate(cfg)) shouldBe cfg

  test("parse rejects a certificate that is not PEM"):
    val yaml = ClientConfig.generate(cfg)
    val bad = yaml.replace("BEGIN CERTIFICATE", "BEGIN NOTHING").replace("END CERTIFICATE", "END NOTHING")
    intercept[FormatError]:
      ClientConfig.parse(bad)

  test("parse rejects an empty bootstrap-hosts list"):
    intercept[FormatError]:
      ClientConfig.Config(systemId, TestTLS.client, Nil)
    val yaml = ClientConfig.generate(cfg).split("\n").takeWhile(!_.startsWith("bootstrap-hosts:")).mkString("\n") +
      "\nbootstrap-hosts: []\n"
    intercept[FormatError]:
      ClientConfig.parse(yaml)

  test("BootstrapEndpoint.parse handles names, ports and bracketed IPv6"):
    BootstrapEndpoint.parse("node-a", 4751) shouldBe Right(BootstrapEndpoint("node-a", 4751))
    BootstrapEndpoint.parse("node-a:5000", 4751) shouldBe Right(BootstrapEndpoint("node-a", 5000))
    BootstrapEndpoint.parse("[fd00::5]:4800", 4751) shouldBe Right(BootstrapEndpoint("[fd00::5]", 4800))
    BootstrapEndpoint.parse("[fd00::5]", 4751) shouldBe Right(BootstrapEndpoint("[fd00::5]", 4751))

  test("BootstrapEndpoint.parse rejects bad ports and empty hosts"):
    List("node-a:abc", "node-a:0", "node-a:70000", "", ":4751", "[fd00::5", "[fd00::5]x", "a b").foreach: s =>
      val r = BootstrapEndpoint.parse(s, 4751)
      r.isLeft shouldBe true
      r.left.toOption.get should include(s)

  test("BootstrapEndpoint.parse rejects unbracketed IPv6 literals"):
    List("fd00::5", "::1").foreach: s =>
      val r = BootstrapEndpoint.parse(s, 4751)
      r.isLeft shouldBe true
      r.left.toOption.get should include("IPv6 literals must be bracketed")
      r.left.toOption.get should include(s)

  test("write creates an owner-only file"):
    val path = tempFile()
    ClientConfig.write(path, cfg)
    PosixFilePermissions.toString(Files.getPosixFilePermissions(path)) shouldBe "rw-------"
    ClientConfig.load(path) shouldBe cfg

  test("updateCachedHosts leaves the file alone when the host set matches in a different order"):
    val path = tempFile()
    ClientConfig.write(path, cached)
    val before = Files.readAllBytes(path)
    val f = fetched(List(("node-b", 4800), ("10.0.0.5", 4751)))
    ClientConfig.updateCachedHosts(path, cached, f) shouldBe CacheUpdate.Unchanged
    Files.readAllBytes(path) shouldBe before

  test("updateCachedHosts rewrites the file when the host set differs"):
    val path = tempFile()
    ClientConfig.write(path, cached)
    val f = fetched(List(("10.0.0.5", 4751), ("10.0.0.6", 4751), ("10.0.0.7", 4751)))
    ClientConfig.updateCachedHosts(path, cached, f) shouldBe CacheUpdate.Updated
    ClientConfig.load(path).bootstrapHosts shouldBe ClientConfig.endpointsOf(f)

  test("updateCachedHosts reports Failed rather than throwing when the file is not writable"):
    val path = tempFile()
    ClientConfig.write(path, cached)
    val dir = path.getParent
    val f = fetched(List(("10.0.0.6", 4751)))
    try
      Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("r-x------"))
      ClientConfig.updateCachedHosts(path, cached, f) match
        case CacheUpdate.Failed(_) =>
        case other => fail(s"expected Failed, got $other")
    finally
      Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("rwx------"))

  test("updateCachedHosts reports Failed and leaves a read-only client config untouched"):
    val path = tempFile()
    ClientConfig.write(path, cached)
    val before = Files.readAllBytes(path)
    val f = fetched(List(("10.0.0.6", 4751)))
    try
      Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("r--------"))
      ClientConfig.updateCachedHosts(path, cached, f) match
        case CacheUpdate.Failed(_) =>
        case other => fail(s"expected Failed, got $other")
      Files.readAllBytes(path) shouldBe before
    finally
      Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rw-------"))
