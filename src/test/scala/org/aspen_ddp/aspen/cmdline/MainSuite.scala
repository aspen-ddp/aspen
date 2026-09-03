package org.aspen_ddp.aspen.cmdline

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.aspen_ddp.aspen.common.metadata.*
import org.aspen_ddp.aspen.common.allocation_group.AllocationGroupId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.util.YamlFormat
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.server.HostConfig
import org.aspen_ddp.aspen.server.store.backend.RocksDBConfig
import org.yaml.snakeyaml.parser.ParserException

import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, SimpleFileVisitor}
import java.util.UUID
import java.util.concurrent.TimeoutException
import scala.collection.mutable
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.{DAYS, Duration, HOURS, MINUTES, SECONDS}

class MainSuite extends AnyFunSuite with Matchers:

  /** Temp trees created by the current test, removed by withTempDir. */
  private def withTempDir[T](prefix: String)(fn: Path => T): T =
    val dir = Files.createTempDirectory(prefix)
    try
      fn(dir)
    finally
      deleteTree(dir)

  private def deleteTree(root: Path): Unit =
    Files.walkFileTree(root, new SimpleFileVisitor[Path]:
      override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult =
        Files.deleteIfExists(file)
        FileVisitResult.CONTINUE

      override def postVisitDirectory(dir: Path, err: java.io.IOException): FileVisitResult =
        Files.deleteIfExists(dir)
        FileVisitResult.CONTINUE
    )

  test("formatBytes renders sub-KiB values as integer bytes"):
    Main.formatBytes(0L) shouldBe "0 B"
    Main.formatBytes(512L) shouldBe "512 B"
    Main.formatBytes(1023L) shouldBe "1023 B"

  test("formatBytes renders KiB with one decimal"):
    Main.formatBytes(1024L) shouldBe "1.0 KiB"
    Main.formatBytes(1536L) shouldBe "1.5 KiB"

  test("formatBytes selects the largest fitting binary unit"):
    // 1.5 * 1024^4
    Main.formatBytes(1649267441664L) shouldBe "1.5 TiB"
    // 10 * 1024^4
    Main.formatBytes(10995116277760L) shouldBe "10.0 TiB"

  test("resolveRef parses a UUID string and dispatches to byUuid"):
    val u = UUID.fromString("3f1ca5f7-74be-405d-aa48-739fc23651a4")
    var nameCalled = false
    val f = Main.resolveRef[String](
      u.toString,
      uuid => s"uuid:$uuid",
      _ => { nameCalled = true; Future.successful("name") })
    Await.result(f, Duration(1, SECONDS)) shouldBe s"uuid:$u"
    nameCalled shouldBe false

  test("resolveRef falls back to byName for a non-UUID string"):
    val f = Main.resolveRef[String](
      "mypool",
      uuid => s"uuid:$uuid",
      n => Future.successful(s"name:$n"))
    Await.result(f, Duration(1, SECONDS)) shouldBe "name:mypool"

  test("formatHostState renders identity, ports, and device list"):
    val hostId = HostId(UUID.fromString("11111111-1111-1111-1111-111111111111"))
    val devId  = StorageDeviceId(UUID.fromString("22222222-2222-2222-2222-222222222222"))
    val s = HostState(hostId, "node_a", "127.0.0.1", 4750, 4751, 4752, Set(devId))
    val out = Main.formatHostState(s)
    out should include ("Host: node_a")
    out should include ("11111111-1111-1111-1111-111111111111")
    out should include ("127.0.0.1")
    out should include ("4750")
    out should include ("22222222-2222-2222-2222-222222222222")

  test("formatHostState shows 'none' when there are no devices"):
    val hostId = HostId(UUID.fromString("11111111-1111-1111-1111-111111111111"))
    val s = HostState(hostId, "node_a", "127.0.0.1", 4750, 4751, 4752, Set.empty)
    Main.formatHostState(s) should include ("Storage Devices:     none")

  test("formatDeviceState renders identity, resolved names, usage, and stores"):
    val devId  = StorageDeviceId(UUID.fromString("22222222-2222-2222-2222-222222222222"))
    val hostId = HostId(UUID.fromString("11111111-1111-1111-1111-111111111111"))
    val setId  = StorageDeviceSetId(UUID.fromString("33333333-3333-3333-3333-333333333333"))
    val storeId = StoreId(PoolId(UUID.fromString("44444444-4444-4444-4444-444444444444")), 0.toByte)
    val stores = Map(storeId ->
      StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None))
    val s = StorageDeviceState(devId, hostId, 512L, 2048L, stores, setId)
    val out = Main.formatDeviceState(s, Some("node_a"), Some("fast-nvme"))
    out should include ("Storage Device: 22222222-2222-2222-2222-222222222222")
    out should include ("node_a")
    out should include ("fast-nvme")
    out should include ("Active")
    out should include (storeId.toString)

  test("formatDeviceState falls back to raw UUIDs when names are None"):
    val devId  = StorageDeviceId(UUID.fromString("22222222-2222-2222-2222-222222222222"))
    val hostId = HostId(UUID.fromString("11111111-1111-1111-1111-111111111111"))
    val setId  = StorageDeviceSetId(UUID.fromString("33333333-3333-3333-3333-333333333333"))
    val s = StorageDeviceState(devId, hostId, 0L, 0L, Map.empty, setId)
    val out = Main.formatDeviceState(s, None, None)
    out should include ("11111111-1111-1111-1111-111111111111")
    out should include ("33333333-3333-3333-3333-333333333333")
    out should include ("Stores:     none")

  test("formatPoolState renders identity, IDA, device set, and stores"):
    val poolId = PoolId(UUID.fromString("44444444-4444-4444-4444-444444444444"))
    val hostId = HostId(UUID.fromString("11111111-1111-1111-1111-111111111111"))
    val devId  = StorageDeviceId(UUID.fromString("22222222-2222-2222-2222-222222222222"))
    val setId  = StorageDeviceSetId(UUID.fromString("33333333-3333-3333-3333-333333333333"))
    val stores = Array(StoragePoolState.StoreEntry(hostId, devId))
    val s = StoragePoolState(poolId, "mypool", Replication(3, 2), None, stores,
      RocksDBConfig(), setId, currentUsage = 1024L, maximumStoreSize = 0L, allocationGroups = Nil)
    val out = Main.formatPoolState(s, Some("fast-nvme"))
    out should include ("Pool: mypool")
    out should include ("44444444-4444-4444-4444-444444444444")
    out should include ("Replication")
    out should include ("fast-nvme")
    out should include ("[0]")
    out should include ("22222222-2222-2222-2222-222222222222")

  test("formatPoolState falls back to set UUID and shows 'none' lists"):
    val poolId = PoolId(UUID.fromString("44444444-4444-4444-4444-444444444444"))
    val setId  = StorageDeviceSetId(UUID.fromString("33333333-3333-3333-3333-333333333333"))
    val s = StoragePoolState(poolId, "mypool", Replication(3, 2), None, Array.empty,
      RocksDBConfig(), setId)
    val out = Main.formatPoolState(s, None)
    out should include ("33333333-3333-3333-3333-333333333333")
    out should include ("Alloc Groups: none")
    out should include ("Stores:       none")

  test("formatDeviceSetState renders identity, parent name, and members"):
    val setId    = StorageDeviceSetId(UUID.fromString("33333333-3333-3333-3333-333333333333"))
    val parentId = StorageDeviceSetId(UUID.fromString("55555555-5555-5555-5555-555555555555"))
    val devId    = StorageDeviceId(UUID.fromString("22222222-2222-2222-2222-222222222222"))
    val s = StorageDeviceSetState(setId, "fast-nvme", 0, Some(parentId),
      List(devId), Nil, Nil)
    val out = Main.formatDeviceSetState(s, Some("root-set"))
    out should include ("Device Set: fast-nvme")
    out should include ("33333333-3333-3333-3333-333333333333")
    out should include ("Level:    0")
    out should include ("root-set")
    out should include ("22222222-2222-2222-2222-222222222222")

  test("formatDeviceSetState shows 'none' parent and empty member lists"):
    val setId = StorageDeviceSetId(UUID.fromString("33333333-3333-3333-3333-333333333333"))
    val s = StorageDeviceSetState(setId, "root-set", 1, None, Nil, Nil, Nil)
    val out = Main.formatDeviceSetState(s, None)
    out should include ("Parent:   none")
    out should include ("Member Devices: none")
    out should include ("Member Sets:    none")
    out should include ("Assigned Pools: none")
    out should include ("Pending Transfers: 0")

  test("formatAllocationGroupState renders identity, usage, and members"):
    val groupId = AllocationGroupId(UUID.fromString("66666666-6666-6666-6666-666666666666"))
    val member = AllocationGroupState.Member(
      AllocationGroupState.MemberType.Pool,
      UUID.fromString("44444444-4444-4444-4444-444444444444"),
      None, 1024L, 4096L)
    val s = AllocationGroupState(groupId, 0, "gold", List(member), Nil)
    val out = Main.formatAllocationGroupState(s)
    out should include ("Allocation Group: gold")
    out should include ("66666666-6666-6666-6666-666666666666")
    out should include ("Level: 0")
    out should include ("Pool")
    out should include ("44444444-4444-4444-4444-444444444444")

  test("formatAllocationGroupState shows 'none' for empty members and parents"):
    val groupId = AllocationGroupId(UUID.fromString("66666666-6666-6666-6666-666666666666"))
    val s = AllocationGroupState(groupId, 1, "platinum", Nil, Nil)
    val out = Main.formatAllocationGroupState(s)
    out should include ("Members: none")
    out should include ("Parent Groups: none")

  // The host command reads both files out of the host directory rather than taking the
  // bootstrap config on the command line, so bootstrap must leave both of them behind.
  test("bootstrap writes host and bootstrap config files into the host directory"):
    withTempDir("aspen-bootstrap"): tmp =>
      Main.bootstrap(Replication(3, 2), tmp, "10.0.0.5", 4750, 4751, 4752) shouldBe 0

      val hostDir = tmp.resolve("bootstrap-host")

      val hostCfg = HostConfig.loadHostConfig(
        hostDir.resolve(HostConfig.configFilename).toFile)

      val bsCfg = BootstrapConfig.loadBootstrapConfig(
        hostDir.resolve(BootstrapConfig.configFilename).toFile)

      bsCfg.aspenSystemId shouldBe hostCfg.aspenSystemId
      bsCfg.bootstrapIDA shouldBe Replication(3, 2)
      bsCfg.hosts.map(_.hostId) shouldBe List(hostCfg.hostId)

      val bsHost = bsCfg.hosts.head
      bsHost.name shouldBe hostCfg.name
      bsHost.dataPort shouldBe hostCfg.dataPort
      bsHost.cncPort shouldBe hostCfg.cncPort
      bsHost.storeTransferPort shouldBe hostCfg.storeTransferPort
      bsHost.stores shouldBe (0 until 3).map(i => StoreId(PoolId.BootstrapPoolId, i.toByte)).toList

      // The supplied address is written into both files from separate literals. Asserting the
      // value rather than just that the two agree is what catches only one of them being wired up.
      hostCfg.address shouldBe "10.0.0.5"
      bsHost.address shouldBe "10.0.0.5"

  test("validateHostAddress accepts IPv4, DNS names, and bracketed IPv6"):
    Main.validateHostAddress("10.0.0.5") shouldBe None
    Main.validateHostAddress("node-a.example.com") shouldBe None
    Main.validateHostAddress("[fd00::5]") shouldBe None
    Main.validateHostAddress("127.0.0.1") shouldBe None

  test("validateHostAddress rejects empty and whitespace-bearing addresses"):
    Main.validateHostAddress("") shouldBe Some("Address must not be empty")
    Main.validateHostAddress("   ") shouldBe Some("Address must not be empty")
    Main.validateHostAddress("node a") shouldBe Some("Address must not contain whitespace")

  test("validateHostAddress rejects a URI scheme"):
    Main.validateHostAddress("tcp://10.0.0.5") shouldBe
      Some("Address must not include a URI scheme; pass just the host (e.g. 10.0.0.5)")

  test("validateHostAddress rejects an embedded port"):
    Main.validateHostAddress("10.0.0.5:4750") shouldBe
      Some("Address must not include a port; use --data-port/--cnc-port/--store-transfer-port")

  test("validateHostAddress rejects an unbracketed IPv6 literal"):
    Main.validateHostAddress("fd00::5") shouldBe
      Some("IPv6 literals must be bracketed, e.g. [fd00::5]")

  test("isUnreachableAddress flags loopback and the bind wildcard"):
    Main.isUnreachableAddress("localhost") shouldBe true
    Main.isUnreachableAddress("LocalHost") shouldBe true
    Main.isUnreachableAddress("127.0.0.1") shouldBe true
    Main.isUnreachableAddress("127.1.2.3") shouldBe true
    Main.isUnreachableAddress("[::1]") shouldBe true
    Main.isUnreachableAddress("::1") shouldBe true
    Main.isUnreachableAddress("0.0.0.0") shouldBe true
    Main.isUnreachableAddress("[::]") shouldBe true

  test("isUnreachableAddress passes routable addresses"):
    Main.isUnreachableAddress("10.0.0.5") shouldBe false
    Main.isUnreachableAddress("node-a.example.com") shouldBe false
    Main.isUnreachableAddress("[fd00::5]") shouldBe false

  test("commandErrorMessage reports a config that parses but does not validate"):
    Main.commandErrorMessage(new YamlFormat.FormatError("Object Required")) should
      include("Error loading config file")

  test("commandErrorMessage reports a config that does not parse"):
    // Taken from a real SnakeYAML failure rather than a hand-built exception, so this breaks
    // if the hierarchy the clause matches on ever shifts. Malformed YAML raises none of the
    // four types the catch originally listed, which is what let it escape main entirely.
    val yamlEx = intercept[ParserException]:
      BootstrapConfig.parseBootstrapConfig("bootstrap-hosts: [unclosed\n")

    Main.commandErrorMessage(yamlEx) should include("Error parsing config file")

  test("commandErrorMessage reports the remaining expected command failures"):
    Main.commandErrorMessage(new Main.ConfigError("no such pool")) should include("no such pool")
    Main.commandErrorMessage(new TimeoutException()) should include("timed out")
    Main.commandErrorMessage(new IllegalArgumentException("bad width")) should include("bad width")

  test("commandErrorMessage leaves an unexpected exception unhandled"):
    // The clause set is the set of ways a user can get it wrong. Anything else is a bug and
    // must stay loud, so widening this to NonFatal is what this assertion is here to catch.
    Main.commandErrorMessage.isDefinedAt(new RuntimeException("boom")) shouldBe false
    Main.commandErrorMessage.isDefinedAt(new NullPointerException()) shouldBe false

  test("formatPoolState renders migration status when a migration is recorded"):
    val poolId = PoolId(UUID.fromString("44444444-4444-4444-4444-444444444444"))
    val setId  = StorageDeviceSetId(UUID.fromString("33333333-3333-3333-3333-333333333333"))
    val target = StorageDeviceSetId(UUID.fromString("66666666-6666-6666-6666-666666666666"))
    val s = StoragePoolState(poolId, "mypool", Replication(3, 2), None, Array.empty,
      RocksDBConfig(), setId,
      migration = Some(StoragePoolState.Migration(
        target, StoragePoolState.MigrationStatus.InProgress)))
    val out = Main.formatPoolState(s, Some("fast-nvme"))
    out should include ("Migration:")
    out should include ("InProgress")
    out should include ("66666666-6666-6666-6666-666666666666")

  test("formatPoolState omits the migration line when there is no migration"):
    val poolId = PoolId(UUID.fromString("44444444-4444-4444-4444-444444444444"))
    val setId  = StorageDeviceSetId(UUID.fromString("33333333-3333-3333-3333-333333333333"))
    val s = StoragePoolState(poolId, "mypool", Replication(3, 2), None, Array.empty,
      RocksDBConfig(), setId)
    Main.formatPoolState(s, None) should not include "Migration:"

  test("parseRebalancePeriod returns the display form when no period is given"):
    Main.parseRebalancePeriod(None, None) shouldBe Right(None)

  test("parseRebalancePeriod accepts each unit in singular and plural form"):
    Main.parseRebalancePeriod(Some("20"), Some("minutes")) shouldBe Right(Some(Duration(20, MINUTES)))
    Main.parseRebalancePeriod(Some("1"), Some("minute")) shouldBe Right(Some(Duration(1, MINUTES)))
    Main.parseRebalancePeriod(Some("4"), Some("hours")) shouldBe Right(Some(Duration(4, HOURS)))
    Main.parseRebalancePeriod(Some("1"), Some("hour")) shouldBe Right(Some(Duration(1, HOURS)))
    Main.parseRebalancePeriod(Some("7"), Some("days")) shouldBe Right(Some(Duration(7, DAYS)))
    Main.parseRebalancePeriod(Some("1"), Some("day")) shouldBe Right(Some(Duration(1, DAYS)))

  test("parseRebalancePeriod is case-insensitive about units"):
    Main.parseRebalancePeriod(Some("4"), Some("HOURS")) shouldBe Right(Some(Duration(4, HOURS)))

  test("parseRebalancePeriod maps 'disabled' to a zero period"):
    Main.parseRebalancePeriod(Some("disabled"), None) shouldBe Right(Some(Duration.Zero))

  test("parseRebalancePeriod accepts an explicit zero"):
    Main.parseRebalancePeriod(Some("0"), Some("hours")) shouldBe Right(Some(Duration.Zero))

  test("parseRebalancePeriod rejects 'disabled' with a unit"):
    Main.parseRebalancePeriod(Some("disabled"), Some("hours")) shouldBe Left("'disabled' takes no unit")

  test("parseRebalancePeriod rejects a count with no unit"):
    Main.parseRebalancePeriod(Some("4"), None) shouldBe
      Left("a unit is required: minutes, hours, or days")

  test("parseRebalancePeriod rejects an unknown unit"):
    Main.parseRebalancePeriod(Some("4"), Some("weeks")) shouldBe
      Left("unknown unit 'weeks': expected minutes, hours, or days")

  test("parseRebalancePeriod rejects a non-numeric count"):
    Main.parseRebalancePeriod(Some("soon"), Some("hours")) shouldBe
      Left("period must be a whole number")

  test("parseRebalancePeriod rejects a negative count"):
    Main.parseRebalancePeriod(Some("-1"), Some("hours")) shouldBe Left("period must not be negative")

  test("parseRebalancePeriod rejects a count that exceeds Duration's range"):
    Main.parseRebalancePeriod(Some("999999999999999"), Some("days")) shouldBe
      Left("period is too large (maximum ~292 years)")

  test("formatRebalancePeriod renders zero as disabled"):
    Main.formatRebalancePeriod(Duration.Zero) shouldBe "disabled"

  test("formatRebalancePeriod selects the largest unit that divides evenly"):
    Main.formatRebalancePeriod(Duration(20, MINUTES)) shouldBe "20 minutes"
    Main.formatRebalancePeriod(Duration(240, MINUTES)) shouldBe "4 hours"
    Main.formatRebalancePeriod(Duration(7, DAYS)) shouldBe "7 days"
    Main.formatRebalancePeriod(Duration(90, MINUTES)) shouldBe "90 minutes"

  test("formatRebalancePeriod uses singular units for one"):
    Main.formatRebalancePeriod(Duration(1, MINUTES)) shouldBe "1 minute"
    Main.formatRebalancePeriod(Duration(1, HOURS)) shouldBe "1 hour"
    Main.formatRebalancePeriod(Duration(1, DAYS)) shouldBe "1 day"

  test("formatRebalanceStatus reports 'never' and the next poll before the first sweep"):
    val out = Main.formatRebalanceStatus(Duration(8, HOURS), HLCTimestamp.Zero)
    out should include("Automatic rebalancing period: 8 hours")
    out should include("Last sweep:                   never")
    out should include("Next sweep due:               next poll")

  test("formatRebalanceStatus omits the next sweep when disabled"):
    val out = Main.formatRebalanceStatus(Duration.Zero, HLCTimestamp.Zero)
    out should include("Automatic rebalancing period: disabled")
    out should not include "Next sweep due"

  test("formatRebalanceStatus reports both times once a sweep has run"):
    val out = Main.formatRebalanceStatus(Duration(8, HOURS), HLCTimestamp.now)
    out should include("Last sweep:")
    out should include("Next sweep due:")
    out should not include "never"

  test("formatWallTime renders a fixed epoch millisecond value"):
    // Asserted on shape rather than an exact string: the helper renders in the local zone.
    Main.formatWallTime(1_756_742_400_000L) should fullyMatch regex
      """\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}"""

