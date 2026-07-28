package org.aspen_ddp.aspen.cmdline

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.aspen_ddp.aspen.common.metadata.*
import org.aspen_ddp.aspen.common.allocation_group.AllocationGroupId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.server.HostConfig
import org.aspen_ddp.aspen.server.store.backend.RocksDBConfig

import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, SimpleFileVisitor}
import java.util.UUID
import scala.collection.mutable
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.{Duration, SECONDS}

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
      Main.bootstrap(Replication(3, 2), tmp, 4750, 4751, 4752) shouldBe 0

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
      bsHost.address shouldBe hostCfg.address
      bsHost.dataPort shouldBe hostCfg.dataPort
      bsHost.cncPort shouldBe hostCfg.cncPort
      bsHost.storeTransferPort shouldBe hostCfg.storeTransferPort
      bsHost.stores shouldBe (0 until 3).map(i => StoreId(PoolId.BootstrapPoolId, i.toByte)).toList
