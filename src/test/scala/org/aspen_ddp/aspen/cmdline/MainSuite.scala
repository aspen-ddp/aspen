package org.aspen_ddp.aspen.cmdline

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.aspen_ddp.aspen.common.metadata.*
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.server.store.backend.RocksDBConfig

import java.util.UUID
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.{Duration, SECONDS}

class MainSuite extends AnyFunSuite with Matchers:

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
