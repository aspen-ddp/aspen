package org.aspen_ddp.aspen.vdisk.tcmu

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable.ListBuffer

class ConfigfsSuite extends AnyFunSuite with Matchers:

  // Written from libtcmu's parser expectations (" Size: " then an unsigned integer): the kernel source,
  // target_core_user.c, is not available locally.
  private val infoText =
    "Status: ACTIVATED  Max Data Area: 8MB  Config: aspen/vd1  Size: 8388608  MaxDataAreaMB: 8  " +
      "DataPagesPerBlk: 1\n"

  // Note: the seeded files below model configfs, where every attribute file pre-exists.
  private class Tree:
    val root = os.temp.dir()
    val core = root / "core"
    val uio = root / "uio"
    val dev = root / "dev"
    os.makeDir.all(core)
    os.makeDir.all(uio)
    os.makeDir.all(dev)

    def devDir(hba: String = "30", device: String = "vd1"): os.Path = core / s"user_$hba" / device

    def configfsDev(hba: String = "30", device: String = "vd1"): os.Path =
      val d = devDir(hba, device)
      os.makeDir.all(d / "attrib")
      os.makeDir.all(d / "action")
      os.makeDir.all(d / "wwn")
      os.write(d / "attrib" / "hw_block_size", "512\n")
      os.write(d / "attrib" / "hw_max_sectors", "128\n")
      os.write(d / "info", infoText)
      os.write(d / "wwn" / "vpd_unit_serial", "T10 VPD Unit Serial Number: abc-123\n")
      d

    def uioDev(minor: Int, name: String, mapSize: String = "0x800000\n"): Unit =
      val d = uio / s"uio$minor"
      os.makeDir.all(d / "maps" / "map0")
      os.write(d / "name", name + "\n")
      os.write(d / "maps" / "map0" / "size", mapSize)

    def configfs = new Configfs(core, uio, dev)

    def recording(failOn: Set[String] = Set.empty, missing: Set[String] = Set.empty)
        : (Configfs, ListBuffer[(os.Path, String)]) =
      val log = ListBuffer.empty[(os.Path, String)]
      val c = new Configfs(core, uio, dev):
        override protected def writeFile(path: os.Path, value: String): Unit =
          log += ((path, value))
          if missing.contains(path.last) then
            throw new java.nio.file.NoSuchFileException(path.toString)
          if failOn.contains(path.last + " " + value) then
            throw new java.io.IOException("injected " + path.last)
      (c, log)

  test("reads block size, max sectors, info Size and unit serial"):
    val t = Tree()
    t.configfsDev()
    val c = t.configfs
    c.hwBlockSize("30", "vd1") should be (512)
    c.hwMaxSectors("30", "vd1") should be (128)
    c.size("30", "vd1") should be (8388608L)
    c.unitSerial("30", "vd1") should be (Some("abc-123"))

  test("size accepts Size as the last field of info"):
    val t = Tree()
    val d = t.configfsDev()
    os.write.over(d / "info", "Status: ACTIVATED  Config: x  Size: 4096\n")
    t.configfs.size("30", "vd1") should be (4096L)

  test("missing info Size names the path"):
    val t = Tree()
    val d = t.configfsDev()
    os.write.over(d / "info", "Status: ACTIVATED  Config: aspen/vd1\n")
    val e = intercept[Exception](t.configfs.size("30", "vd1"))
    e.getMessage should include ((d / "info").toString)
    e.getMessage should include ("Size")

  test("unreadable attribute names the path"):
    val t = Tree()
    val d = t.configfsDev()
    os.remove(d / "attrib" / "hw_block_size")
    val e = intercept[Exception](t.configfs.hwBlockSize("30", "vd1"))
    e.getMessage should include ((d / "attrib" / "hw_block_size").toString)

  test("non-numeric attribute names the path"):
    val t = Tree()
    val d = t.configfsDev()
    os.write.over(d / "attrib" / "hw_max_sectors", "zzz\n")
    val e = intercept[Exception](t.configfs.hwMaxSectors("30", "vd1"))
    e.getMessage should include ((d / "attrib" / "hw_max_sectors").toString)

  test("a unit serial containing ':' is kept whole"):
    val t = Tree()
    val d = t.configfsDev()
    os.write.over(d / "wwn" / "vpd_unit_serial", "T10 VPD Unit Serial Number: naa:6001:abc\n")
    t.configfs.unitSerial("30", "vd1") should be (Some("naa:6001:abc"))

  test("missing unit serial is None"):
    val t = Tree()
    val d = t.configfsDev()
    os.remove(d / "wwn" / "vpd_unit_serial")
    t.configfs.unitSerial("30", "vd1") should be (None)

  test("action writes the value to action/<name>"):
    val t = Tree()
    val d = t.configfsDev()
    os.write(d / "action" / "block_dev", "")
    t.configfs.action("30", "vd1", "block_dev", 1)
    os.read(d / "action" / "block_dev").trim should be ("1")

  test("resetRing writes block_dev, reset_ring, block_dev in order"):
    val t = Tree()
    val d = t.devDir() / "action"
    val (c, log) = t.recording()
    c.control("30", "vd1").resetRing()
    log.toList should be (List((d / "block_dev", "1"), (d / "reset_ring", "1"), (d / "block_dev", "0")))

  test("resetRing unblocks even when reset_ring fails, then reports the failure"):
    val t = Tree()
    val d = t.devDir() / "action"
    val (c, log) = t.recording(failOn = Set("reset_ring 1"))
    val e = intercept[java.io.IOException](c.control("30", "vd1").resetRing())
    e.getMessage should include ("reset_ring")
    log.toList should be (List((d / "block_dev", "1"), (d / "reset_ring", "1"), (d / "block_dev", "0")))

  test("resetRing is skipped when block_dev does not exist"):
    val t = Tree()
    val d = t.devDir() / "action"
    val (c, log) = t.recording(missing = Set("block_dev"))
    c.control("30", "vd1").resetRing()
    log.toList should be (List((d / "block_dev", "1")))

  test("resetRing against the real files leaves block_dev at 0"):
    val t = Tree()
    val d = t.configfsDev()
    os.write(d / "action" / "block_dev", "")
    os.write(d / "action" / "reset_ring", "")
    t.configfs.control("30", "vd1").resetRing()
    os.read(d / "action" / "block_dev").trim should be ("0")
    os.read(d / "action" / "reset_ring").trim should be ("1")

  test("uio name with slashes in cfgstring parses"):
    UioName.parse("tcm-user/30/vd1/aspen/my/disk") should be (Right(UioName("30", "vd1", "aspen/my/disk")))
    val n = UioName.parse("tcm-user/30/vd1/aspen/vd1")
    n should be (Right(UioName("30", "vd1", "aspen/vd1")))
    n.map(_.subtype) should be (Right("aspen"))

  test("uio name rejects malformed strings"):
    UioName.parse("tcm-user/30/vd1").isLeft should be (true)
    UioName.parse("tcm-user/30/vd1/aspen").isLeft should be (true)
    UioName.parse("tcm-user/30/vd1//vd1").isLeft should be (true)
    UioName.parse("other/30/vd1/aspen/x").isLeft should be (true)
    UioName.parse("").isLeft should be (true)

  test("uioName reads the sysfs name file"):
    val t = Tree()
    t.uioDev(3, "tcm-user/30/vd1/aspen/vd1")
    t.configfs.uioName(3) should be (Right(UioName("30", "vd1", "aspen/vd1")))

  test("uioName of a missing device is an error naming the path"):
    val t = Tree()
    val e = intercept[Exception](t.configfs.uioName(9))
    e.getMessage should include ((t.uio / "uio9" / "name").toString)

  test("mapSize parses hex"):
    val t = Tree()
    t.uioDev(0, "tcm-user/30/vd1/aspen/vd1", "0x1f00000\n")
    t.configfs.mapSize(0) should be (0x1f00000L)

  test("mapSize rejects non-hex content naming the path"):
    val t = Tree()
    t.uioDev(0, "tcm-user/30/vd1/aspen/vd1", "bogus\n")
    val e = intercept[Exception](t.configfs.mapSize(0))
    e.getMessage should include ((t.uio / "uio0" / "maps" / "map0" / "size").toString)

  test("uioDevicePath is /dev/uioN"):
    val t = Tree()
    t.configfs.uioDevicePath(4) should be (t.dev / "uio4")

  test("scanUio returns only tcm-user devices"):
    val t = Tree()
    t.uioDev(0, "tcm-user/30/vd1/aspen/vd1")
    t.uioDev(1, "someother-driver")
    t.uioDev(2, "tcm-user/31/vd2/aspen/vd2")
    os.makeDir.all(t.uio / "notuio")
    os.makeDir.all(t.uio / "uio7") // no name file
    t.configfs.scanUio().sortBy(_._1) should be (List(
      (0, UioName("30", "vd1", "aspen/vd1")),
      (2, UioName("31", "vd2", "aspen/vd2"))))

  test("scanUio skips a device whose name cannot be read"):
    val t = Tree()
    t.uioDev(0, "tcm-user/30/vd1/aspen/vd1")
    t.uioDev(1, "tcm-user/31/vd2/aspen/vd2")
    os.remove(t.uio / "uio1" / "name")
    os.makeDir(t.uio / "uio1" / "name") // a directory where the name file is expected
    t.configfs.scanUio() should be (List((0, UioName("30", "vd1", "aspen/vd1"))))

  test("the default writeFile to a missing attribute throws NoSuchFileException and creates nothing"):
    val t = Tree()
    val d = t.devDir() / "action"
    os.makeDir.all(d)
    intercept[java.nio.file.NoSuchFileException](t.configfs.action("30", "vd1", "block_dev", 1))
    os.exists(d / "block_dev") should be (false)
    // and resetRing therefore skips instead of failing
    t.configfs.control("30", "vd1").resetRing()
    os.list(d) should be (Nil)

  test("hardware attributes outside 1..Int.MaxValue are rejected naming path and value"):
    val t = Tree()
    val d = t.configfsDev()
    for bad <- List("0", "-4", "4294967296") do
      os.write.over(d / "attrib" / "hw_block_size", bad + "\n")
      val e = intercept[Exception](t.configfs.hwBlockSize("30", "vd1"))
      e.getMessage should include ((d / "attrib" / "hw_block_size").toString)
      e.getMessage should include (bad)
