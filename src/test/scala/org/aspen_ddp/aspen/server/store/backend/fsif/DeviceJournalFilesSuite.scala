package org.aspen_ddp.aspen.server.store.backend.fsif

import java.util.UUID

import org.scalatest.funsuite.AnyFunSuite

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.server.StorageDeviceConfig

class DeviceJournalFilesSuite extends AnyFunSuite:
  private val sizing = JournalSizing(new UUID(5, 6), 3, 8192)

  private def withDir(f: os.Path => Unit): Unit =
    val d = os.temp.dir(prefix = "fsif-journal-test")
    try f(d / "j")
    finally os.remove.all(d)

  private val cfg = StorageDeviceConfig(StorageDeviceId(new UUID(1, 1)), new UUID(2, 2))

  test("create writes count files of exactly fileSize zero bytes and a readable sizing"):
    withDir: dir =>
      assert(!DeviceJournalFiles.exists(dir))
      DeviceJournalFiles.create(dir, sizing)
      assert(DeviceJournalFiles.exists(dir))
      (0 until 3).foreach: n =>
        val b = os.read.bytes(dir / DeviceJournalScan.fileName(n))
        assert(b.length == 8192 && b.forall(_ == 0))
      assert(DeviceJournalFiles.readSizing(dir) == sizing)

  test("create fills files larger than one write chunk"):
    withDir: dir =>
      val big = JournalSizing(new UUID(5, 7), 3, (1L << 20) + 4096)
      DeviceJournalFiles.create(dir, big)
      assert(DeviceJournalFiles.readSizing(dir) == big)
      assert(os.read.bytes(dir / "2.fj").forall(_ == 0))

  test("readSizing refuses a missing file, an extra file and a short file"):
    withDir: dir =>
      DeviceJournalFiles.create(dir, sizing)
      os.remove(dir / "1.fj")
      intercept[JournalSizingMismatch](DeviceJournalFiles.readSizing(dir))
      os.write(dir / "1.fj", new Array[Byte](8192))
      DeviceJournalFiles.readSizing(dir)
      os.write(dir / "3.fj", new Array[Byte](8192))
      intercept[JournalSizingMismatch](DeviceJournalFiles.readSizing(dir))
      os.remove(dir / "3.fj")
      os.write.over(dir / "2.fj", new Array[Byte](4096))
      intercept[JournalSizingMismatch](DeviceJournalFiles.readSizing(dir))
      os.write.over(dir / "2.fj", new Array[Byte](8192))
      os.write.over(dir / "sizing", Array[Byte](1, 2, 3))
      intercept[JournalSizingMismatch](DeviceJournalFiles.readSizing(dir))
      os.remove(dir / "sizing")
      intercept[JournalSizingMismatch](DeviceJournalFiles.readSizing(dir))

  test("sizingFor uses the defaults and the config keys; refuses 2 files"):
    val d = DeviceJournalFiles.sizingFor(cfg)
    assert(d.files == 4 && d.fileSize == 256L << 20)
    assert(DeviceJournalFiles.sizingFor(cfg) != d) // fresh random id
    val s = DeviceJournalFiles.sizingFor(cfg.copy(fsifJournalFiles = Some(5), fsifJournalFileSizeMb = Some(2)))
    assert(s.files == 5 && s.fileSize == 2L << 20)
    intercept[JournalConfigInvalid](DeviceJournalFiles.sizingFor(cfg.copy(fsifJournalFiles = Some(2))))
    Seq(0, -1, 1025).foreach: mb =>
      intercept[JournalConfigInvalid](DeviceJournalFiles.sizingFor(cfg.copy(fsifJournalFileSizeMb = Some(mb))))
    assert(DeviceJournalFiles.sizingFor(cfg.copy(fsifJournalFileSizeMb = Some(1024))).fileSize == 1L << 30)

  test("names"):
    assert(DeviceJournalFiles.lostName(new UUID(0, 1)) == s"fsif-journal.lost-${new UUID(0, 1)}")
