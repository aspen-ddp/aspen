package org.aspen_ddp.aspen.server.store.backend.fsif

import java.util.UUID

import scala.util.Random

import org.scalatest.funsuite.AnyFunSuite

import org.aspen_ddp.aspen.common.objects.ObjectId

class DeviceJournalScanSuite extends AnyFunSuite:
  private val jid = new UUID(11, 22)
  private val sizing = JournalSizing(jid, 3, 64 * 1024)
  private def salt(file: Int): Long = 1000L + file

  private def batch(serial: Long): Batch =
    Batch(serial, Vector(TaggedRecord(new UUID(1, serial), JournalRecord.Mapping(serial.toInt, serial.toInt, ObjectId(new UUID(serial, 7))))))

  private def newFiles() = new SimulatedFsifFiles(new Random(1))

  private def put(files: SimulatedFsifFiles, name: String, pos: Long, bytes: Array[Byte]): Unit =
    val f = files.open(name)
    f.write(pos, bytes)
    f.force()
    f.close()

  /** Each plan entry: file index, header first, header oldestNeeded, the file's batches written back to back */
  private def writeRing(files: SimulatedFsifFiles, sz: JournalSizing, plan: Seq[(Int, Long, Long, Seq[Batch])],
                        saltOf: Int => Long = salt): Unit =
    plan.foreach: (idx, first, oldest, batches) =>
      val name = DeviceJournalScan.fileName(idx)
      put(files, name, 0, DeviceJournalFormat.encodeHeader(FileHeader(sz.journalId, saltOf(idx), first, oldest)))
      var pos = DeviceJournalFormat.HeaderSize.toLong
      batches.foreach: b =>
        val enc = DeviceJournalFormat.encodeBatch(sz.journalId, saltOf(idx), b)
        put(files, name, pos, enc)
        pos += enc.length

  private def serials(s: Scan): Vector[Long] = s match
    case Scan.Valid(bs, _) => bs.map(_.serial)
    case other => fail(s"expected Valid, got $other")

  private def batchLen(serial: Long): Int = DeviceJournalFormat.batchSize(batch(serial).records)

  test("empty ring is Empty"):
    assert(DeviceJournalScan.scan(newFiles(), sizing) == Scan.Empty)

  test("one file, three batches, Valid with tail after the third"):
    val files = newFiles()
    writeRing(files, sizing, Seq((0, 5L, 5L, Seq(batch(5), batch(6), batch(7)))))
    val s = DeviceJournalScan.scan(files, sizing)
    assert(serials(s) == Vector(5L, 6L, 7L))
    val Scan.Valid(_, tail) = s: @unchecked
    assert(tail == Tail(0, 4096L + 3 * batchLen(5), 8, salt(0)))

  test("header only: tail at HeaderSize with serial first"):
    val files = newFiles()
    writeRing(files, sizing, Seq((1, 9L, 9L, Seq.empty)))
    val Scan.Valid(bs, tail) = DeviceJournalScan.scan(files, sizing): @unchecked
    assert(bs.isEmpty)
    assert(tail == Tail(1, 4096, 9, salt(1)))

  test("clean switch: the batches end before the next file's first, Valid"):
    val files = newFiles()
    writeRing(files, sizing, Seq(
      (0, 1L, 1L, Seq(batch(1), batch(2))),
      (1, 3L, 2L, Seq(batch(3)))))
    val s = DeviceJournalScan.scan(files, sizing)
    assert(serials(s) == Vector(2L, 3L))
    val Scan.Valid(_, tail) = s: @unchecked
    assert(tail.file == 1 && tail.nextSerial == 4)

  test("torn last batch in the newest file is a torn tail"):
    val files = newFiles()
    writeRing(files, sizing, Seq((0, 1L, 1L, Seq(batch(1), batch(2)))))
    val pos = 4096L + batchLen(1)
    put(files, "0.fj", pos + 10, Array[Byte](0x55, 0x66))
    val s = DeviceJournalScan.scan(files, sizing)
    assert(serials(s) == Vector(1L))
    val Scan.Valid(_, tail) = s: @unchecked
    assert(tail == Tail(0, pos, 2, salt(0)))

  test("a rotted batch followed by a valid next batch is Corrupt"):
    val files = newFiles()
    writeRing(files, sizing, Seq((0, 1L, 1L, Seq(batch(1), batch(2), batch(3)))))
    put(files, "0.fj", 4096L + batchLen(1) + 10, Array[Byte](0x55, 0x66))
    assert(DeviceJournalScan.scan(files, sizing).isInstanceOf[Scan.Corrupt])

  test("a rotted batch below oldestNeeded is not read"):
    val files = newFiles()
    writeRing(files, sizing, Seq((0, 1L, 3L, Seq(batch(1), batch(2), batch(3), batch(4)))))
    put(files, "0.fj", 4096L + batchLen(1) + 10, Array[Byte](0x55, 0x66))
    val s = DeviceJournalScan.scan(files, sizing)
    assert(serials(s) == Vector(3L, 4L))
    val Scan.Valid(_, tail) = s: @unchecked
    assert(tail.nextSerial == 5)

  test("a rotted batch below oldestNeeded with oldestNeeded unreachable is Corrupt"):
    val files = newFiles()
    writeRing(files, sizing, Seq((0, 1L, 3L, Seq(batch(1), batch(2)))))
    put(files, "0.fj", 4096L + batchLen(1) + 10, Array[Byte](0x55, 0x66))
    assert(DeviceJournalScan.scan(files, sizing).isInstanceOf[Scan.Corrupt])

  test("a torn header rewrite of the file being entered is ignored"):
    val files = newFiles()
    writeRing(files, sizing, Seq((0, 1L, 1L, Seq(batch(1), batch(2)))))
    put(files, "1.fj", 0, new Array[Byte](4096))
    val s = DeviceJournalScan.scan(files, sizing)
    assert(serials(s) == Vector(1L, 2L))
    val Scan.Valid(_, tail) = s: @unchecked
    assert(tail.file == 0 && tail.nextSerial == 3)

  test("a rotted header of a file inside the needed range is Corrupt"):
    val files = newFiles()
    writeRing(files, sizing, Seq(
      (0, 1L, 1L, Seq(batch(1), batch(2))),
      (1, 3L, 1L, Seq(batch(3), batch(4))),
      (2, 5L, 1L, Seq(batch(5)))))
    put(files, "0.fj", 20, Array[Byte](0x7f))
    assert(DeviceJournalScan.scan(files, sizing).isInstanceOf[Scan.Corrupt])

  test("a rotted header of a file in the middle of the needed range is Corrupt"):
    val files = newFiles()
    writeRing(files, sizing, Seq(
      (0, 1L, 1L, Seq(batch(1), batch(2))),
      (1, 3L, 1L, Seq(batch(3), batch(4))),
      (2, 5L, 1L, Seq(batch(5)))))
    put(files, "1.fj", 20, Array[Byte](0x7f))
    assert(DeviceJournalScan.scan(files, sizing).isInstanceOf[Scan.Corrupt])

  test("a rotted header of a file wholly below oldestNeeded is ignored"):
    val files = newFiles()
    writeRing(files, sizing, Seq(
      (0, 1L, 1L, Seq(batch(1), batch(2))),
      (1, 3L, 3L, Seq(batch(3), batch(4))),
      (2, 5L, 4L, Seq(batch(5)))))
    put(files, "0.fj", 20, Array[Byte](0x7f))
    assert(serials(DeviceJournalScan.scan(files, sizing)) == Vector(4L, 5L))

  test("stale batches of an earlier pass (old salt) after the current pass's last batch end the file, not Corrupt"):
    val files = newFiles()
    writeRing(files, sizing, Seq((0, 1L, 1L, Seq(batch(1), batch(2), batch(3), batch(4)))), _ => 77L)
    writeRing(files, sizing, Seq((0, 10L, 10L, Seq(batch(10), batch(11)))))
    val s = DeviceJournalScan.scan(files, sizing)
    assert(serials(s) == Vector(10L, 11L))
    val Scan.Valid(_, tail) = s: @unchecked
    assert(tail == Tail(0, 4096L + 2 * batchLen(10), 12, salt(0)))

  test("an Unparseable batch in the needed range is Corrupt"):
    val files = newFiles()
    writeRing(files, sizing, Seq((0, 1L, 1L, Seq(batch(1)))))
    // a CRC-valid batch whose count claims a record it does not hold
    val enc = DeviceJournalFormat.encodeBatch(jid, salt(0), batch(2))
    val bb = java.nio.ByteBuffer.wrap(enc)
    bb.putInt(12, 5)
    val len = bb.getInt(0)
    val c = new java.util.zip.CRC32C
    val prefix = java.nio.ByteBuffer.allocate(24)
    prefix.putLong(jid.getMostSignificantBits).putLong(jid.getLeastSignificantBits).putLong(salt(0))
    c.update(prefix.array())
    c.update(enc, 0, 4 + len)
    bb.putInt(4 + len, c.getValue.toInt)
    put(files, "0.fj", 4096L + batchLen(1), enc)
    assert(DeviceJournalScan.scan(files, sizing).isInstanceOf[Scan.Corrupt])

  test("equal firsts: a header-only file is older than the file holding batches from that first"):
    // A crash left file 2 header-only at first 4; a writer that then entered file 0 at first 4 (the bug R22 fixed)
    val files = newFiles()
    writeRing(files, sizing, Seq(
      (2, 4L, 4L, Seq.empty),
      (0, 4L, 4L, Seq(batch(4), batch(5))),
      (1, 6L, 4L, Seq(batch(6)))))
    val s = DeviceJournalScan.scan(files, sizing)
    assert(serials(s) == Vector(4L, 5L, 6L))
    val Scan.Valid(_, tail) = s: @unchecked
    assert(tail.file == 1 && tail.nextSerial == 7)

  test("equal firsts: the newest header-only file does not hide the batches of its twin"):
    val files = newFiles()
    writeRing(files, sizing, Seq(
      (2, 4L, 4L, Seq.empty),
      (0, 4L, 4L, Seq(batch(4))),
      (1, 2L, 2L, Seq(batch(2), batch(3)))))
    val s = DeviceJournalScan.scan(files, sizing)
    assert(serials(s) == Vector(4L))
    val Scan.Valid(_, tail) = s: @unchecked
    assert(tail.file == 0 && tail.nextSerial == 5)

  test("equal firsts both holding a batch at that first are Corrupt"):
    val files = newFiles()
    writeRing(files, sizing, Seq(
      (0, 4L, 4L, Seq(batch(4))),
      (2, 4L, 4L, Seq(batch(4), batch(5)))))
    assert(DeviceJournalScan.scan(files, sizing).isInstanceOf[Scan.Corrupt])
