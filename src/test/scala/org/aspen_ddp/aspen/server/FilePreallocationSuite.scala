package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.server.FilePreallocation.Mode
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

object FilePreallocationSuite:
  /** The bytes of disk `f` occupies (Linux `stat`): less than its size for a sparse file */
  def allocatedBytes(f: os.Path): Long =
    val Array(blocks, blockSize) = os.proc("stat", "-c", "%b %B", f).call().out.trim().split(" ")
    blocks.toLong * blockSize.toLong

class FilePreallocationSuite extends AnyFunSuite with Matchers:
  import FilePreallocationSuite.allocatedBytes

  private val Size = 3 * 1024 * 1024L + 512

  /** A temp dir (tmpfs here), and one under target/ (the build's file system: ext4 on the development hosts) */
  private def withDirs(f: os.Path => Unit): Unit =
    val tmp = os.temp.dir(prefix = "aspen-prealloc", deleteOnExit = false)
    val target = os.pwd / "target" / s"aspen-prealloc-${java.util.UUID.randomUUID()}"
    os.makeDir.all(target)
    try
      f(tmp)
      f(target)
    finally
      os.remove.all(tmp)
      os.remove.all(target)

  private def assertPreallocated(f: os.Path): Unit =
    os.size(f) should be(Size)
    allocatedBytes(f) should be >= Size
    os.read.bytes(f).forall(_ == 0) should be(true)

  for mode <- List(Mode.Fallocate, Mode.ZeroFill, Mode.Auto) do
    test(s"$mode creates a fully allocated file of zeros of the exact size"):
      withDirs: dir =>
        val f = dir / s"$mode.crl"
        FilePreallocation.preallocate(f, Size, mode)
        assertPreallocated(f)

  test("fallocate is callable on Linux, the supported host platform"):
    assume(System.getProperty("os.name").toLowerCase.startsWith("linux"))
    FilePreallocation.fallocateUnavailable should be(None)

  test("A zero-length file is created"):
    withDirs: dir =>
      FilePreallocation.preallocate(dir / "empty", 0, Mode.Fallocate)
      os.size(dir / "empty") should be(0)

  test("An existing file is refused and left as it was"):
    withDirs: dir =>
      val f = dir / "existing"
      os.write(f, "keep me")
      for mode <- List(Mode.Fallocate, Mode.ZeroFill, Mode.Auto) do
        assertThrows[java.nio.file.FileAlreadyExistsException](FilePreallocation.preallocate(f, Size, mode))
        os.read(f) should be("keep me")

  test("A failed fallocate removes its partial file"):
    withDirs: dir =>
      // Larger than any test file system can hold: ENOSPC or EFBIG
      val f = dir / "huge"
      assertThrows[java.io.IOException](FilePreallocation.preallocate(f, Long.MaxValue / 2, Mode.Fallocate))
      os.exists(f) should be(false)
