package org.aspen_ddp.aspen.server

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.IOException

class FileStatSuite extends AnyFunSuite with Matchers:
  private def withDir(f: os.Path => Unit): Unit =
    val dir = os.temp.dir()
    try f(dir)
    finally
      os.walk(dir).filter(os.isDir(_)).foreach(os.perms.set(_, "rwx------"))
      os.remove.all(dir)

  test("present and absent files and directories are told apart"):
    withDir: dir =>
      os.write(dir / "f", "x")
      FileStat.exists(dir / "f") should be(true)
      FileStat.isFile(dir / "f") should be(true)
      FileStat.isDir(dir / "f") should be(false)
      FileStat.isDir(dir) should be(true)
      FileStat.exists(dir / "missing") should be(false)
      FileStat.existence(dir / "missing") should be(Some(false))

  test("a path under a regular file is absent"):
    withDir: dir =>
      os.write(dir / "f", "x")
      FileStat.exists(dir / "f" / "child") should be(false)

  test("a failed stat is no absence: it throws, where os.exists answers false (X-W18)"):
    withDir: dir =>
      val inner = dir / "store"
      os.makeDir(inner)
      os.write(inner / "marker", "")
      os.perms.set(inner, "rw-------")
      os.exists(inner / "marker") should be(false)
      an[IOException] should be thrownBy FileStat.exists(inner / "marker")
      an[IOException] should be thrownBy FileStat.isFile(inner / "marker")
      FileStat.existence(inner / "marker") should be(None)
