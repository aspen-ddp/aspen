package org.aspen_ddp.aspen.server

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DurableFilesSuite extends AnyFunSuite with Matchers:

  private def withDir(f: os.Path => Unit): Unit =
    val dir = os.temp.dir(prefix = "aspen-durable", deleteOnExit = false)
    try f(dir) finally os.remove.all(dir)

  test("write replaces the file and leaves no temp file"):
    withDir: dir =>
      val file = dir / "marker"
      DurableFiles.write(file, "old")
      DurableFiles.write(file, "new")
      os.read(file) should be("new")
      os.list(dir).map(_.last) should be(Seq("marker"))

  test("a failing fill removes the temp file and keeps the old content"):
    withDir: dir =>
      val file = dir / "marker"
      DurableFiles.write(file, "old")
      val ex = intercept[RuntimeException]:
        DurableFiles.writeWith(file): out =>
          out.write("partial".getBytes)
          throw new RuntimeException("boom")
      ex.getMessage should be("boom")
      os.read(file) should be("old")
      os.list(dir).map(_.last) should be(Seq("marker"))

  test("a failing move removes the temp file"):
    withDir: dir =>
      // A non-empty directory at the target makes the rename fail.
      val file = dir / "marker"
      os.makeDir(file)
      os.write(file / "child", "x")
      an[Exception] should be thrownBy DurableFiles.write(file, "new")
      os.list(dir).map(_.last) should be(Seq("marker"))
