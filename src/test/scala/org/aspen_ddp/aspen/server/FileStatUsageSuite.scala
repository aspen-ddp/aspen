package org.aspen_ddp.aspen.server

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Keeps every existence test under `server/` on FileStat: os-lib's `os.exists`, `os.isDir` and `os.isFile` answer
 *  false on a failed stat, which taken for an absence can drop state, load a tombstoned device or retire a finished
 *  copy (cross-document-review 2.2). Comments are skipped, so FileStat's own scaladoc may name them. */
class FileStatUsageSuite extends AnyFunSuite with Matchers:
  private val serverDir = os.pwd / "src" / "main" / "scala" / "org" / "aspen_ddp" / "aspen" / "server"
  private val forbidden = raw"\bos\.(exists|isDir|isFile)\b".r

  test("no os-lib existence test under server/ outside FileStat"):
    os.isDir(serverDir) should be(true)
    val uses =
      for
        file <- os.walk(serverDir).filter(_.ext == "scala").toList
        (line, n) <- os.read.lines(file).zipWithIndex
        trimmed = line.trim
        if !trimmed.startsWith("//") && !trimmed.startsWith("*") && !trimmed.startsWith("/*")
        code = line.indexOf("//") match
          case -1 => line
          case i => line.take(i)
        if forbidden.findFirstIn(code).isDefined
      yield s"${file.relativeTo(os.pwd)}:${n + 1}: $trimmed"
    uses shouldBe empty
