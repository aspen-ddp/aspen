package org.aspen_ddp.aspen.vdisk.tcmu

import org.aspen_ddp.aspen.vdisk.tcmu.sys.{Libc, Linux, SysError}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.lang.foreign.{Arena, ValueLayout}
import java.lang.foreign.ValueLayout.JAVA_BYTE

class LibcSuite extends AnyFunSuite with Matchers:

  test("open of a missing file reports ENOENT"):
    val e = intercept[SysError](Libc.open("/nonexistent/x", Linux.O_RDONLY))
    e.errno should be (Linux.ENOENT)
    e.call should be ("open")
    e.message should include ("No such file")

  test("pipe round trip"):
    val (r, w) = Libc.pipe()
    val arena = Arena.ofConfined()
    try
      val out = arena.allocate(4)
      Array[Byte](1, 2, 3, 4).zipWithIndex.foreach((b, i) => out.set(JAVA_BYTE, i, b))
      Libc.write(w, out) should be (4L)
      val in = arena.allocate(4)
      Libc.read(r, in) should be (4L)
      in.toArray(JAVA_BYTE).toList should be (List[Byte](1, 2, 3, 4))
    finally
      arena.close()
      Libc.close(r)
      Libc.close(w)

  test("poll reports a readable pipe"):
    val (r, w) = Libc.pipe()
    val arena = Arena.ofConfined()
    try
      Libc.poll(Seq(r -> Linux.POLLIN), 0).toList should be (List(0))
      Libc.write(w, arena.allocate(1)) should be (1L)
      Libc.poll(Seq(r -> Linux.POLLIN), -1).toList should be (List(Linux.POLLIN))
    finally
      arena.close()
      Libc.close(r)
      Libc.close(w)

  test("mmapShared maps a file and the closed arena makes access throw"):
    val f = os.temp(Array.fill[Byte](4096)(0))
    try
      val init = os.read.bytes(f)
      init(100) = 7
      os.write.over(f, init)
      val fd = Libc.open(f.toString, Linux.O_RDWR | Linux.O_CLOEXEC)
      val arena = Arena.ofShared()
      try
        val seg = Libc.mmapShared(fd, 4096, arena)
        seg.get(JAVA_BYTE, 100) should be (7.toByte)
        seg.set(JAVA_BYTE, 200, 9.toByte)
        arena.close()
        os.read.bytes(f)(200) should be (9.toByte)
        intercept[IllegalStateException](seg.get(JAVA_BYTE, 0))
      finally
        Libc.close(fd)
    finally
      os.remove(f)

  test("geteuid returns a non-negative id"):
    Libc.geteuid() should be >= 0
