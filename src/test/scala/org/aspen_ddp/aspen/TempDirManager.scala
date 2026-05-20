package org.aspen_ddp.aspen

import java.io.File
import java.nio.file.Files

class TempDirManager {

  val tdir: File = Files.createTempDirectory("scalatest").toFile

  def delete(): Unit =

    def cleanup(f: File): Unit =
      if f.isFile then
        f.delete()
      else
        val children = f.listFiles()
        if children != null then
          children.foreach(cleanup)
        f.delete()

    cleanup(tdir)
}