package org.aspen_ddp.aspen

class TempDirManager {

  val tdir: os.Path = os.temp.dir(prefix = "scalatest", deleteOnExit = false)

  def delete(): Unit = os.remove.all(tdir)
}
