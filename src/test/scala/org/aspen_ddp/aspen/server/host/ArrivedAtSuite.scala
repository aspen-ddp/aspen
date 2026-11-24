package org.aspen_ddp.aspen.server.host

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ArrivedAtSuite extends AnyFunSuite with Matchers:
  private def withStoreDir(f: os.Path => Unit): Unit =
    val dir = os.temp.dir()
    try f(dir / "store")
    finally
      os.walk(dir).filter(os.isDir(_)).foreach(os.perms.set(_, "rwx------"))
      os.remove.all(dir)

  test("a store with no arrival file has no arrival"):
    withStoreDir: store =>
      os.makeDir(store)
      LoadDisk.readArrivedAt(store) should be(None)

  test("a written arrival is read back"):
    withStoreDir: store =>
      os.makeDir(store)
      os.write(store / StoreLoader.ArrivedAtFile, "12345")
      LoadDisk.readArrivedAt(store) should be(Some(12345L))

  test("a failed stat of the arrival file counts as an arrival now, keeping the arrival margin (X-W26)"):
    withStoreDir: store =>
      os.makeDir(store)
      os.write(store / StoreLoader.ArrivedAtFile, "12345")
      // A directory without search permission fails every stat beneath it with EACCES, where os.exists answers false
      os.perms.set(store, "rw-------")
      val before = System.currentTimeMillis()
      val arrived = LoadDisk.readArrivedAt(store)
      arrived.isDefined should be(true)
      arrived.get should be >= before
