package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.{StoreId, StoreIncarnation}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class StoreIncarnationFileSuite extends AnyFunSuite with Matchers:
  private val storeId = StoreId(PoolId(new UUID(1, 2)), 3.toByte)

  test("a written file is read back unchanged"):
    val dir = os.temp.dir()
    try
      val first = StoreIncarnationFile.writeFresh(dir)
      first should not be StoreIncarnation.default(storeId)
      StoreIncarnationFile.load(dir, storeId) should be(first)
      StoreIncarnationFile.load(dir, storeId) should be(first)
    finally os.remove.all(dir)

  test("a missing file is reported lost and nothing is written (SL-W15)"):
    val dir = os.temp.dir()
    try
      an[StoreIncarnationFile.IncarnationLost] should be thrownBy StoreIncarnationFile.load(dir, storeId)
      os.list(dir) should be(Nil)
    finally os.remove.all(dir)

  test("an unparsable file throws and is never replaced (SL-W15)"):
    val dir = os.temp.dir()
    try
      val file = dir / StoreIncarnationFile.FileName
      os.write.over(file, "not a uuid")
      val err = the[Exception] thrownBy StoreIncarnationFile.load(dir, storeId)
      err should not be a[StoreIncarnationFile.IncarnationLost]
      os.read(file) should be("not a uuid")
    finally os.remove.all(dir)

  test("a file whose stat fails throws, not lost, and is never replaced (SL-W15)"):
    val dir = os.temp.dir()
    try
      val inner = dir / "store"
      os.makeDir(inner)
      val written = StoreIncarnationFile.writeFresh(inner)
      // No search permission on the directory: the stat fails with EACCES while the file is there
      os.perms.set(inner, "rw-------")
      val err =
        try the[Exception] thrownBy StoreIncarnationFile.load(inner, storeId)
        finally os.perms.set(inner, "rwx------")
      err should not be a[StoreIncarnationFile.IncarnationLost]
      StoreIncarnationFile.load(inner, storeId) should be(written)
    finally os.remove.all(dir)

  test("a backend with no directory gets the store's default and nothing is written"):
    StoreIncarnationFile.load(os.root, storeId) should be(StoreIncarnation.default(storeId))
    os.exists(os.root / StoreIncarnationFile.FileName) should be(false)
