package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.{StoreId, StoreIncarnation}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class StoreIncarnationFileSuite extends AnyFunSuite with Matchers:
  private val storeId = StoreId(PoolId(new UUID(1, 2)), 3.toByte)

  test("a missing file is created durably and read back unchanged"):
    val dir = os.temp.dir()
    try
      val first = StoreIncarnationFile.loadOrCreate(dir, storeId)
      os.exists(dir / StoreIncarnationFile.FileName) should be(true)
      os.list(dir).map(_.last) should be(Seq(StoreIncarnationFile.FileName))
      first should not be StoreIncarnation.default(storeId)
      StoreIncarnationFile.loadOrCreate(dir, storeId) should be(first)
    finally os.remove.all(dir)

  test("an unreadable file is replaced by a fresh incarnation"):
    val dir = os.temp.dir()
    try
      os.write.over(dir / StoreIncarnationFile.FileName, "not a uuid")
      val fresh = StoreIncarnationFile.loadOrCreate(dir, storeId)
      os.read(dir / StoreIncarnationFile.FileName).trim should be(fresh.uuid.toString)
      StoreIncarnationFile.loadOrCreate(dir, storeId) should be(fresh)
    finally os.remove.all(dir)

  test("a backend with no directory gets the store's default and nothing is written"):
    StoreIncarnationFile.loadOrCreate(os.root, storeId) should be(StoreIncarnation.default(storeId))
    os.exists(os.root / StoreIncarnationFile.FileName) should be(false)
