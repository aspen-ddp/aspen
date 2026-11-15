package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.{StoreId, StoreIncarnation}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import java.util.concurrent.LinkedBlockingQueue

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
      val file = dir / StoreIncarnationFile.FileName
      os.write.over(file, "not a uuid")
      val lines = new LinkedBlockingQueue[(scribe.Level, String)]()
      val handler = scribe.handler.LogHandler(scribe.Level.Info): r =>
        val text = r.logOutput.plainText
        if text.contains(file.toString) then
          lines.add(r.level -> text)
      scribe.Logger.root.withHandler(handler).replace()
      val fresh =
        try StoreIncarnationFile.loadOrCreate(dir, storeId)
        finally scribe.Logger.root.withoutHandler(handler).replace()
      // Replaced loudly, naming the file: the store loses its identity
      lines.toArray(Array.empty[(scribe.Level, String)]).toList.map(_._1) should be(List(scribe.Level.Warn))
      os.read(file).trim should be(fresh.uuid.toString)
      StoreIncarnationFile.loadOrCreate(dir, storeId) should be(fresh)
    finally os.remove.all(dir)

  test("a backend with no directory gets the store's default and nothing is written"):
    StoreIncarnationFile.loadOrCreate(os.root, storeId) should be(StoreIncarnation.default(storeId))
    os.exists(os.root / StoreIncarnationFile.FileName) should be(false)
