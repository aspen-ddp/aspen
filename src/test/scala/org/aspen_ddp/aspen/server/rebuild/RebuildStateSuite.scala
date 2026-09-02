package org.aspen_ddp.aspen.server.rebuild

import org.aspen_ddp.aspen.common.objects.{Key, ObjectId}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class RebuildStateSuite extends AnyFunSuite with Matchers:

  private val storeId = StoreId(PoolId(UUID.fromString("11111111-1111-1111-1111-111111111111")),
                                3.toByte)

  private def objectId(u: String): ObjectId = ObjectId(UUID.fromString(u))

  private def tmpDir(): os.Path =
    os.temp.dir(prefix = "rebuild-state-suite")

  test("a state with no progress round-trips"):
    val dir = tmpDir()
    val state = RebuildState(storeId, None, Nil)
    RebuildState.save(dir, state)
    RebuildState.load(dir) should be(Some(state))

  test("a state with a checkpoint and failures round-trips"):
    val dir = tmpDir()
    // An arbitrary non-UTF8 byte sequence: allocation-tree keys are raw ObjectId bytes, so the
    // encoding has to survive bytes that are not printable characters.
    val key = Key(Array[Byte](0, 1, -1, 127, -128, 42))
    val state = RebuildState(storeId, Some(key),
      List(objectId("22222222-2222-2222-2222-222222222222"),
           objectId("33333333-3333-3333-3333-333333333333")))
    RebuildState.save(dir, state)
    val loaded = RebuildState.load(dir).get
    loaded.storeId should be(storeId)
    loaded.lastRestoredKey.map(_.bytes.toList) should be(Some(key.bytes.toList))
    loaded.failedObjects should be(state.failedObjects)

  test("load returns None when no checkpoint exists"):
    RebuildState.load(tmpDir()) should be(None)

  test("save overwrites an existing checkpoint"):
    val dir = tmpDir()
    RebuildState.save(dir, RebuildState(storeId, Some(Key(Array[Byte](1))), Nil))
    RebuildState.save(dir, RebuildState(storeId, Some(Key(Array[Byte](2))), Nil))
    RebuildState.load(dir).get.lastRestoredKey.map(_.bytes.toList) should be(Some(List[Byte](2)))

  test("the failed-object cap is 10000"):
    RebuildState.MaxFailedObjects should be(10000)
