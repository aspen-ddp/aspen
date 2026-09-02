package org.aspen_ddp.aspen.server.rebuild

import org.aspen_ddp.aspen.common.objects.{Key, ObjectId}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files
import java.nio.file.attribute.BasicFileAttributes
import java.util.UUID

class RebuildStateSuite extends AnyFunSuite with Matchers:

  private val storeId = StoreId(PoolId(UUID.fromString("11111111-1111-1111-1111-111111111111")),
                                3.toByte)

  private def objectId(u: String): ObjectId = ObjectId(UUID.fromString(u))

  private def tmpDir(): os.Path =
    os.temp.dir(prefix = "rebuild-state-suite")

  /** The file's identity on disk, as distinct from its path. Cancels the test rather than
   *  failing it on a filesystem that does not report one. */
  private def fileKey(p: os.Path): Object =
    val key = Files.readAttributes(p.toNIO, classOf[BasicFileAttributes]).fileKey()
    assume(key != null, s"filesystem does not report a file key for $p")
    key

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

  test("save replaces the checkpoint by rename rather than truncating it in place"):
    val dir = tmpDir()
    RebuildState.save(dir, RebuildState(storeId, Some(Key(Array[Byte](1))), Nil))
    val first = fileKey(dir / RebuildState.stateFilename)
    RebuildState.save(dir, RebuildState(storeId, Some(Key(Array[Byte](2))), Nil))
    val second = fileKey(dir / RebuildState.stateFilename)

    // Aspen is crash-only, so a crash between a truncate and the write that follows it is the
    // ordinary termination mode landing in an unlucky window. The inode is what distinguishes
    // the two implementations: an in-place overwrite reuses it and leaves the file empty for
    // that window, a rename installs a new one and leaves the old contents readable until the
    // instant it is replaced.
    second should not be first
    // The scratch file is renamed away, not left behind.
    os.exists(dir / RebuildState.tempFilename) should be(false)
    RebuildState.load(dir).get.lastRestoredKey.map(_.bytes.toList) should be(Some(List[Byte](2)))

  test("load returns None for an unparsable checkpoint"):
    // A checkpoint torn by a crash predating the atomic save, in the shapes it can take: the
    // empty file a truncate leaves before its write lands, a prefix that is still valid YAML but
    // no longer a RebuildState, a prefix that is not YAML at all, and a document that parses to
    // something other than a mapping.
    val cases = Map(
      "empty" -> "",
      "truncated" -> "store-id: 11111111-1111-1111-1111-111111111111:3\nlast-rest",
      "unparsable" -> "failed-objects: [unclosed",
      "not a mapping" -> "just a plain scalar")

    cases.foreach: (name: String, contents: String) =>
      val dir = tmpDir()
      os.write.over(dir / RebuildState.stateFilename, contents)
      withClue(s"$name: ") {
        // Total, not throwing: load is the first statement of a rebuild pass, so a throw here
        // makes the store permanently unrebuildable without an operator deleting the file.
        RebuildState.load(dir) should be(None)
      }

  test("removeAll drops the checkpoint and any scratch file, and tolerates neither existing"):
    val dir = tmpDir()
    RebuildState.removeAll(dir)
    os.write.over(dir / RebuildState.stateFilename, "anything")
    os.write.over(dir / RebuildState.tempFilename, "anything")
    RebuildState.removeAll(dir)
    os.exists(dir / RebuildState.stateFilename) should be(false)
    os.exists(dir / RebuildState.tempFilename) should be(false)

  test("the failed-object cap is 10000"):
    RebuildState.MaxFailedObjects should be(10000)
