package org.aspen_ddp.aspen.server.store

import java.util.UUID

import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.objects.{Metadata, ObjectId, ObjectRefcount, ObjectRevision, ObjectType}
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.server.store.cache.SimpleLRUObjectCache
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.language.implicitConversions

/** Checks the one invariant every cache must uphold: a lookup of id X must never
  * return the state of some other object Y.
  */
class LRUCacheIdentitySuite extends AnyFunSuite with Matchers {

  def mkobj(n: Int): ObjectState = new ObjectState(
    ObjectId(new UUID(0, n)),
    Metadata(ObjectRevision(TransactionId(new UUID(0, 2))), ObjectRefcount(1, 1), HLCTimestamp(1)),
    ObjectType.Data,
    DataBuffer(new Array[Byte](0))
  )

  test("lookup never returns another object's state") {
    val maxEntries = 8
    val c = new SimpleLRUObjectCache(maxEntries)
    val objs = (1 to 40).map(mkobj)

    val rng = new scala.util.Random(12345)

    // Fill
    objs.take(maxEntries).foreach(c.insert)

    var inserted = objs.take(maxEntries).map(_.objectId).toSet

    for i <- maxEntries until objs.length do
      // Interleave a few random gets (the access pattern a real store has)
      for _ <- 0 until 3 do
        val id = objs(rng.nextInt(objs.length)).objectId
        c.get(id).foreach { st =>
          withClue(s"step $i: get($id) returned state for ${st.objectId}: ") {
            st.objectId should be(id)
          }
        }

      c.insert(objs(i))
      inserted += objs(i).objectId

      // Every id still resolvable must map to its own state
      for o <- objs do
        c.get(o.objectId).foreach { st =>
          withClue(s"step $i: get(${o.objectId}) returned state for ${st.objectId}: ") {
            st.objectId should be(o.objectId)
          }
        }
  }
}
