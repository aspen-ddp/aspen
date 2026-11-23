package org.aspen_ddp.aspen.server.store

import java.util.UUID

import org.aspen_ddp.aspen.common.network.ReadResponse
import org.aspen_ddp.aspen.common.objects.{FullObject, ObjectType}
import org.aspen_ddp.aspen.server.store.FrontendSuite.*
import org.aspen_ddp.aspen.server.store.backend.MapBackend
import org.aspen_ddp.aspen.server.store.cache.HostObjectCache
import org.aspen_ddp.aspen.server.transaction.TransactionStatusCache
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Promise

/** The Frontend drops a client read, with no state and no answer, while its backend reports its device's reads over
  * the bound; internal reads are never dropped */
class FrontendReadBoundSuite extends AnyFunSuite with Matchers:

  private final class Fixture:
    @volatile var over = false
    var reads = 0
    val backend: MapBackend = new MapBackend(storeId):
      override def readsOverBound: Boolean = over
      override def read(pointer: org.aspen_ddp.aspen.common.objects.ObjectPointer): Unit =
        reads += 1
        super.read(pointer)
    val net = new TestNet
    val frontend = new Frontend(storeId, backend, new HostObjectCache(1024 * 1024).forStore(storeId), net,
      new TestCrl, new TransactionStatusCache(), loadedAt = FrontendSuite.longLoaded)
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
    val os = mkKVObjectState()
    backend.bootstrapAllocate(oid2, os.objectType, os.metadata, os.data)

  test("a network read over the bound is dropped with no state"):
    val f = new Fixture
    f.over = true
    f.frontend.readObjectForNetwork(clientId, new UUID(1, 1), kvPtr, FullObject())
    f.net.clientResponses shouldBe empty
    f.reads shouldBe 0
    // No pending-read entry was left behind: once under the bound the same read starts afresh and is answered once
    f.over = false
    f.frontend.readObjectForNetwork(clientId, new UUID(1, 1), kvPtr, FullObject())
    f.reads shouldBe 1
    f.net.clientResponses.collect { case r: ReadResponse => r }.size shouldBe 1

  test("a network read of a cached object is dropped too, and nothing is parked for a dropped awaitCommit read"):
    val f = new Fixture
    f.frontend.readObjectForNetwork(clientId, new UUID(1, 1), kvPtr, FullObject())
    f.net.clientResponses.size shouldBe 1
    f.over = true
    f.frontend.readObjectForNetwork(clientId, new UUID(1, 2), kvPtr, FullObject(), Set(txid1))
    f.net.clientResponses.size shouldBe 1
    f.reads shouldBe 1

  test("internal reads ignore the bound"):
    val f = new Fixture
    f.over = true
    val snapshot = Promise[RepairSnapshot]()
    f.frontend.snapshotForRepair(kvPtr, snapshot)
    snapshot.isCompleted shouldBe true
    f.reads shouldBe 1

  test("reads under the bound are answered"):
    val f = new Fixture
    f.frontend.readObjectForNetwork(clientId, new UUID(1, 1), kvPtr, FullObject())
    f.net.clientResponses.collect { case r: ReadResponse => r }.size shouldBe 1
    f.reads shouldBe 1

  test("the drop is logged once per transition into and out of the state, not per read"):
    val f = new Fixture
    val lines = new java.util.concurrent.LinkedBlockingQueue[String]()
    val handler = scribe.handler.LogHandler(scribe.Level.Info): r =>
      val text = r.logOutput.plainText
      if text.contains("read backlog") then lines.add(text)
    org.aspen_ddp.aspen.LogCapture.during(org.aspen_ddp.aspen.LogCapture.Aspen, handler):
      f.over = true
      (1 to 5).foreach(i => f.frontend.readObjectForNetwork(clientId, new UUID(1, i), kvPtr, FullObject()))
      f.over = false
      (6 to 8).foreach(i => f.frontend.readObjectForNetwork(clientId, new UUID(1, i), kvPtr, FullObject()))
    val seen = lines.toArray(Array.empty[String]).toList
    seen.count(_.contains("is over the bound")) shouldBe 1
    seen.count(_.contains("back under the bound")) shouldBe 1
