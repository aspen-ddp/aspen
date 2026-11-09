package org.aspen_ddp.aspen.server.transaction

import org.aspen_ddp.aspen.common.transaction.{TransactionId, TransactionStatus}
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import scala.concurrent.duration.*

class TransactionStatusCacheSuite extends AnyFunSuite with Matchers with Eventually:

  test("the number of entries is capped") {
    val cache = new TransactionStatusCache(FiniteDuration(1, HOURS), maxEntries = 100)
    (0 until 10_000).foreach(i => cache.updateStatus(TransactionId(new UUID(0, i)), TransactionStatus.Committed))
    // Eviction is eventual: one cleanUp handles a bounded number of the buffered writes, more under a loaded CPU
    eventually(timeout(org.scalatest.time.Span(10, org.scalatest.time.Seconds)),
               interval(org.scalatest.time.Span(1, org.scalatest.time.Millis))):
      cache.size should be <= 100L
  }

  test("an entry within the cap is remembered") {
    val cache = new TransactionStatusCache(FiniteDuration(1, HOURS), maxEntries = 100)
    val txid = TransactionId(new UUID(0, 1))
    cache.updateStatus(txid, TransactionStatus.Committed, finalized = true)
    cache.getStatus(txid) shouldBe Some(TransactionStatusCache.Entry(TransactionStatus.Committed, true))
  }

  test("a size eviction of an entry younger than the early-eviction age is counted and warned") {
    val cache = new TransactionStatusCache(FiniteDuration(7, SECONDS), maxEntries = 1,
      earlyEvictionAge = Some(FiniteDuration(6, SECONDS)))
    cache.updateStatus(TransactionId(new UUID(0, 1)), TransactionStatus.Committed)
    cache.updateStatus(TransactionId(new UUID(0, 2)), TransactionStatus.Committed)
    // cleanUp drains both buffered writes and evicts, running the listener on this thread
    cache.cleanUpForTest()
    cache.earlyEvictions shouldBe 1L
  }

  test("a size eviction is not counted without an early-eviction age") {
    val cache = new TransactionStatusCache(FiniteDuration(7, SECONDS), maxEntries = 1)
    cache.updateStatus(TransactionId(new UUID(0, 1)), TransactionStatus.Committed)
    cache.updateStatus(TransactionId(new UUID(0, 2)), TransactionStatus.Committed)
    cache.size shouldBe 1L
    cache.earlyEvictions shouldBe 0L
  }
