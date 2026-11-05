package org.aspen_ddp.aspen.server.transaction

import org.aspen_ddp.aspen.common.transaction.{TransactionId, TransactionStatus}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import scala.concurrent.duration.*

class TransactionStatusCacheSuite extends AnyFunSuite with Matchers:

  test("the number of entries is capped") {
    val cache = new TransactionStatusCache(FiniteDuration(1, HOURS), maxEntries = 100)
    (0 until 10_000).foreach(i => cache.updateStatus(TransactionId(new UUID(0, i)), TransactionStatus.Committed))
    cache.size should be <= 100L
  }

  test("an entry within the cap is remembered") {
    val cache = new TransactionStatusCache(FiniteDuration(1, HOURS), maxEntries = 100)
    val txid = TransactionId(new UUID(0, 1))
    cache.updateStatus(txid, TransactionStatus.Committed, finalized = true)
    cache.getStatus(txid) shouldBe Some(TransactionStatusCache.Entry(TransactionStatus.Committed, true))
  }
