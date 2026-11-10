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

  test("the late flag is remembered") {
    val cache = new TransactionStatusCache(FiniteDuration(1, HOURS), maxEntries = 100)
    val txid = TransactionId(new UUID(0, 1))
    cache.updateStatus(txid, TransactionStatus.Committed, finalized = true, late = true)
    cache.getStatus(txid).get.late shouldBe true
    cache.updateStatus(txid, TransactionStatus.Committed, finalized = true)
    cache.getStatus(txid).get.late shouldBe true
  }

  // Updates are monotone (Ruling 30): a later write of the same transaction never loses what an earlier one recorded

  test("the finalized flag is sticky") {
    val cache = new TransactionStatusCache(FiniteDuration(1, HOURS), maxEntries = 100)
    val txid = TransactionId(new UUID(0, 1))
    cache.updateStatus(txid, TransactionStatus.Aborted, finalized = true)
    cache.updateStatus(txid, TransactionStatus.Aborted)
    cache.getStatus(txid) shouldBe Some(TransactionStatusCache.Entry(TransactionStatus.Aborted, finalized = true))
  }

  test("a resolved status never reverts to Unresolved") {
    val cache = new TransactionStatusCache(FiniteDuration(1, HOURS), maxEntries = 100)
    val txid = TransactionId(new UUID(0, 1))
    cache.updateStatus(txid, TransactionStatus.Committed)
    cache.updateStatus(txid, TransactionStatus.Unresolved)
    cache.getStatus(txid).get.status shouldBe TransactionStatus.Committed
  }

  test("an Unresolved entry takes a later resolution") {
    val cache = new TransactionStatusCache(FiniteDuration(1, HOURS), maxEntries = 100)
    val txid = TransactionId(new UUID(0, 1))
    cache.updateStatus(txid, TransactionStatus.Unresolved)
    cache.updateStatus(txid, TransactionStatus.Committed, finalized = true, late = true)
    cache.getStatus(txid) shouldBe Some(TransactionStatusCache.Entry(TransactionStatus.Committed, true, true))
  }

  // X3: a store's Tx created before the transaction was finalized elsewhere on the host, then resolved by a delayed
  // non-late TxResolved, used to overwrite the finalized late entry, so the finalized relay answered a replay non-late
  test("the X3 sequence of writes keeps the entry finalized and late") {
    val cache = new TransactionStatusCache(FiniteDuration(1, HOURS), maxEntries = 100)
    val txid = TransactionId(new UUID(0, 1))
    cache.updateStatus(txid, TransactionStatus.Unresolved) // a sibling store's Tx is created
    cache.updateStatus(txid, TransactionStatus.Committed, finalized = true, late = true) // another store finalizes
    cache.updateStatus(txid, TransactionStatus.Unresolved) // yet another Tx is created
    cache.updateStatus(txid, TransactionStatus.Committed) // the sibling's delayed resolution (Tx.resolved)
    cache.getStatus(txid) shouldBe Some(TransactionStatusCache.Entry(TransactionStatus.Committed, true, true))
  }
