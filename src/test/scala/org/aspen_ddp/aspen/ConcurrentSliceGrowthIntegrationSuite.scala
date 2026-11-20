package org.aspen_ddp.aspen

import org.aspen_ddp.aspen.client.{TestTransactions, Transaction}
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.network.{TxFinalized, TxMessage, TxResolved}
import org.aspen_ddp.aspen.common.objects.{Insert, Key, KeyValueObjectPointer, Value}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate
import org.aspen_ddp.aspen.server.store.KVObjectState

import scala.concurrent.Future
import scala.language.implicitConversions

/** Commit votes that hold compatible locks on one KeyValue object fit the slice limit together, so none of them fails
  * its apply re-check (transaction-requirements.md 6, slice-size reservations). Two transactions insert disjoint keys
  * into an object with room for one entry, not two. Each fits on its own, and their key locks are compatible, so
  * without the reservation every store votes commit for both and skips the second apply: a committed update lost.
  *
  * Stores check growth against the slice they hold, which lacks the commits they did not vote for, so growth is
  * admitted only within the KeyValue limit (IDA.maxKVSliceSize, transaction-requirements R16): the union of the
  * committed updates, which repair and rebuild install, then fits the pool's slice limit (cross-document-review X-W9).
  */
class ConcurrentSliceGrowthIntegrationSuite extends IntegrationTestSuite:
  val sliceLimit: Int = 16384

  override def testPoolMaxSliceSize: Option[Int] = Some(sliceLimit)

  /** The limit the stores admit KeyValue growth within */
  def kvLimit: Long = net.ida.maxKVSliceSize(sliceLimit)

  private def k(s: String): Key = Key(s.getBytes)

  /** A KeyValue object, linked into the radicle, holding one value that leaves `headroom` bytes below kvLimit */
  private def allocateNearlyFull(headroom: Int): Future[KeyValueObjectPointer] =
    // Empty state: mask byte + 1-byte count. Entry: 24 bytes of revision and timestamp, 1 + 3 key, 2 + n value
    val fill = kvLimit.toInt - headroom - (2 + 24 + 4 + 2)
    given tx: Transaction = TestTransactions.replaySafe(client.newTransaction())
    tx.update(radicle, None, None, List(KeyValueUpdate.DoesNotExist(k("kvo"))), List(Insert(k("kvo"), Array[Byte](1))))
    for
      pool <- client.getStoragePool(Radicle.poolId)
      kp <- pool.allocator.allocateKeyValueObject(Map(k("big") -> Value(new Array[Byte](fill))))
      _ <- tx.commit()
    yield kp

  private def insert(kp: KeyValueObjectPointer, key: Key, before: Transaction => Unit = _ => ()):
    (Transaction, Future[Boolean]) =
    val tx = TestTransactions.replaySafe(client.newTransaction())
    before(tx)
    tx.update(kp, None, None, List(KeyValueUpdate.DoesNotExist(key)), List(Insert(key, new Array[Byte](40))))
    tx -> tx.commit().map(_ => true).recover { case _ => false }

  atest("disjoint inserts that each fit but not together never both commit"):
    // An inserted entry: 24 + (1 + 2) key + (1 + 40) value = 68 bytes. Room for one, not two. The first transaction's
    // outcome is held back from every store, so its commit votes still hold their locks when the second one votes
    var held = List[TxMessage]()
    for
      kp <- allocateNearlyFull(100)
      (tx1, f1) = insert(kp, k("a1"))
      _ = net.dropTxMessage =
            case m @ (_: TxResolved | _: TxFinalized) if m.transactionId == tx1.id =>
              held = held :+ m
              true
            case _ => false
      c1 <- f1
      (_, f2) = insert(kp, k("a2"))
      c2 <- f2
      _ = net.dropTxMessage = _ => false
      _ = held.foreach(net.deliverTransactionMessage)
      _ <- net.waitForTransactionsToComplete()
      kvos <- client.read(kp)
    yield
      def held(i: Int): Set[Key] = net.stores(i).m.get(kp.id).flatMap(_.kvState).map(_.content.keySet).getOrElse(Set())
      Set(c1, c2) should contain (true)
      (c1 && c2) should be (false)
      kvos.contents.contains(k("a1")) should be (c1)
      kvos.contents.contains(k("a2")) should be (c2)
      val committed = Set(k("big")) ++ Option.when(c1)(k("a1")) ++ Option.when(c2)(k("a2"))
      net.stores.indices.map(held) should be (net.stores.indices.map(_ => committed))

  atest("growths committed by different voter sets fit the slice limit together"):
    // Room below kvLimit for two inserted entries (68 bytes each), not three. Each insert misses one store outright
    // (every message of it dropped there, and that store never its leader), a different one each time, so each store
    // votes for two of them and holds two, and each insert commits on the other two. The union holds all three
    val keys = List(k("b0"), k("b1"), k("b2"))
    def missing(i: Int)(tx: Transaction): Unit =
      net.droppedTx += ((net.stores(i).storeId, tx.id))
      net.offlineStores = Set(net.stores(i).storeId)
    for
      kp <- allocateNearlyFull(2 * 68 + 10)
      c0 <- insert(kp, keys(0), missing(2))._2
      c1 <- insert(kp, keys(1), missing(0))._2
      c2 <- insert(kp, keys(2), missing(1))._2
      _ = net.offlineStores = Set()
      _ <- net.waitForTransactionsToComplete()
      kvos <- client.read(kp)
    yield
      def state(i: Int) = net.stores(i).m(kp.id).kvState.get
      List(c0, c1, c2) should be (List(true, true, true))
      keys.foreach(key => kvos.contents.contains(key) should be (true))
      net.stores.indices.foreach(i => state(i).encodedSize.toLong should be <= kvLimit)
      val union = net.stores.indices.map(state(_).content).reduce(_ ++ _)
      union.keySet should be (Set(k("big")) ++ keys)
      KVObjectState.encodedSize(None, None, None, None, union).toLong should be <= sliceLimit.toLong
