package org.aspen_ddp.aspen.server.crl.simple

import org.aspen_ddp.aspen.FileBasedTests
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, ObjectId, ObjectRevision}
import org.aspen_ddp.aspen.common.paxos.{PersistentState, ProposalId}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{DataUpdate, DataUpdateOperation, ObjectUpdate, TransactionDescription,
  TransactionDisposition, TransactionId, TransactionStatus}
import org.aspen_ddp.aspen.server.crl.{CrlAdmission, TransactionRecoveryState}

import java.util.UUID
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import scala.collection.mutable
import scala.util.Random

/** A seeded random workload against a real SimpleCRL of 4 streams of 256 KiB, kept within the admission the
  * Frontend enforces (crash-recovery-log 6.1): a new transaction, or late data for an admitted one, only while the
  * counted total is below the limit, and at most T per transaction. Deletions are delayed at random, and bursts are
  * held live across a full wrap of the ring. Every so often the CRL is shut down (once everything queued is durable)
  * and reopened, and what it recovers must be the model. Its `failed` never completes: within the limit every entry
  * is written (Lemmas A-C) */
class SimpleCRLStressSuite extends FileBasedTests:

  private val numStreams = 4
  private val streamSize = 256 * 1024L
  private val opsPerSeed = 1200
  private val reopenEvery = 200

  private val pool = PoolId(new UUID(0x57, 0))
  private val stores = Vector(StoreId(pool, 0), StoreId(pool, 1))
  private val ida = Replication(3, 2)
  private val pointer = DataObjectPointer(ObjectId(new UUID(0x57, 1)), pool, Array[Byte]())

  /** A transaction in the model: its newest saved state, what admission counts for it, and whether its data was
    * dropped. A dropped record's next save leaves the updates out; until one, recovery returns them */
  private case class Held(state: TransactionRecoveryState, charged: Long, dropped: Boolean, savedSinceDrop: Boolean):
    def recovered: TransactionRecoveryState =
      if savedSinceDrop then state.copy(objectUpdates = Nil) else state

  private def charge(state: TransactionRecoveryState): Long = CrlAdmission.recoveredBytes(state)

  for seed <- 1 to 20 do
    test(s"A random workload within admission recovers its model and never fails the CRL, seed $seed"):
      run(seed)

  private def run(seed: Int): Unit =
    val rnd = new Random(seed)
    var opened = SimpleCRL(tdir, numStreams, streamSize)
    var crl = opened.crl
    val limit = crl.admissionLimitBytes
    val maxTx = crl.maxTransactionBytes

    val txs = mutable.Map[(StoreId, TransactionId), Held]()
    val stubs = mutable.Map[(StoreId, TransactionId), HLCTimestamp]()
    // Bursts kept live until the op index given: no deletion picks them before then
    val heldUntil = mutable.Map[(StoreId, TransactionId), Int]()
    var total = 0L
    var nextId = 0L

    def newTxd(tid: TransactionId): DataBuffer =
      TransactionDescription(tid, HLCTimestamp(seed.toLong << 32 | nextId), pointer, 1.toByte,
        List(DataUpdate(pointer, ObjectRevision(tid), DataUpdateOperation.Overwrite)),
        List(), None, List(), List(), ida, Map(pool -> ida)).serialize()

    def updates(count: Int, maxBytes: Int): List[ObjectUpdate] =
      List.tabulate(count): k =>
        val size = if rnd.nextInt(10) == 0 then rnd.nextInt(maxBytes.max(1)) else rnd.nextInt(2048.min(maxBytes.max(1)))
        ObjectUpdate(ObjectId(new UUID(nextId, k)), DataBuffer(Array.fill[Byte](size)(rnd.nextInt().toByte)))

    def save(key: (StoreId, TransactionId), state: TransactionRecoveryState, waitFor: Boolean): Unit =
      val done = new LinkedBlockingQueue[String]()
      crl.save(key._2, state, () => done.put(""))
      if waitFor then
        assert(done.poll(10, TimeUnit.SECONDS) == "", s"seed $seed: a save was not written")

    def paxos(n: Int): PersistentState =
      PersistentState(Some(ProposalId(n, 1)), if n % 2 == 0 then Some((ProposalId(n, 1), true)) else None)

    /** A new transaction, admitted only below the limit, with 0 to 3 updates and at most T counted */
    def newTransaction(op: Int, holdFor: Int): Unit =
      if total < limit then
        nextId += 1
        val key = (stores(rnd.nextInt(stores.size)), TransactionId(new UUID(seed, nextId)))
        val txd = newTxd(key._2)
        val count = if rnd.nextInt(4) == 0 then 0 else 1 + rnd.nextInt(3)
        val room = (maxTx - CrlAdmission.TxOverheadBytes - txd.size) / 3 - CrlAdmission.ObjectUpdateOverheadBytes
        val state = TransactionRecoveryState(key._1, txd, updates(count, (room.min(40 * 1024)).toInt),
          TransactionDisposition.Undetermined, TransactionStatus.Unresolved, PersistentState.initial)
        val c = charge(state)
        assert(c <= maxTx)
        total += c
        txs(key) = Held(state, c, dropped = false, savedSinceDrop = false)
        if holdFor > 0 then
          heldUntil(key) = op + holdFor
        save(key, state, waitFor = rnd.nextInt(3) == 0)

    def pick(op: Int, deletable: Boolean): Option[(StoreId, TransactionId)] =
      val keys = txs.keysIterator.filter(k => !deletable || heldUntil.get(k).forall(_ <= op)).toVector
      if keys.isEmpty then None else Some(keys(rnd.nextInt(keys.size)))

    def reopen(): Unit =
      // Shutdown discards unwritten entries: everything queued is made durable first
      val barrier = new LinkedBlockingQueue[String]()
      crl.whenDurable(stores(0), () => barrier.put(""))
      assert(barrier.poll(10, TimeUnit.SECONDS) == "", s"seed $seed: the barrier never ran")
      assert(!crl.failed.isCompleted, s"seed $seed: the CRL failed: ${crl.failed.value}")
      crl.shutdown()
      opened = SimpleCRL(tdir, numStreams, streamSize)
      crl = opened.crl
      val recovered = opened.trsList.map(t => (t.storeId, t.txd.transactionId) -> t).toMap
      assert(recovered.keySet == txs.keySet, s"seed $seed")
      txs.foreach: (key, held) =>
        val want = held.recovered
        val got = recovered(key)
        assert(got.serializedTxd == want.serializedTxd, s"seed $seed, $key")
        assert(got.objectUpdates == want.objectUpdates, s"seed $seed, $key")
        assert((got.disposition, got.status, got.paxosAcceptorState) ==
          (want.disposition, want.status, want.paxosAcceptorState), s"seed $seed, $key")
      assert(opened.appliedStubs.map((id, start) => (id.storeId, id.transactionId) -> start) == stubs.toMap,
        s"seed $seed")
      // Admission counts what the open recovered, as the device's open charges it
      txs.foreach: (key, _) =>
        val got = recovered(key)
        txs(key) = Held(got, charge(got), dropped = false, savedSinceDrop = false)
      total = crl.recoveredBytesByStore.values.sum

    for op <- 1 to opsPerSeed do
      rnd.nextInt(100) match
        case r if r < 30 => newTransaction(op, holdFor = 0)
        case r if r < 33 =>
          // A dense burst, held live across a full wrap of the ring (about 270 ops)
          for _ <- 1 to 4 + rnd.nextInt(8) do newTransaction(op, holdFor = 400)
        case r if r < 50 =>
          // Another Paxos state, the same updates
          pick(op, deletable = false).foreach: key =>
            val held = txs(key)
            val state = held.state.copy(paxosAcceptorState = paxos(op),
              disposition = TransactionDisposition.VoteCommit)
            txs(key) = held.copy(state = state, savedSinceDrop = held.dropped)
            save(key, state, waitFor = rnd.nextInt(3) == 0)
        case r if r < 58 =>
          // Late data for a data-less transaction, charged only below the limit (A1) and within T
          pick(op, deletable = false).filter(k => txs(k).state.objectUpdates.isEmpty && !txs(k).dropped)
            .foreach: key =>
              val held = txs(key)
              val room = (maxTx - held.charged) / 2 - CrlAdmission.ObjectUpdateOverheadBytes
              if total < limit && room > 0 then
                val state = held.state.copy(objectUpdates = updates(1 + rnd.nextInt(2), room.min(40 * 1024).toInt))
                val added = charge(state) - held.charged
                total += added
                txs(key) = held.copy(state = state, charged = held.charged + added)
                save(key, state, waitFor = rnd.nextInt(3) == 0)
        case r if r < 85 =>
          pick(op, deletable = true).foreach: key =>
            crl.deleteTransaction(key._1, key._2)
            total -= txs(key).charged
            txs -= key
            heldUntil -= key
        case r if r < 89 =>
          pick(op, deletable = false).filter(k => !txs(k).dropped).foreach: key =>
            crl.dropTransactionObjectData(key._1, key._2)
            txs(key) = txs(key).copy(dropped = true)
        case r if r < 93 =>
          // A stub replaces a transaction's state: its charge is released once the stub is durable
          pick(op, deletable = true).foreach: key =>
            val done = new LinkedBlockingQueue[String]()
            crl.saveAppliedStub(key._1, key._2, HLCTimestamp(op), () => done.put(""))
            assert(done.poll(10, TimeUnit.SECONDS) == "", s"seed $seed: a stub was not written")
            total += CrlAdmission.AppliedStubBytes - txs(key).charged
            txs -= key
            heldUntil -= key
            stubs(key) = HLCTimestamp(op)
        case r if r < 96 =>
          if stubs.nonEmpty then
            val key = stubs.keysIterator.drop(rnd.nextInt(stubs.size)).next()
            crl.deleteAppliedStub(key._1, key._2)
            total -= CrlAdmission.AppliedStubBytes
            stubs -= key
        case _ =>
          // An unoffset stub: owed for a transaction this CRL never held, charged without a check
          nextId += 1
          val key = (stores(rnd.nextInt(stores.size)), TransactionId(new UUID(seed, nextId)))
          crl.saveAppliedStub(key._1, key._2, HLCTimestamp(op), () => ())
          total += CrlAdmission.AppliedStubBytes
          stubs(key) = HLCTimestamp(op)
      if op % reopenEvery == 0 then
        reopen()
    reopen()
    crl.shutdown()
