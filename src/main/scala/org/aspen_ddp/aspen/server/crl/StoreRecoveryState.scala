package org.aspen_ddp.aspen.server.crl

import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.transaction.TransactionId

/** What a CRL holds for one store: its transactions' recovery state, and its applied stubs.
  *
  * An applied stub, (store, transaction) "applied", records that the store already holds the transaction's update, so
  * an UnackedCommits entry naming the store for it is stale. It carries no description, object data, Paxos state or
  * requirements, and is never recovered as a transaction. A stub and a transaction's state are separate entries: a
  * stub's save replaces the transaction's state, but a later save of the transaction leaves the stub, and only
  * deleteAppliedStub, deleteTransactions or dropStore remove it. Each stub carries its transaction's start timestamp:
  * its store drops it once the transaction is past the horizon and the expiry margin (Frontend.expireStubs).
  */
final case class StoreRecoveryState(transactions: List[TransactionRecoveryState] = Nil,
                                    appliedStubs: Map[TransactionId, HLCTimestamp] = Map()):
  def isEmpty: Boolean = transactions.isEmpty && appliedStubs.isEmpty

  /** The ids of every entry, transactions' and stubs': what a deletion of all of it names */
  def entryIds: List[TransactionId] = (transactions.map(_.txd.transactionId) ++ appliedStubs.keys).distinct

object StoreRecoveryState:
  val Empty: StoreRecoveryState = StoreRecoveryState()
