package org.aspen_ddp.aspen.server.crl

import org.aspen_ddp.aspen.common.transaction.TransactionId

/** What a CRL holds for one store: its transactions' recovery state, and its applied stubs.
  *
  * An applied stub, (store, transaction) "applied", records that the store already holds the transaction's update, so
  * an UnackedCommits entry naming the store for it is stale. It carries no description, object data, Paxos state or
  * requirements, and is never recovered as a transaction. A stub and a transaction's state are separate entries: a
  * stub's save replaces the transaction's state, but a later save of the transaction leaves the stub, and only
  * deleteAppliedStub, deleteTransactions or dropStore remove it.
  */
final case class StoreRecoveryState(transactions: List[TransactionRecoveryState] = Nil,
                                    appliedStubs: Set[TransactionId] = Set()):
  def isEmpty: Boolean = transactions.isEmpty && appliedStubs.isEmpty

  /** The ids of every entry, transactions' and stubs': what a deletion of all of it names */
  def entryIds: List[TransactionId] = (transactions.map(_.txd.transactionId) ++ appliedStubs).distinct

  /** This state with `newer`'s entries added, in place of any of its own with the same ids */
  def supersededBy(newer: StoreRecoveryState): StoreRecoveryState =
    val ids = newer.entryIds.toSet
    StoreRecoveryState(transactions.filterNot(t => ids.contains(t.txd.transactionId)) ++ newer.transactions,
      appliedStubs.filterNot(ids.contains) ++ newer.appliedStubs)

object StoreRecoveryState:
  val Empty: StoreRecoveryState = StoreRecoveryState()
