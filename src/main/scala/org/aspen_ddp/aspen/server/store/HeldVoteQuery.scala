package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.common.network.HeldVote
import org.aspen_ddp.aspen.common.objects.ObjectId
import org.aspen_ddp.aspen.common.store.StoreId

import scala.util.Try

/** Asks the other stores of `storeId`'s pool for the votes they hold (CurrentAllocationsRequest, HeldVote) and gives
  * `answer` the union of the replies once `width - writeThreshold + 1` of them have answered (all of them, if fewer
  * stores exist), or a Failure. `answer` may be called from any thread: the Frontend posts it to its own executor
  * (Frontend.post).
  *
  * Any transaction that can still commit with a commit vote of this store's lost incarnation counted needs
  * `writeThreshold - 1` live commit voters among the other stores, and that many replies include one of them. A
  * rebuilt store uses it to learn which transactions its inherited votes still back (Frontend.inheritedVoteLock).
  *
  * Stores have no client, so the Host supplies this (Host.heldVoteQuery)
  */
trait HeldVoteQuery:
  def query(storeId: StoreId, answer: Try[List[HeldVote]] => Unit): Unit

  /** A rebuilt store's inherited-growth scan (Frontend.inheritedGrowthLocks): marks each object of `mark` in the
    * ErrorTree against the store, then answers every object with an ErrorTree or UnackedCommits entry against it
    * (StoreRepairer.inheritedGrowthScan), or a Failure. The default answers no objects at once: for tests and stores
    * without a host, which have no repair either. The Host's runs the real scan
    */
  def growthScan(storeId: StoreId, mark: Set[ObjectId], answer: Try[Set[ObjectId]] => Unit): Unit =
    answer(scala.util.Success(Set()))

object HeldVoteQuery:
  /** For tests and stores without a host: never answers */
  val Never: HeldVoteQuery = (_, _) => ()
