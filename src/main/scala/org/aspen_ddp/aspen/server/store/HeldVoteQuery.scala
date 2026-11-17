package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.common.network.HeldVote
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

object HeldVoteQuery:
  /** For tests and stores without a host: never answers */
  val Never: HeldVoteQuery = (_, _) => ()
