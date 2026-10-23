package org.aspen_ddp.aspen.client.internal.transaction

import org.aspen_ddp.aspen.client.internal.network.Messenger as ClientMessenger
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.objects.ObjectPointer
import org.aspen_ddp.aspen.common.store.StoreId

import scala.util.Random

object DesignatedLeader:

  /** Chooses a random store from the primary object's pool to be a transaction's designated leader. Spreading the
   *  choice spreads the leadership load, and steering it away from unreachable stores avoids a Paxos round lost
   *  to a leader that never answers.
   *
   *  The messenger's knowledge is advisory and may be stale or missing, so the choice falls back in tiers: stores
   *  not offline whose host is known online, then stores not offline, then any store. Any store is correct; a bad
   *  choice only costs latency until another store takes over leadership.
   */
  def choose(messenger: ClientMessenger, pointer: ObjectPointer, ida: IDA, random: Random = Random): Byte =
    val all = (0 until ida.width).map(i => StoreId(pointer.poolId, i.toByte))
    val notOffline = all.filterNot(messenger.isStoreOffline)
    val preferred = notOffline.filter(messenger.isStoreHostOnline)
    val candidates =
      if preferred.nonEmpty then preferred
      else if notOffline.nonEmpty then notOffline
      else all
    candidates(random.nextInt(candidates.size)).poolIndex
