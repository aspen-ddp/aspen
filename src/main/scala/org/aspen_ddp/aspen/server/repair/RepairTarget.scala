package org.aspen_ddp.aspen.server.repair

import org.aspen_ddp.aspen.client.ObjectState as ClientObjectState
import org.aspen_ddp.aspen.common.objects.{ObjectId, ObjectPointer}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.server.store.{ReconcileResult, RepairSnapshot}

import scala.concurrent.Promise

/** Completes a repair operation aimed at a store this host no longer holds.
  *
  * Failing rather than succeeding is deliberate: the store still exists, it just belongs to
  * another host now, so the errorTree entry must survive for that host's sweep to find.
  */
case class StoreNotHosted(storeId: StoreId)
  extends Exception(s"Store $storeId is not hosted here")

/** The operations the repair service needs from a Host.
  *
  * Host is a large class with an event loop and a device-discovery constructor. Depending on
  * this trait instead lets StoreRepairer and RepairService be tested against a recording fake.
  */
trait RepairTarget:

  /** Stores this host currently owns and can write to. Excludes offline stores, stores being
    * rebuilt (already having everything read) and stores transferring out (about to stop being
    * ours).
    */
  def repairableStoreIds: List[StoreId]

  /** O(1) per-entry counterpart to `repairableStoreIds`: whether the given store is currently
    * repairable on this host. Must agree with `repairableStoreIds.contains(storeId)` exactly.
    */
  def isRepairable(storeId: StoreId): Boolean

  /** Whether the store holds transaction `txid` live (recovered or active), or may once the CRL state it is loading
    * is recovered. A TracelessCommits entry for such a transaction is skipped: the store's Tx applies it. Callable
    * from any thread: it reads `@volatile` snapshots, as `Host.hasTransactions` does
    */
  def holdsTransaction(storeId: StoreId, txid: TransactionId): Boolean

  /** Records the store's copy of the object, the first step of a repair (`Reconcile`). Completes `p` with
    * Failure(StoreNotHosted) if the store has left this host.
    */
  def snapshot(storeId: StoreId, pointer: ObjectPointer, p: Promise[RepairSnapshot]): Unit

  /** Makes the store's copy agree with `read`, a full read taken after `snapshot`, deferring what `read.readLocks`
    * reports locked and leaving alone anything changed since `snapshot`. Completes `p` with what was left undone, or
    * with Failure(StoreNotHosted) if the store has left this host.
    */
  def reconcile(storeId: StoreId, snapshot: RepairSnapshot, read: ClientObjectState, p: Promise[ReconcileResult]): Unit

  /** Deletes the store's copy of an object that is gone from the rest of the system. An empty
    * `storePointer` means "delete by ObjectId alone". Completes `completion` with
    * Failure(StoreNotHosted) if the store has left this host.
    */
  def repairDelete(storeId: StoreId, objectId: ObjectId,
                   storePointer: Array[Byte], completion: Promise[Unit]): Unit
