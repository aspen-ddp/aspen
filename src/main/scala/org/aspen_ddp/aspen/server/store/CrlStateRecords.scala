package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.common.objects.ObjectId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.server.crl.CrashRecoveryLog
import org.aspen_ddp.aspen.server.transaction.Tx

/** The transactions whose CRL state may still hold an update of some object here, by txid and by object. The Frontend
  * keeps two: allocations (crash-recovery-log 3.4) and blind key Inserts (transaction-requirements 9.4). A deletion
  * that meets an entry waits until that state durably holds no update of the object (`Frontend.awaitAllocationsDurable`,
  * `awaitBlindInsertsDurable`). Entries are kept after their Tx is forgotten, until the state's deletion or stub is
  * durable. Empty for every object no recent write covers. Owned by the store's executor.
  *
  * `Record.tx` is the transaction's latest Tx. `Record.concluded`: its state's deletion or stub is queued in the CRL,
  * not yet known durable
  */
final class CrlStateRecords(storeId: StoreId, crl: CrashRecoveryLog, post: (() => Unit) => Unit):
  final class Record(var tx: Tx, var objects: Set[ObjectId], var concluded: Boolean)

  private val records = scala.collection.mutable.HashMap[TransactionId, Record]()
  private val byObject = scala.collection.mutable.HashMap[ObjectId, Set[TransactionId]]()

  def get(txid: TransactionId): Option[Record] = records.get(txid)

  /** The transactions covering `oid` */
  def covering(oid: ObjectId): Set[TransactionId] = byObject.getOrElse(oid, Set())

  /** Covers `oids` by `tx`, a later Tx of an already registered transaction replacing its earlier one */
  def register(tx: Tx, oids: Set[ObjectId]): Unit =
    if oids.nonEmpty then
      val txid = tx.transactionId
      records.get(txid) match
        case Some(r) =>
          r.tx = tx
          r.concluded = false
          r.objects ++= oids
        case None => records(txid) = new Record(tx, oids, false)
      oids.foreach(oid => byObject(oid) = byObject.getOrElse(oid, Set()) + txid)

  private def unindex(txid: TransactionId, oids: Set[ObjectId]): Unit =
    oids.foreach: oid =>
      val rest = byObject.getOrElse(oid, Set()) - txid
      if rest.isEmpty then byObject.remove(oid) else byObject(oid) = rest

  /** `txid`'s CRL state was deleted or replaced by a stub: no save of it may follow. Its record is dropped once that is
    * durable, unless a newer Tx registered it again meanwhile
    */
  def concluded(txid: TransactionId): Unit =
    records.get(txid).foreach: r =>
      r.concluded = true
      crl.whenDurable(storeId, () => post { () =>
        records.get(txid).filter(_ eq r).filter(_.concluded).foreach: r =>
          records.remove(txid)
          unindex(txid, r.objects)
      })

  /** The update of `oid` is gone from `txid`'s state for good: later deletions need nothing from it */
  def dropObject(txid: TransactionId, r: Record, oid: ObjectId): Unit =
    r.objects -= oid
    unindex(txid, Set(oid))
    if r.objects.isEmpty then records.remove(txid)
