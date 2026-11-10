package org.aspen_ddp.aspen.compute.systemtask

import org.aspen_ddp.aspen.client.{AspenClient, Transaction}
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, PutRequirement, TieredKeyValueList}
import org.aspen_ddp.aspen.common.objects.{Key, KeyValueObjectPointer, Value}
import org.aspen_ddp.aspen.common.util.{byte2uuid, uuid2byte}

import java.util.UUID
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}

/** Helpers over the SystemTaskExecutorService task TKVL, hosted in the service state object
 *  under `SystemTaskExecutorService.TaskTreeKey`. Key = task UUID, Value = encoded pointer to
 *  the task's durable state object. */
object SystemTaskServiceState:

  private def tkvl(client: AspenClient, statePtr: KeyValueObjectPointer): TieredKeyValueList =
    TieredKeyValueList(client, KVObjectRootManager(client, SystemTaskExecutorService.TaskTreeKey, statePtr))

  /** Insert a task into the registry. A checked insert: a concurrent enroll of the same key aborts this one, which
   *  the retry re-reads. The insert requires the revision of the tree node it goes into
   *  (`TieredKeyValueList.prepareSetPinned`), which pins the transaction: a replay after any later change to the node
   *  fails instead of enrolling the task again.
   */
  def enroll(client: AspenClient,
             statePtr: KeyValueObjectPointer,
             taskId: UUID,
             taskStatePtr: KeyValueObjectPointer): Future[Unit] =
    given ExecutionContext = client.clientContext
    client.transactUntilSuccessful: tx =>
      given Transaction = tx
      tkvl(client, statePtr).prepareSetPinned(Key(uuid2byte(taskId)), Value(taskStatePtr.toArray), PutRequirement.Checked)

  /** Insert a task into the registry within an existing transaction, whose task state object is typically allocated
   *  by that transaction. A checked insert, which is not a pin. With `pinned` it also requires the revision of the
   *  tree node it goes into (`TieredKeyValueList.prepareSetPinned`), for transactions with no other pin; that
   *  serializes every pinned enrollment on the node, so transactions pinned otherwise (FSIF index releases) leave it
   *  false.
   */
  def enrollInTx(client: AspenClient,
                 statePtr: KeyValueObjectPointer,
                 taskId: UUID,
                 taskStatePtr: KeyValueObjectPointer,
                 pinned: Boolean = false)(using tx: Transaction): Future[Unit] =
    val tree = tkvl(client, statePtr)
    val key = Key(uuid2byte(taskId))
    if pinned then tree.prepareSetPinned(key, Value(taskStatePtr.toArray), PutRequirement.Checked)
    else tree.set(key, Value(taskStatePtr.toArray))

  /** Read all enrolled (taskId, taskStatePointer) pairs. */
  def scan(client: AspenClient,
           statePtr: KeyValueObjectPointer): Future[List[(UUID, KeyValueObjectPointer)]] =
    given ExecutionContext = client.clientContext
    val buf = ListBuffer[(UUID, KeyValueObjectPointer)]()
    tkvl(client, statePtr).foreach: (_, key, vs) =>
      buf += ((byte2uuid(key.bytes), KeyValueObjectPointer(vs.value.bytes)))
      Future.unit
    .map(_ => buf.toList)

  /** Remove a task from the registry. Idempotent: a no-op if the key is already gone. The delete requires the key's
   *  revision as read, which pins the transaction.
   */
  def remove(client: AspenClient,
             statePtr: KeyValueObjectPointer,
             taskId: UUID): Future[Unit] =
    given ExecutionContext = client.clientContext
    client.transactUntilSuccessful: tx =>
      given Transaction = tx
      tkvl(client, statePtr).delete(Key(uuid2byte(taskId)))

  /** Delete a task's registry entry within an existing transaction. */
  def removeInTx(client: AspenClient,
                 statePtr: KeyValueObjectPointer,
                 taskId: UUID)(using tx: Transaction): Future[Unit] =
    tkvl(client, statePtr).delete(Key(uuid2byte(taskId)))
