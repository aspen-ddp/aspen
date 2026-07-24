package org.aspen_ddp.aspen.compute.systemtask

import org.aspen_ddp.aspen.client.{AspenClient, Transaction}
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
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

  /** Insert a task into the registry. Idempotent: a re-enroll of the same key overwrites the
   *  value (the pointer is stable for a given task UUID). */
  def enroll(client: AspenClient,
             statePtr: KeyValueObjectPointer,
             taskId: UUID,
             taskStatePtr: KeyValueObjectPointer): Future[Unit] =
    given ExecutionContext = client.clientContext
    client.transactUntilSuccessful: tx =>
      given Transaction = tx
      tkvl(client, statePtr).set(Key(uuid2byte(taskId)), Value(taskStatePtr.toArray))

  /** Insert a task into the registry within an existing transaction. Idempotent: a re-enroll
   *  of the same key overwrites the value (the pointer is stable for a given task UUID). */
  def enrollInTx(client: AspenClient,
                 statePtr: KeyValueObjectPointer,
                 taskId: UUID,
                 taskStatePtr: KeyValueObjectPointer)(using tx: Transaction): Future[Unit] =
    tkvl(client, statePtr).set(Key(uuid2byte(taskId)), Value(taskStatePtr.toArray))

  /** Read all enrolled (taskId, taskStatePointer) pairs. */
  def scan(client: AspenClient,
           statePtr: KeyValueObjectPointer): Future[List[(UUID, KeyValueObjectPointer)]] =
    given ExecutionContext = client.clientContext
    val buf = ListBuffer[(UUID, KeyValueObjectPointer)]()
    tkvl(client, statePtr).foreach: (_, key, vs) =>
      buf += ((byte2uuid(key.bytes), KeyValueObjectPointer(vs.value.bytes)))
      Future.unit
    .map(_ => buf.toList)

  /** Remove a task from the registry. Idempotent: a no-op if the key is already gone. */
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
