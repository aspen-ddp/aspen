package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.common.objects.{Key, Value}

/** One insert of a batch update (`TieredKeyValueList.prepareUpdates`, `DataObjectTKVL.prepareUpdates`).
  *
  * With `requireDoesNotExist`, the transaction fails with `KeyAlreadyExists` if the tree already holds `key`. That
  * makes the common allocate-an-object-and-store-its-pointer pattern safe to retry: a retry of a transaction that did
  * commit fails instead of storing a second pointer.
  */
final case class TKVLInsert(key: Key, value: Value, requireDoesNotExist: Boolean = false)
