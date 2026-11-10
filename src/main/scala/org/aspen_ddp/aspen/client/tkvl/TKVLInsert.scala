package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.common.objects.{Key, Value}

/** One insert of a batch update (`TieredKeyValueList.prepareUpdates`, `DataObjectTKVL.prepareUpdates`).
  *
  * By default the insert is checked (`PutRequirement.Checked`): a competing write of `key` aborts the transaction.
  *
  * With `requireDoesNotExist`, the transaction fails with `KeyAlreadyExists` if the tree already holds `key`. That
  * makes the common allocate-an-object-and-store-its-pointer pattern safe to retry: a retry of a transaction that did
  * commit fails instead of storing a second pointer.
  *
  * With `blind`, the insert has no key requirement (`PutRequirement.Blind`), for idempotent upserts only.
  */
final case class TKVLInsert(key: Key, value: Value, requireDoesNotExist: Boolean = false, blind: Boolean = false):
  require(!(requireDoesNotExist && blind), "A TKVLInsert cannot both require its key not exist and be blind")

  def requirement: PutRequirement =
    if requireDoesNotExist then PutRequirement.MustNotExist
    else if blind then PutRequirement.Blind
    else PutRequirement.Checked
