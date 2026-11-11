package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.common.objects.{Key, ObjectRevision, Value}

/** One insert of a batch update (`TieredKeyValueList.prepareUpdates`, `DataObjectTKVL.prepareUpdates`).
  *
  * By default the insert is checked (`PutRequirement.Checked`): a competing write of `key` aborts the transaction.
  *
  * With `requireDoesNotExist`, the transaction fails with `KeyAlreadyExists` if the tree already holds `key`. That
  * makes the common allocate-an-object-and-store-its-pointer pattern safe to retry: a retry of a transaction that did
  * commit fails instead of storing a second pointer.
  *
  * With `blind`, the insert has no key requirement (`PutRequirement.Blind`), for idempotent upserts only.
  *
  * With `requiredRevision`, the key must be held at that revision (`PutRequirement.AtRevision`, KeyValue-object trees
  * only).
  */
final case class TKVLInsert(key: Key,
                            value: Value,
                            requireDoesNotExist: Boolean = false,
                            blind: Boolean = false,
                            requiredRevision: Option[ObjectRevision] = None):
  require(List(requireDoesNotExist, blind, requiredRevision.isDefined).count(identity) <= 1,
    "A TKVLInsert takes at most one of requireDoesNotExist, blind and requiredRevision")

  def requirement: PutRequirement =
    if requireDoesNotExist then PutRequirement.MustNotExist
    else if blind then PutRequirement.Blind
    else requiredRevision.fold(PutRequirement.Checked)(PutRequirement.AtRevision(_))
