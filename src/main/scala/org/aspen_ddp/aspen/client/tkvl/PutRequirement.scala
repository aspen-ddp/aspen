package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.common.objects.ObjectRevision

/** What a TKVL put requires of its key's prior state.
  *
  * On a KeyValue-object tree each case is a per-key requirement on the node, derived from the node as read, so it
  * adds no node-level serialization. On a DataObject tree every rewrite already requires the node's revision, so
  * `Checked` and `Blind` add nothing and `AtRevision` is unsupported.
  */
enum PutRequirement:
  /** The default: `KeyRevision(k, rev)` if the node read holds the key, `DoesNotExist(k)` if not. A competing write
    * of the key aborts the transaction as an ordinary abort, which the caller's retry handles.
    */
  case Checked
  /** `DoesNotExist(k)`. Fails with `KeyAlreadyExists` if the node read holds the key */
  case MustNotExist
  /** `KeyRevision(k, rev)` */
  case AtRevision(rev: ObjectRevision)
  /** No key requirement. For idempotent upserts only: the put wins over any concurrent write of the key */
  case Blind
