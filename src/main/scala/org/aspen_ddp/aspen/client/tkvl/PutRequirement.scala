package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.common.objects.ObjectRevision

/** What a TKVL put requires of its key's prior state.
  *
  * On a KeyValue-object tree each case is a per-key requirement on the node, derived from the node as read, so it
  * adds no node-level serialization. On a DataObject tree every rewrite already requires the node's revision, so
  * `Checked` adds nothing and `AtRevision` is unsupported. There is no unconditioned put: stores refuse an Insert
  * that checks nothing of its key (transaction-requirements 2.5).
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
