package org.aspen_ddp.aspen.common.rebalancing

import org.aspen_ddp.aspen.common.metadata.StorageDeviceSetId
import org.aspen_ddp.aspen.common.objects.Key
import org.aspen_ddp.aspen.common.util.{uuid2byte, byte2uuid}

/** The minimal KV state object for a SetRebalanceDurableTask: it stores only the setId under
 *  SetIdKey. All rebalancing progress lives in the set's pendingTransfers list; this object
 *  exists to satisfy the DurableTask taskPointer contract and to be the refcount-tracked
 *  anchor the service points at. */
object SetRebalanceTaskState:
  val SetIdKey: Key = Key(Array[Byte](0))

  def initialContent(setId: StorageDeviceSetId): Map[Key, Array[Byte]] =
    Map(SetIdKey -> uuid2byte(setId.uuid))

  def decodeSetId(bytes: Array[Byte]): StorageDeviceSetId =
    StorageDeviceSetId(byte2uuid(bytes))
