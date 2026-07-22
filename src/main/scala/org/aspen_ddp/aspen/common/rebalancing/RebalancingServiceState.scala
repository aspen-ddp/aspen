package org.aspen_ddp.aspen.common.rebalancing

import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.common.metadata.StorageDeviceSetId
import org.aspen_ddp.aspen.common.network.Codec
import org.aspen_ddp.aspen.common.objects.{Key, KeyValueObjectPointer}

/** Helpers for the RebalancingDurableService's KV state object. The single
 *  ActiveTasksKey holds the encoded ActiveRebalancingTasks list: one
 *  (setId, taskStatePointer) entry per in-flight set rebalance. */
object RebalancingServiceState:

  val ActiveTasksKey: Key = Key(Array[Byte](0))

  type ActiveTasks = List[(StorageDeviceSetId, KeyValueObjectPointer)]

  def encodeActiveTasks(tasks: ActiveTasks): Array[Byte] =
    codec.ActiveRebalancingTasks(tasks = tasks.map(Codec.encode)).toByteArray

  def decodeActiveTasks(bytes: Array[Byte]): ActiveTasks =
    codec.ActiveRebalancingTasks.parseFrom(bytes).tasks.map(Codec.decodeActiveRebalancingTask).toList
