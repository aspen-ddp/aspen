package org.aspen_ddp.aspen.common.rebalancing

import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.metadata.StorageDeviceSetId
import org.aspen_ddp.aspen.common.network.Codec
import org.aspen_ddp.aspen.common.objects.{Key, KeyValueObjectPointer}
import org.aspen_ddp.aspen.common.util.{byte2long, long2byte}

import scala.concurrent.duration.{Duration, MINUTES}

/** Helpers for the RebalancingDurableService's KV state object.
 *
 *  ActiveTasksKey holds the encoded ActiveRebalancingTasks list: one
 *  (setId, taskStatePointer) entry per in-flight set rebalance.
 *
 *  AutoRebalancePeriodKey and LastAutoRebalanceKey drive the automatic sweep: how often to
 *  plan every level-0 set, and when the last sweep finished. Both are bare scalars rather
 *  than protobuf messages, matching SetRebalanceTaskState's plain-bytes UUID. */
object RebalancingServiceState:

  val ActiveTasksKey: Key = Key(Array[Byte](0))

  /** The interval between automatic rebalance sweeps, stored as whole minutes.
   *  Zero disables automatic rebalancing. */
  val AutoRebalancePeriodKey: Key = Key(Array[Byte](1))

  /** HLCTimestamp of the last *completed* automatic sweep. Written only by a sweep, never
   *  by a period change, so shortening the period can make a sweep immediately due. */
  val LastAutoRebalanceKey: Key = Key(Array[Byte](2))

  type ActiveTasks = List[(StorageDeviceSetId, KeyValueObjectPointer)]

  def encodeActiveTasks(tasks: ActiveTasks): Array[Byte] =
    codec.ActiveRebalancingTasks(tasks = tasks.map(Codec.encode)).toByteArray

  def decodeActiveTasks(bytes: Array[Byte]): ActiveTasks =
    codec.ActiveRebalancingTasks.parseFrom(bytes).tasks.map(Codec.decodeActiveRebalancingTask).toList

  def encodeAutoRebalancePeriod(period: Duration): Array[Byte] = long2byte(period.toMinutes)

  def decodeAutoRebalancePeriod(bytes: Array[Byte]): Duration = Duration(byte2long(bytes), MINUTES)

  def encodeLastAutoRebalance(ts: HLCTimestamp): Array[Byte] = long2byte(ts.asLong)

  def decodeLastAutoRebalance(bytes: Array[Byte]): HLCTimestamp = HLCTimestamp(byte2long(bytes))
