package org.aspen_ddp.aspen.common.eventlog

import com.google.protobuf.ByteString
import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.client.ObjectAllocatorId
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.network.Codec
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Key}
import org.aspen_ddp.aspen.common.util.{byte2long, long2byte}

import java.util.UUID

/** Keys and value codecs for the EventLogDurableService's KV state object.
 *
 *  IncomingKey and LoggedKey hold encoded tree Roots (KV and data-object trees respectively). */
object EventLogState:

  val IncomingKey: Key = Key(Array[Byte](0))
  val LoggedKey: Key = Key(Array[Byte](1))
  val TypeIdsKey: Key = Key(Array[Byte](2))
  val ObserversKey: Key = Key(Array[Byte](3))
  val AllocatorKey: Key = Key(Array[Byte](4))
  val TailsKey: Key = Key(Array[Byte](5))
  val TailFloorKey: Key = Key(Array[Byte](6))

  /** Number of tail objects kept in the Tails value */
  val TailCount = 10

  /** Event type UUID to its compact 16-bit id (read as unsigned) */
  type TypeIds = Map[UUID, Short]

  /** An observer factory type and its opaque configuration */
  final case class ObserverConfig(factoryType: UUID, config: Array[Byte]):
    override def equals(other: Any): Boolean = other match
      case o: ObserverConfig => factoryType == o.factoryType && java.util.Arrays.equals(config, o.config)
      case _ => false
    override def hashCode: Int = factoryType.hashCode * 31 + java.util.Arrays.hashCode(config)

  /** The tail objects, the index of the one currently written, and the allocator they came from */
  final case class Tails(pointers: Vector[DataObjectPointer], current: Int, allocator: ObjectAllocatorId)

  def encodeTypeIds(ids: TypeIds): Array[Byte] =
    codec.EventLogTypeIds(entries = ids.toSeq.map((uuid, id) =>
      codec.EventLogTypeIdEntry(typeUuid = Some(Codec.encodeUUID(uuid)), typeId = id & 0xFFFF))).toByteArray

  def decodeTypeIds(bytes: Array[Byte]): TypeIds =
    codec.EventLogTypeIds.parseFrom(bytes).entries.map(e =>
      Codec.decodeUUID(e.typeUuid.get) -> e.typeId.toShort).toMap

  def encodeObservers(observers: List[ObserverConfig]): Array[Byte] =
    codec.EventLogObservers(observers = observers.map(o =>
      codec.EventLogObserverConfig(
        factoryType = Some(Codec.encodeUUID(o.factoryType)),
        config = ByteString.copyFrom(o.config)))).toByteArray

  def decodeObservers(bytes: Array[Byte]): List[ObserverConfig] =
    codec.EventLogObservers.parseFrom(bytes).observers.map(o =>
      ObserverConfig(Codec.decodeUUID(o.factoryType.get), o.config.toByteArray)).toList

  def encodeAllocator(id: ObjectAllocatorId): Array[Byte] = id.toArray

  def decodeAllocator(bytes: Array[Byte]): ObjectAllocatorId = ObjectAllocatorId(bytes)

  def encodeTails(tails: Tails): Array[Byte] =
    codec.EventLogTails(
      pointers = tails.pointers.map(p => ByteString.copyFrom(p.toArray)),
      current = tails.current,
      allocator = ByteString.copyFrom(tails.allocator.toArray)).toByteArray

  def decodeTails(bytes: Array[Byte]): Tails =
    val t = codec.EventLogTails.parseFrom(bytes)
    val pointers = t.pointers.map(b => DataObjectPointer(b.toByteArray)).toVector
    if t.current < 0 || t.current >= pointers.size then
      throw new IllegalArgumentException(s"Event log tails: current index ${t.current} is not one of ${pointers.size}")
    Tails(pointers, t.current, ObjectAllocatorId(t.allocator.toByteArray))

  def encodeTailFloor(ts: HLCTimestamp): Array[Byte] = long2byte(ts.asLong)

  def decodeTailFloor(bytes: Array[Byte]): HLCTimestamp = HLCTimestamp(byte2long(bytes))
