package org.aspen_ddp.aspen.common.rebalancing

import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceSetId}
import org.aspen_ddp.aspen.common.network.Codec
import org.aspen_ddp.aspen.common.store.StoreId

/** Payload types carried in the `encodedContent` of a ServiceMessage addressed to the
 *  RebalancingDurableService. Best-effort wake-ups; correctness comes from polling. */
sealed trait RebalancingMessage

case class NewSetRebalanceInitiated(setId: StorageDeviceSetId) extends RebalancingMessage

case class TransferComplete(setId: StorageDeviceSetId,
                            storeId: StoreId,
                            fromDevice: StorageDeviceId,
                            toDevice: StorageDeviceId) extends RebalancingMessage

object RebalancingMessage:

  def encode(m: RebalancingMessage): Array[Byte] =
    val proto = m match
      case NewSetRebalanceInitiated(setId) =>
        codec.RebalancingMessage(codec.RebalancingMessage.Msg.NewSetRebalanceInitiated(
          codec.NewSetRebalanceInitiated(setId = Some(Codec.encodeUUID(setId.uuid)))))
      case TransferComplete(setId, storeId, from, to) =>
        codec.RebalancingMessage(codec.RebalancingMessage.Msg.TransferComplete(
          codec.TransferComplete(
            setId = Some(Codec.encodeUUID(setId.uuid)),
            storeId = Some(Codec.encode(storeId)),
            fromDevice = Some(Codec.encode(from)),
            toDevice = Some(Codec.encode(to)))))
    proto.toByteArray

  def decode(bytes: Array[Byte]): RebalancingMessage =
    val proto = codec.RebalancingMessage.parseFrom(bytes)
    proto.msg match
      case codec.RebalancingMessage.Msg.NewSetRebalanceInitiated(m) =>
        NewSetRebalanceInitiated(StorageDeviceSetId(Codec.decodeUUID(m.setId.get)))
      case codec.RebalancingMessage.Msg.TransferComplete(m) =>
        TransferComplete(
          StorageDeviceSetId(Codec.decodeUUID(m.setId.get)),
          Codec.decode(m.storeId.get),
          Codec.decode(m.fromDevice.get),
          Codec.decode(m.toDevice.get))
      case codec.RebalancingMessage.Msg.Empty =>
        throw new IllegalArgumentException("Empty RebalancingMessage")
