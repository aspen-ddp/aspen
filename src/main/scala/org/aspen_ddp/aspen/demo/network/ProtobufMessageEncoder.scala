package org.aspen_ddp.aspen.demo.network

import java.nio.{ByteBuffer, ByteOrder}
import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.client.internal.transaction.TransactionBuilder.TransactionData
import org.aspen_ddp.aspen.common.network.*
import org.aspen_ddp.aspen.common.objects.Metadata

object ProtobufMessageEncoder:

  def encodeMessage(msg: HostHeartbeat): Array[Byte] = {
    val arr = codec.Message(msg = codec.Message.Msg.HostHeartbeat(Codec.encode(msg))).toByteArray

    val msgarr = new Array[Byte](4 + arr.length)
    val bb = ByteBuffer.wrap(msgarr)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.putInt(arr.length)
    bb.put(arr)
    msgarr
  }

  def encodeMessage(message: TxMessage): Array[Byte] = {

    var updateContent: Option[TransactionData] = None

    val msgVariant: codec.Message.Msg = message match
      case m: TxPrepare =>
        updateContent = Some(TransactionData(m.objectUpdates, m.preTxRebuilds))
        codec.Message.Msg.Prepare(Codec.encode(m))
      case m: TxPrepareResponse => codec.Message.Msg.PrepareResponse(Codec.encode(m))
      case m: TxAccept => codec.Message.Msg.Accept(Codec.encode(m))
      case m: TxAcceptResponse => codec.Message.Msg.AcceptResponse(Codec.encode(m))
      case m: TxResolved => codec.Message.Msg.Resolved(Codec.encode(m))
      case m: TxCommitted => codec.Message.Msg.Committed(Codec.encode(m))
      case m: TxFinalized => codec.Message.Msg.Finalized(Codec.encode(m))
      case m: TxHeartbeat => codec.Message.Msg.Heartbeat(Codec.encode(m))
      case m: TxStatusRequest => codec.Message.Msg.StatusRequest(Codec.encode(m))
      case m: TxStatusResponse => codec.Message.Msg.StatusResponse(Codec.encode(m))
      case m: TxUnknownStore => codec.Message.Msg.UnknownStore(Codec.encode(m))

    val encodedMsg = codec.Message(msg = msgVariant).toByteArray

    val (contentSize, preTxSize) = updateContent match
      case None => (0, 0)
      case Some(td) =>
        val luSize = td.localUpdates.foldLeft(0)((sz, lu) => sz + 16 + 4 + lu.data.size)
        val lpSize = td.preTransactionRebuilds.foldLeft(0)((sz, p) => sz + 16 + Metadata.EncodedSize + 4 + p.data.size)

        (luSize, lpSize)

    val msg = new Array[Byte](4 + encodedMsg.length + 4 + 4 + contentSize + preTxSize)
    val bb = ByteBuffer.wrap(msg)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.putInt(encodedMsg.length)
    bb.put(encodedMsg)
    bb.putInt(contentSize)
    bb.putInt(preTxSize)

    updateContent.foreach: td =>

      td.localUpdates.foreach: lu =>
        bb.putLong(lu.objectId.uuid.getMostSignificantBits)
        bb.putLong(lu.objectId.uuid.getLeastSignificantBits)
        bb.putInt(lu.data.size)
        bb.put(lu.data.asReadOnlyBuffer())

      td.preTransactionRebuilds.foreach: pt =>
        bb.putLong(pt.objectId.uuid.getMostSignificantBits)
        bb.putLong(pt.objectId.uuid.getLeastSignificantBits)
        pt.requiredMetadata.encodeInto(bb)
        bb.putInt(pt.data.size)
        bb.put(pt.data.asReadOnlyBuffer())
    msg
  }

  def encodeMessage(message: ClientRequest): Array[Byte] =
    val msgVariant: codec.Message.Msg = message match
      case m: Read => codec.Message.Msg.Read(Codec.encode(m))
      case m: OpportunisticRebuild => codec.Message.Msg.OpportunisticRebuild(Codec.encode(m))
      case m: TransactionCompletionQuery => codec.Message.Msg.TransactionCompletionQuery(Codec.encode(m))

    val encodedMsg = codec.Message(msg = msgVariant).toByteArray

    val msg = new Array[Byte](4 + encodedMsg.length)
    val bb = ByteBuffer.wrap(msg)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.putInt(encodedMsg.length)
    bb.put(encodedMsg)

    msg


  def encodeMessage(message: ClientResponse): Array[Byte] =
    val msgVariant: codec.Message.Msg = message match
      case m: ReadResponse => codec.Message.Msg.ReadResponse(Codec.encode(m))
      case m: TransactionCompletionResponse => codec.Message.Msg.TransactionCompletionResponse(Codec.encode(m))
      case m: TransactionFinalized => codec.Message.Msg.TxFinalized(Codec.encode(m))
      case m: TransactionResolved => codec.Message.Msg.TxResolved(Codec.encode(m))

    val encodedMsg = codec.Message(msg = msgVariant).toByteArray

    val msg = new Array[Byte](4 + encodedMsg.length)
    val bb = ByteBuffer.wrap(msg)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.putInt(encodedMsg.length)
    bb.put(encodedMsg)

    msg

  def encodeMessage(message: HostMessage): Array[Byte] =
    val msgVariant: codec.Message.Msg = message match
      case m: StartStoreTransfer => codec.Message.Msg.StartStoreTransfer(Codec.encode(m))
      case m: StoreTransferData => codec.Message.Msg.StoreTransferData(Codec.encode(m))
      case m: CheckStorageDevice => codec.Message.Msg.CheckStorageDevice(Codec.encode(m))

    val encodedMsg = codec.Message(msg = msgVariant).toByteArray

    val msg = new Array[Byte](4 + encodedMsg.length)
    val bb = ByteBuffer.wrap(msg)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.putInt(encodedMsg.length)
    bb.put(encodedMsg)

    msg
