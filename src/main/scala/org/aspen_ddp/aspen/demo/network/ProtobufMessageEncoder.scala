package org.aspen_ddp.aspen.demo.network

import java.nio.{ByteBuffer, ByteOrder}
import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.client.internal.transaction.TransactionBuilder.TransactionData
import org.aspen_ddp.aspen.common.network.*
import org.aspen_ddp.aspen.common.objects.Metadata

object ProtobufMessageEncoder:

  def encodeMessage(msg: HostHeartbeat): Array[Byte] = {
    val arr = codec.Message.newBuilder().setHostHeartbeat(Codec.encode(msg)).build.toByteArray

    val msgarr = new Array[Byte](4 + arr.length)
    val bb = ByteBuffer.wrap(msgarr)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.putInt(arr.length)
    bb.put(arr)
    msgarr
  }

  def encodeMessage(message: TxMessage): Array[Byte] = {

    val builder = codec.Message.newBuilder()

    var updateContent: Option[TransactionData] = None

    message match
      case m: TxPrepare =>
        builder.setPrepare(Codec.encode(m))
        updateContent = Some(TransactionData(m.objectUpdates, m.preTxRebuilds))
      case m: TxPrepareResponse => builder.setPrepareResponse(Codec.encode(m))
      case m: TxAccept => builder.setAccept(Codec.encode(m))
      case m: TxAcceptResponse => builder.setAcceptResponse(Codec.encode(m))
      case m: TxResolved => builder.setResolved(Codec.encode(m))
      case m: TxCommitted => builder.setCommitted(Codec.encode(m))
      case m: TxFinalized => builder.setFinalized(Codec.encode(m))
      case m: TxHeartbeat => builder.setHeartbeat(Codec.encode(m))
      case m: TxStatusRequest => builder.setStatusRequest(Codec.encode(m))
      case m: TxStatusResponse => builder.setStatusResponse(Codec.encode(m))

    val encodedMsg = builder.build.toByteArray

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
    val builder = codec.Message.newBuilder()

    message match
      case m: Read => builder.setRead(Codec.encode(m))
      case m: Allocate => builder.setAllocate(Codec.encode(m))
      case m: OpportunisticRebuild => builder.setOpportunisticRebuild(Codec.encode(m))
      case m: TransactionCompletionQuery => builder.setTransactionCompletionQuery(Codec.encode(m))

    val encodedMsg = builder.build.toByteArray

    val msg = new Array[Byte](4 + encodedMsg.length)
    val bb = ByteBuffer.wrap(msg)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.putInt(encodedMsg.length)
    bb.put(encodedMsg)

    msg


  def encodeMessage(message: ClientResponse): Array[Byte] =
    val builder = codec.Message.newBuilder()

    message match
      case m: ReadResponse => builder.setReadResponse(Codec.encode(m))
      case m: AllocateResponse => builder.setAllocateResponse(Codec.encode(m))
      case m: TransactionCompletionResponse => builder.setTransactionCompletionResponse(Codec.encode(m))
      case m: TransactionFinalized => builder.setTxFinalized(Codec.encode(m))
      case m: TransactionResolved => builder.setTxResolved(Codec.encode(m))

    val encodedMsg = builder.build.toByteArray

    val msg = new Array[Byte](4 + encodedMsg.length)
    val bb = ByteBuffer.wrap(msg)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.putInt(encodedMsg.length)
    bb.put(encodedMsg)

    msg
