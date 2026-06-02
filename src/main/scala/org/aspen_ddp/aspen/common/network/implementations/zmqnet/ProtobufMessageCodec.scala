package org.aspen_ddp.aspen.common.network.implementations.zmqnet

import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID
import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.client.internal.transaction.TransactionBuilder.TransactionData
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.network.*
import org.aspen_ddp.aspen.common.objects.{Metadata, ObjectId}
import org.aspen_ddp.aspen.common.transaction.{ObjectUpdate, PreTransactionOpportunisticRebuild}
import scribe.Logging

object ProtobufMessageCodec extends Logging:

  // ============================================================================
  // Encoding Methods
  // ============================================================================

  def encodeHeartbeat(msg: HostHeartbeat): Array[Byte] =
    val arr = codec.Message(msg = codec.Message.Msg.HostHeartbeat(Codec.encode(msg))).toByteArray

    val msgarr = new Array[Byte](4 + arr.length)
    val bb = ByteBuffer.wrap(msgarr)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.putInt(arr.length)
    bb.put(arr)
    msgarr

  def encodeTxMessage(message: TxMessage): Array[Byte] =
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

  def encodeClientRequest(message: ClientRequest): Array[Byte] =
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

  def encodeClientResponse(message: ClientResponse): Array[Byte] =
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

  def encodeHostMessage(message: HostMessage): Array[Byte] =
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

  /** Unified encode dispatcher - pattern matches on Message type and delegates to type-specific encoder */
  def encodeMessage(msg: Message): Array[Byte] = msg match
    case m: HostHeartbeat => encodeHeartbeat(m)
    case m: TxMessage => encodeTxMessage(m)
    case m: ClientRequest => encodeClientRequest(m)
    case m: ClientResponse => encodeClientResponse(m)
    case m: HostMessage => encodeHostMessage(m)

  // ============================================================================
  // Decoding Methods
  // ============================================================================

  /** Decodes a raw message array into a Message instance.
    *
    * Returns None for empty or malformed messages.
    */
  def decodeMessage(rawMsg: Array[Byte]): Option[Message] =
    if rawMsg.isEmpty then
      return None

    val bb = ByteBuffer.wrap(rawMsg)
    bb.order(ByteOrder.BIG_ENDIAN)

    if bb.remaining() < 4 then
      logger.error("Message too short - missing length prefix")
      return None

    val msgLen = bb.getInt()
    if bb.remaining() < msgLen then
      logger.error(s"Message too short - expected $msgLen bytes, got ${bb.remaining()}")
      return None

    val pbBytes = new Array[Byte](msgLen)
    bb.get(pbBytes)

    val m = try
      codec.Message.parseFrom(pbBytes)
    catch
      case t: Throwable =>
        logger.error(s"Failed to parse protobuf message: $t", t)
        return None

    try
      m.msg match
        case codec.Message.Msg.HostHeartbeat(r) =>
          Some(Codec.decode(r))

        case codec.Message.Msg.Read(r) =>
          Some(Codec.decode(r))

        case codec.Message.Msg.Prepare(p) =>
          bb.position(4 + msgLen)
          val contentSize = bb.getInt()
          val preTxSize = bb.getInt()

          val contentEndPos = bb.position() + contentSize
          val preTxEndPos = contentEndPos + preTxSize

          val updateContent = if bb.remaining() == 0 then
            (Nil, Nil)
          else
            var localUpdates: List[ObjectUpdate] = Nil
            var preTxRebuilds: List[PreTransactionOpportunisticRebuild] = Nil

            while bb.position() != contentEndPos do
              val msb = bb.getLong()
              val lsb = bb.getLong()
              val len = bb.getInt()
              val uuid = new UUID(msb, lsb)

              val slice = bb.asReadOnlyBuffer()
              slice.limit(slice.position() + len)
              bb.position(bb.position() + len)
              localUpdates = ObjectUpdate(ObjectId(uuid), DataBuffer(slice)) :: localUpdates

            while bb.position() != preTxEndPos do
              val msb = bb.getLong()
              val lsb = bb.getLong()
              val uuid = new UUID(msb, lsb)
              val metadata = Metadata(bb)
              val len = bb.getInt()

              val slice = bb.asReadOnlyBuffer()
              slice.limit(slice.position() + len)
              bb.position(bb.position() + len)
              preTxRebuilds = PreTransactionOpportunisticRebuild(ObjectId(uuid), metadata, DataBuffer(slice)) :: preTxRebuilds

            (localUpdates, preTxRebuilds)

          Some(Codec.decode(p, updateContent._1, updateContent._2))

        case codec.Message.Msg.PrepareResponse(r) =>
          Some(Codec.decode(r))

        case codec.Message.Msg.Accept(r) =>
          Some(Codec.decode(r))

        case codec.Message.Msg.AcceptResponse(r) =>
          Some(Codec.decode(r))

        case codec.Message.Msg.Resolved(r) =>
          Some(Codec.decode(r))

        case codec.Message.Msg.Committed(r) =>
          Some(Codec.decode(r))

        case codec.Message.Msg.Finalized(r) =>
          Some(Codec.decode(r))

        case codec.Message.Msg.Heartbeat(r) =>
          Some(Codec.decode(r))

        case codec.Message.Msg.OpportunisticRebuild(r) =>
          Some(Codec.decode(r))

        case codec.Message.Msg.TransactionCompletionQuery(r) =>
          Some(Codec.decode(r))

        case codec.Message.Msg.StoreTransferData(r) =>
          Some(Codec.decode(r))

        case codec.Message.Msg.StartStoreTransfer(r) =>
          Some(Codec.decode(r))

        case codec.Message.Msg.CheckStorageDevice(r) =>
          Some(Codec.decode(r))

        case codec.Message.Msg.UnknownStore(r) =>
          Some(Codec.decode(r))

        case codec.Message.Msg.StatusRequest(r) =>
          Some(Codec.decode(r))

        case codec.Message.Msg.StatusResponse(r) =>
          Some(Codec.decode(r))

        case codec.Message.Msg.ReadResponse(r) =>
          Some(Codec.decode(r))

        case codec.Message.Msg.TransactionCompletionResponse(r) =>
          Some(Codec.decode(r))

        case codec.Message.Msg.TxResolved(r) =>
          Some(Codec.decode(r))

        case codec.Message.Msg.TxFinalized(r) =>
          Some(Codec.decode(r))

        case codec.Message.Msg.Empty =>
          logger.error("Empty Message!")
          None
    catch
      case t: Throwable =>
        logger.error(s"Failed to decode message: $t", t)
        None
