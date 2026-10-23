package org.aspen_ddp.aspen.common.network.implementations.quic

import io.netty.buffer.{ByteBuf, ByteBufAllocator, ByteBufInputStream, ByteBufOutputStream, CompositeByteBuf, Unpooled}
import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.network.*
import org.aspen_ddp.aspen.common.objects.{Metadata, ObjectId}
import org.aspen_ddp.aspen.common.transaction.{ObjectUpdate, PreTransactionOpportunisticRebuild}

import java.nio.ByteBuffer
import java.util.UUID

/** Wire format for the QUIC network plugin (all integers big-endian).
  *
  * {{{
  * datagram     := prefixLen:u32  prefix  section*
  * stream frame := frameLen:u64   prefixLen:u32  prefix  section*     frameLen = 4 + prefixLen + sum(sectionLen)
  * prefix       := headerLen:u32  header  auxLen:u32  aux  nSections:u32  sectionLen:u32 * nSections
  * }}}
  *
  * Bulk payloads travel as sections so they are never copied into protobuf on send.
  */
object QuicMessageCodec:

  val DatagramThreshold: Int = 1024
  val FrameLengthSize: Int = 8

  /** Owns `prefix` (pooled) until converted or released. `sections` are the sender's DataBuffers, never copied.
    * `streamOnly` sends it on a stream whatever its size, for a small message that must not be silently lost.
    */
  final class Encoded(val prefix: ByteBuf, val sections: Array[DataBuffer], val streamOnly: Boolean = false):
    private var consumed = false

    /** 4 + prefix.readableBytes + sum of section sizes */
    val size: Long =
      var total = 4L + prefix.readableBytes()
      var i = 0
      while i < sections.length do
        total += sections(i).size
        i += 1
      total

    def isDatagram: Boolean = !streamOnly && size <= DatagramThreshold

    /** One pooled buffer holding prefixLen, prefix and the sections (copied). Consumes this. */
    def toDatagram(alloc: ByteBufAllocator): ByteBuf =
      require(!consumed, "Encoded already consumed")
      consumed = true
      val out = alloc.buffer(Math.toIntExact(size))
      try
        out.writeInt(prefix.readableBytes())
        out.writeBytes(prefix)
        sections.foreach(s => out.writeBytes(s.asReadOnlyBuffer()))
        out
      catch
        case t: Throwable =>
          out.release()
          throw t
      finally
        prefix.release()

    /** A composite of [frameLen + prefixLen], prefix and read-only wrappers of the sections. Consumes this. */
    def toStreamFrame(alloc: ByteBufAllocator): CompositeByteBuf =
      require(!consumed, "Encoded already consumed")
      consumed = true
      val frame = alloc.compositeBuffer(sections.length + 2)
      try
        val head = alloc.buffer(FrameLengthSize + 4)
        head.writeLong(size) // frameLen = 4 + prefixLen + sections
        head.writeInt(prefix.readableBytes())
        frame.addComponent(true, head)
        frame.addComponent(true, prefix) // frame now owns prefix
        sections.foreach: s =>
          if s.size > 0 then
            frame.addComponent(true, Unpooled.wrappedBuffer(s.asReadOnlyBuffer()))
        frame
      catch
        case t: Throwable =>
          // components added so far are released with the frame. prefix is released here only if not yet added
          val added = frame.numComponents() >= 2
          frame.release()
          if !added then
            prefix.release()
          throw t

    def release(): Unit =
      if !consumed then
        consumed = true
        prefix.release()

  final case class Prefix(header: codec.Message, aux: ByteBuffer, sectionSizes: Array[Int])

  // ============================================================================
  // Encoding
  // ============================================================================

  private def toMsg(msg: Message): codec.Message.Msg = msg match
    case m: HostHeartbeat => codec.Message.Msg.HostHeartbeat(Codec.encode(m))
    case m: TxPrepare => codec.Message.Msg.Prepare(Codec.encode(m))
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
    case m: Read => codec.Message.Msg.Read(Codec.encode(m))
    case m: OpportunisticRebuild => codec.Message.Msg.OpportunisticRebuild(Codec.encode(m))
    case m: TransactionCompletionQuery => codec.Message.Msg.TransactionCompletionQuery(Codec.encode(m))
    case m: ReadResponse => codec.Message.Msg.ReadResponse(Codec.encode(m))
    case m: TransactionCompletionResponse => codec.Message.Msg.TransactionCompletionResponse(Codec.encode(m))
    case m: TransactionFinalized => codec.Message.Msg.TxFinalized(Codec.encode(m))
    case m: TransactionResolved => codec.Message.Msg.TxResolved(Codec.encode(m))
    case m: StartStoreTransfer => codec.Message.Msg.StartStoreTransfer(Codec.encode(m))
    case m: CheckStorageDevice => codec.Message.Msg.CheckStorageDevice(Codec.encode(m))
    case m: ServiceMessage => codec.Message.Msg.ServiceMessage(Codec.encode(m))
    case m: ExecuteSystemTask => codec.Message.Msg.ExecuteSystemTask(Codec.encode(m))
    case m: StoreMoved => codec.Message.Msg.StoreMoved(Codec.encode(m))
    case m: HostEndpointChanged => codec.Message.Msg.HostEndpointChanged(Codec.encode(m))
    case m: ClusterTransportMessage => codec.Message.Msg.ClusterTransport(Codec.encode(m))
    case m: GossipSubscribe => codec.Message.Msg.GossipSubscribe(Codec.encode(m))
    case m: HostOnlineStatus => codec.Message.Msg.HostOnlineStatus(Codec.encode(m))

  private val NoSections: Array[DataBuffer] = Array.empty[DataBuffer]

  private def writeUUID(bb: ByteBuffer, id: UUID): Unit =
    bb.putLong(id.getMostSignificantBits)
    bb.putLong(id.getLeastSignificantBits)

  /** Returns (message used for the header, aux bytes, sections) */
  private def split(msg: Message): (Message, ByteBuffer, Array[DataBuffer]) = msg match
    case m: TxPrepare =>
      val auxSize = 4 + 16 * m.objectUpdates.size + 4 + (16 + Metadata.EncodedSize) * m.preTxRebuilds.size
      val aux = ByteBuffer.allocate(auxSize)
      aux.putInt(m.objectUpdates.size)
      m.objectUpdates.foreach(u => writeUUID(aux, u.objectId.uuid))
      aux.putInt(m.preTxRebuilds.size)
      m.preTxRebuilds.foreach: r =>
        writeUUID(aux, r.objectId.uuid)
        r.requiredMetadata.encodeInto(aux)
      aux.flip()
      (m, aux, (m.objectUpdates.map(_.data) ++ m.preTxRebuilds.map(_.data)).toArray)

    case m @ ReadResponse(_, _, _, _, Right(cs)) if cs.objectData.exists(!_.isEmpty) =>
      (m.copy(result = Right(cs.copy(objectData = None))), ByteBuffer.allocate(0), Array(cs.objectData.get))

    case m: OpportunisticRebuild =>
      (m.copy(data = DataBuffer.Empty), ByteBuffer.allocate(0), Array(m.data))

    case m => (m, ByteBuffer.allocate(0), NoSections)

  /** `streamOnly`: see `Encoded` */
  def encode(msg: Message, alloc: ByteBufAllocator, streamOnly: Boolean = false): Encoded =
    val (headerMsg, aux, sections) = split(msg)
    val header = codec.Message(msg = toMsg(headerMsg))
    val headerLen = header.serializedSize
    val prefixLen = 4 + headerLen + 4 + aux.remaining() + 4 + 4 * sections.length
    val prefix = alloc.buffer(prefixLen)
    try
      prefix.writeInt(headerLen)
      val out = new ByteBufOutputStream(prefix)
      header.writeTo(out)
      out.flush()
      prefix.writeInt(aux.remaining())
      prefix.writeBytes(aux.duplicate())
      prefix.writeInt(sections.length)
      sections.foreach(s => prefix.writeInt(s.size))
      new Encoded(prefix, sections, streamOnly)
    catch
      case t: Throwable =>
        prefix.release()
        throw t

  // ============================================================================
  // Decoding
  // ============================================================================

  private def parseHeader(buf: ByteBuf): Either[String, codec.Message] =
    try
      Right(codec.Message.parseFrom(new ByteBufInputStream(buf)))
    catch
      case t: Throwable => Left(s"Failed to parse protobuf header: $t")

  /** `buf` holds exactly the prefix bytes. Does not release or advance `buf`. */
  def parsePrefix(buf: ByteBuf): Either[String, Prefix] =
    try
      val base = buf.readerIndex()
      val end = base + buf.readableBytes()
      var pos = base

      def readLen(what: String): Either[String, Int] =
        if end - pos < 4 then
          Left(s"Truncated prefix reading $what")
        else
          val v = buf.getInt(pos)
          pos += 4
          if v < 0 then Left(s"Negative $what: $v") else Right(v)

      def readBytes(n: Int, what: String): Either[String, Int] =
        if n > end - pos then
          Left(s"$what length $n exceeds remaining ${end - pos} bytes")
        else
          val start = pos
          pos += n
          Right(start)

      for
        headerLen <- readLen("headerLen")
        headerPos <- readBytes(headerLen, "header")
        auxLen <- readLen("auxLen")
        auxPos <- readBytes(auxLen, "aux")
        nSections <- readLen("nSections")
        _ <- if nSections.toLong * 4 > (end - pos).toLong then Left(s"Section count $nSections exceeds prefix") else Right(())
        sizes <- {
          val arr = new Array[Int](nSections)
          var err: Option[String] = None
          var i = 0
          while err.isEmpty && i < nSections do
            readLen("sectionLen") match
              case Right(n) => arr(i) = n
              case Left(e) => err = Some(e)
            i += 1
          err.toLeft(arr)
        }
        _ <- if pos != end then Left(s"${end - pos} unexpected trailing bytes in prefix") else Right(())
        header <- parseHeader(buf.slice(headerPos, headerLen))
      yield
        val auxBytes = new Array[Byte](auxLen)
        buf.getBytes(auxPos, auxBytes)
        Prefix(header, ByteBuffer.wrap(auxBytes), sizes)
    catch
      case t: Throwable => Left(s"Failed to parse prefix: $t")

  /** sections.length must equal prefix.sectionSizes.length; reconstructs the Message */
  def assemble(prefix: Prefix, sections: Array[DataBuffer]): Either[String, Message] =
    if sections.length != prefix.sectionSizes.length then
      Left(s"Expected ${prefix.sectionSizes.length} sections but got ${sections.length}")
    else if sections.indices.exists(i => sections(i).size != prefix.sectionSizes(i)) then
      Left("Section sizes do not match the prefix")
    else
      try
        prefix.header.msg match
          case codec.Message.Msg.Prepare(p) => assemblePrepare(p, prefix.aux.duplicate(), sections)
          case codec.Message.Msg.ReadResponse(r) =>
            if sections.length > 1 then
              Left("ReadResponse carries at most one section")
            else
              val rr = Codec.decode(r)
              (rr.result, sections.headOption) match
                case (Right(cs), Some(data)) => Right(rr.copy(result = Right(cs.copy(objectData = Some(data)))))
                case (Left(_), Some(_)) => Left("ReadResponse error result cannot carry a data section")
                case _ => Right(rr)
          case codec.Message.Msg.OpportunisticRebuild(r) =>
            if sections.length != 1 then
              Left(s"OpportunisticRebuild requires exactly one section, got ${sections.length}")
            else
              Right(Codec.decode(r).copy(data = sections(0)))
          case other if sections.nonEmpty => Left(s"Unexpected ${sections.length} sections for message")
          case codec.Message.Msg.HostHeartbeat(r) => Right(Codec.decode(r))
          case codec.Message.Msg.Read(r) => Right(Codec.decode(r))
          case codec.Message.Msg.PrepareResponse(r) => Right(Codec.decode(r))
          case codec.Message.Msg.Accept(r) => Right(Codec.decode(r))
          case codec.Message.Msg.AcceptResponse(r) => Right(Codec.decode(r))
          case codec.Message.Msg.Resolved(r) => Right(Codec.decode(r))
          case codec.Message.Msg.Committed(r) => Right(Codec.decode(r))
          case codec.Message.Msg.Finalized(r) => Right(Codec.decode(r))
          case codec.Message.Msg.Heartbeat(r) => Right(Codec.decode(r))
          case codec.Message.Msg.TransactionCompletionQuery(r) => Right(Codec.decode(r))
          case codec.Message.Msg.StartStoreTransfer(r) => Right(Codec.decode(r))
          case codec.Message.Msg.CheckStorageDevice(r) => Right(Codec.decode(r))
          case codec.Message.Msg.ServiceMessage(r) => Right(Codec.decode(r))
          case codec.Message.Msg.ExecuteSystemTask(r) => Right(Codec.decode(r))
          case codec.Message.Msg.StoreMoved(r) => Right(Codec.decode(r))
          case codec.Message.Msg.HostEndpointChanged(r) => Right(Codec.decode(r))
          case codec.Message.Msg.ClusterTransport(r) => Right(Codec.decode(r))
          case codec.Message.Msg.GossipSubscribe(r) => Right(Codec.decode(r))
          case codec.Message.Msg.HostOnlineStatus(r) => Right(Codec.decode(r))
          case codec.Message.Msg.UnknownStore(r) => Right(Codec.decode(r))
          case codec.Message.Msg.StatusRequest(r) => Right(Codec.decode(r))
          case codec.Message.Msg.StatusResponse(r) => Right(Codec.decode(r))
          case codec.Message.Msg.TransactionCompletionResponse(r) => Right(Codec.decode(r))
          case codec.Message.Msg.TxResolved(r) => Right(Codec.decode(r))
          case codec.Message.Msg.TxFinalized(r) => Right(Codec.decode(r))
          case codec.Message.Msg.Empty => Left("Empty message")
      catch
        case t: Throwable => Left(s"Failed to decode message: $t")

  private def assemblePrepare(p: codec.TxPrepare, aux: ByteBuffer, sections: Array[DataBuffer]): Either[String, Message] =
    if aux.remaining() < 4 then
      Left("TxPrepare aux too short")
    else
      val nUpdates = aux.getInt()
      if nUpdates < 0 || nUpdates.toLong * 16 + 4 > aux.remaining() then
        Left(s"Invalid TxPrepare update count $nUpdates")
      else
        val updateIds = Array.fill(nUpdates)(ObjectId(new UUID(aux.getLong(), aux.getLong())))
        val nRebuilds = aux.getInt()
        if nRebuilds < 0 || nRebuilds.toLong * (16 + Metadata.EncodedSize) != aux.remaining().toLong then
          Left(s"Invalid TxPrepare rebuild count $nRebuilds")
        else if nUpdates.toLong + nRebuilds != sections.length then
          Left(s"TxPrepare expects ${nUpdates.toLong + nRebuilds} sections, got ${sections.length}")
        else
          val updates = updateIds.indices.map(i => ObjectUpdate(updateIds(i), sections(i))).toList
          val rebuilds = (0 until nRebuilds).map: i =>
            val id = ObjectId(new UUID(aux.getLong(), aux.getLong()))
            PreTransactionOpportunisticRebuild(id, Metadata(aux), sections(nUpdates + i))
          .toList
          Right(Codec.decode(p, updates, rebuilds))

  /** Whole datagram; copies sections into heap DataBuffers. Does not release `buf`. */
  def decodeDatagram(buf: ByteBuf): Either[String, Message] =
    try
      val base = buf.readerIndex()
      val total = buf.readableBytes()
      if total < 4 then
        Left("Datagram too short")
      else
        val prefixLen = buf.getInt(base)
        if prefixLen < 0 || prefixLen > total - 4 then
          Left(s"Invalid prefix length $prefixLen")
        else
          parsePrefix(buf.slice(base + 4, prefixLen)).flatMap: p =>
            val sectionBytes = p.sectionSizes.foldLeft(0L)(_ + _)
            if sectionBytes != (total - 4 - prefixLen).toLong then
              Left(s"Datagram holds ${total - 4 - prefixLen} section bytes but prefix declares $sectionBytes")
            else
              var pos = base + 4 + prefixLen
              val sections = p.sectionSizes.map: n =>
                val arr = new Array[Byte](n)
                buf.getBytes(pos, arr)
                pos += n
                DataBuffer(arr)
              assemble(p, sections)
    catch
      case t: Throwable => Left(s"Failed to decode datagram: $t")
