package org.aspen_ddp.aspen.common.network.implementations.quic.gossip

import io.scalecube.cluster.transport.api.{Message, MessageCodec}

import java.io.{InputStream, ObjectInputFilter, ObjectInputStream, ObjectOutputStream, OutputStream}

/** Scalecube's JDK serialization behind an `ObjectInputFilter` allowlist.
  *
  * Cluster payloads arrive from any TLS-credentialed peer, clients included, so the plain `ObjectInputStream` of
  * scalecube's default codec would instantiate whatever serializable class a peer names. Only the classes scalecube's
  * own messages are built from are admitted: scalecube's (`Message`, `Member`, `PingData`, `SyncData`,
  * `MembershipRecord`, `Gossip`, `GossipRequest`, the metadata request and response, and their enums), `java.lang`
  * (strings, enums, boxed values) and `java.util` (collections), plus primitive arrays (Aspen gossip travels as
  * `byte[]`). The metadata response's `ByteBuffer` is written as raw bytes, not as an object, so it needs no entry.
  * Depth, array length, reference count and stream size are bounded too. Anything else fails deserialization with
  * `InvalidClassException`. Serialization is scalecube's, unchanged.
  */
object FilteredMessageCodec extends MessageCodec:
  val MaxDepth: Long = 16
  val MaxArrayLength: Long = 1 << 20
  val MaxReferences: Long = 100_000
  val MaxStreamBytes: Long = 4L << 20

  private def allowed(cls: Class[?]): Boolean =
    var c = cls
    while c.isArray do
      c = c.getComponentType
    val name = c.getName
    c.isPrimitive ||
      name.startsWith("io.scalecube.") ||
      name.startsWith("java.util.") ||
      (name.startsWith("java.lang.") && name.lastIndexOf('.') == "java.lang".length)

  val filter: ObjectInputFilter = (info: ObjectInputFilter.FilterInfo) =>
    if info.depth() > MaxDepth || info.arrayLength() > MaxArrayLength || info.references() > MaxReferences ||
      info.streamBytes() > MaxStreamBytes then
      ObjectInputFilter.Status.REJECTED
    else if info.serialClass() == null || allowed(info.serialClass()) then
      ObjectInputFilter.Status.ALLOWED
    else
      ObjectInputFilter.Status.REJECTED

  override def deserialize(is: InputStream): Message =
    val ois = new ObjectInputStream(is)
    try
      ois.setObjectInputFilter(filter)
      val message = new Message()
      message.readExternal(ois)
      message
    finally ois.close()

  override def serialize(message: Message, os: OutputStream): Unit =
    val oos = new ObjectOutputStream(os)
    try
      message.writeExternal(oos)
      oos.flush()
    finally oos.close()
