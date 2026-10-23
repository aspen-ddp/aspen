package org.aspen_ddp.aspen.common.network.snapshot

import com.github.luben.zstd.Zstd
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.network.{CachedHost, CachedStore, Codec}

final case class HostSnapshot(scanStart: HLCTimestamp, hosts: List[CachedHost])

final case class StoreSnapshot(scanStart: HLCTimestamp, stores: List[CachedStore])

/** Protobuf encoded, zstd compressed lists of every host endpoint and store->host mapping.
  * The decode functions throw on corrupt input.
  */
object NetworkSnapshot:
  private val CompressionLevel = 3

  def encodeHosts(s: HostSnapshot): Array[Byte] =
    compress(Codec.encodeHostSnapshot(s.hosts, s.scanStart))

  def decodeHosts(b: Array[Byte]): HostSnapshot =
    val (hosts, scanStart) = Codec.decodeHostSnapshot(decompress(b))
    HostSnapshot(scanStart, hosts)

  def encodeStores(s: StoreSnapshot): Array[Byte] =
    compress(Codec.encodeStoreSnapshot(s.stores, s.scanStart))

  def decodeStores(b: Array[Byte]): StoreSnapshot =
    val (stores, scanStart) = Codec.decodeStoreSnapshot(decompress(b))
    StoreSnapshot(scanStart, stores)

  private def compress(raw: Array[Byte]): Array[Byte] = Zstd.compress(raw, CompressionLevel)

  /** Far above any real snapshot (some 100 bytes per host, under 50 per store), yet small enough that a corrupt
   *  or hostile frame cannot make a reader allocate without bound */
  val MaxDecompressedSize: Long = 256L * 1024 * 1024

  /** The frame's declared size is checked before it is allocated */
  private def decompress(b: Array[Byte]): Array[Byte] =
    // Negative for a frame that declares no size or is not a zstd frame at all
    val size = Zstd.getFrameContentSize(b)
    if size < 0 then
      throw new IllegalArgumentException(s"Invalid network snapshot: unknown or invalid decompressed size ($size)")
    if size > MaxDecompressedSize then
      throw new IllegalArgumentException(
        s"Invalid network snapshot: declared size $size exceeds the $MaxDecompressedSize byte limit")
    Zstd.decompress(b, size.toInt)
