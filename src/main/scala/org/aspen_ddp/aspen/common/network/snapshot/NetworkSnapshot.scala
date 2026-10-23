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

  private def decompress(b: Array[Byte]): Array[Byte] =
    val size = Zstd.decompressedSize(b)
    if size < 0 then
      throw new IllegalArgumentException("Invalid network snapshot: unknown decompressed size")
    Zstd.decompress(b, size.toInt)
