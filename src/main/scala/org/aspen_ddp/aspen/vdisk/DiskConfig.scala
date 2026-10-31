package org.aspen_ddp.aspen.vdisk

import java.nio.ByteBuffer
import java.util.UUID

import org.aspen_ddp.aspen.client.ObjectAllocatorId
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Key, KeyValueObjectPointer}

/** A virtual disk as recorded in the catalog.
  *
  * @param configPointer the disk's KeyValue config object, laid out per [[DiskConfig]]
  * @param fencePointer the DataObject holding the disk's [[FenceState]]
  * @param allocatorId the allocator every block object of the disk is allocated from
  */
final case class DiskInfo(name: String,
                          diskId: UUID,
                          configPointer: KeyValueObjectPointer,
                          fencePointer: DataObjectPointer,
                          geometry: DiskGeometry,
                          allocatorId: ObjectAllocatorId)

/** Keys and value encodings of a disk's config object. Everything but `SizeKey` is fixed at creation. */
object DiskConfig:
  /** Registry namespace for disk names */
  val RegistryNamespace: String = "vdisk"

  val SizeKey: Key = Key("size")
  val LogicalBlockSizeKey: Key = Key("logical-block-size")
  val BlockObjectSizeKey: Key = Key("block-object-size")
  val AllocatorKey: Key = Key("allocator")
  val FenceKey: Key = Key("fence")
  /** Hosts the Root of the block map, a DataObjects tree keyed by block index */
  val MapRootKey: Key = Key("map")

  def encodeLong(v: Long): Array[Byte] = ByteBuffer.allocate(8).putLong(v).array()
  def decodeLong(arr: Array[Byte]): Long = ByteBuffer.wrap(arr).getLong()
  def encodeInt(v: Int): Array[Byte] = ByteBuffer.allocate(4).putInt(v).array()
  def decodeInt(arr: Array[Byte]): Int = ByteBuffer.wrap(arr).getInt()
