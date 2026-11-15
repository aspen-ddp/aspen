package org.aspen_ddp.aspen.common.store

import java.nio.ByteBuffer
import java.util.UUID

/** Identifies one life of a store's data: a store that loses its content and is rebuilt takes a new one. */
final case class StoreIncarnation(uuid: UUID)

object StoreIncarnation:
  def random(): StoreIncarnation = StoreIncarnation(UUID.randomUUID())

  /** The incarnation of a store that has never been rebuilt: derived from its id, so stable and never the zero UUID */
  def default(storeId: StoreId): StoreIncarnation =
    val bb = ByteBuffer.allocate(17)
    bb.putLong(storeId.poolId.uuid.getMostSignificantBits)
    bb.putLong(storeId.poolId.uuid.getLeastSignificantBits)
    bb.put(storeId.poolIndex)
    val u = UUID.nameUUIDFromBytes(bb.array())
    if u.getMostSignificantBits == 0L && u.getLeastSignificantBits == 0L then StoreIncarnation(new UUID(0L, 1L))
    else StoreIncarnation(u)
