package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.common.store.{StoreId, StoreIncarnation}
import org.aspen_ddp.aspen.server.DurableFiles
import scribe.Logging

import java.util.UUID
import scala.util.{Failure, Success, Try}

/** The store's durable identity: one UUID in the store directory's `incarnation` file. It survives restarts and
 *  transfers (the transfer archives the whole directory) and is replaced by `RebuildingStore.finish`, so a rebuilt
 *  store is told apart from the one whose Paxos state it lost. */
object StoreIncarnationFile extends Logging:
  val FileName = "incarnation"

  /** The store's incarnation: read from its directory, or created there if the file is absent or unreadable.
   *  A backend with no directory (`MapBackend`, path `os.root`) gets the store's default and is never written. */
  def loadOrCreate(backendPath: os.Path, storeId: StoreId): StoreIncarnation =
    if backendPath == os.root then
      StoreIncarnation.default(storeId)
    else
      val file = backendPath / FileName
      val existing =
        if os.exists(file) then
          Try(StoreIncarnation(UUID.fromString(os.read(file).trim))) match
            case Success(incarnation) => Some(incarnation)
            case Failure(err) =>
              logger.warn(s"Store $storeId: cannot read its incarnation file $file, replacing it with a fresh " +
                s"incarnation: $err")
              None
        else
          None
      existing.getOrElse:
        val created = writeFresh(backendPath)
        logger.info(s"Store $storeId has incarnation ${created.uuid}")
        created

  /** Replaces the incarnation file in `storeDir` with a new random one, durably, and returns it */
  def writeFresh(storeDir: os.Path): StoreIncarnation =
    val incarnation = StoreIncarnation.random()
    DurableFiles.write(storeDir / FileName, incarnation.uuid.toString)
    incarnation
