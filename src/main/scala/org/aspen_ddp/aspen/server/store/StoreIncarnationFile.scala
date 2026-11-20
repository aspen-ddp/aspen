package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.common.store.{StoreId, StoreIncarnation}
import org.aspen_ddp.aspen.server.{DurableFiles, FileStat}
import scribe.Logging

import java.util.UUID

/** The store's durable identity: one UUID in the store directory's `incarnation` file. Written when the directory is
 *  made (StoreCreation, for a new store and a rebuild's staging copy) and replaced by `RebuildingStore.finish`, so a
 *  rebuilt store is told apart from the one whose Paxos state it lost. It survives restarts and transfers (the
 *  transfer archives the whole directory). A load never writes it: a store that keeps its data and CRL state under a
 *  fresh incarnation would take a recovered commit vote's UnackedCommits entry, written under the old one, for a
 *  commit it does not owe, and skip it (cross-document-review 2.2, systemic-loss-protection D14). */
object StoreIncarnationFile extends Logging:
  val FileName = "incarnation"

  /** The incarnation file is known to be absent: the store has lost its identity */
  final class IncarnationLost(file: os.Path) extends Exception(s"no incarnation file at $file")

  /** The store's incarnation, read from its directory. Throws IncarnationLost if the file is known to be absent, and
   *  any other error (a failed stat or read, content that does not parse) as it is: the file may be fine, and is
   *  never replaced. A backend with no directory (`MapBackend`, path `os.root`) gets the store's default. */
  def load(backendPath: os.Path, storeId: StoreId): StoreIncarnation =
    if backendPath == os.root then
      StoreIncarnation.default(storeId)
    else
      val file = backendPath / FileName
      if !FileStat.exists(file) then
        throw new IncarnationLost(file)
      StoreIncarnation(UUID.fromString(os.read(file).trim))

  /** Replaces the incarnation file in `storeDir` with a new random one, durably, and returns it */
  def writeFresh(storeDir: os.Path): StoreIncarnation =
    val incarnation = StoreIncarnation.random()
    DurableFiles.write(storeDir / FileName, incarnation.uuid.toString)
    incarnation
