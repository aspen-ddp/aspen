package org.aspen_ddp.aspen.server.rebuild

import org.aspen_ddp.aspen.common.network.PendingAllocation
import org.aspen_ddp.aspen.common.objects.{Key, ObjectId, ObjectPointer}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.common.util.YamlFormat.*

import java.util.{HexFormat, UUID}

/** The `rebuild-state.yaml` checkpoint, written into a rebuild's staging directory.
 *
 *  Its presence is what makes a restart a resume, so it is deleted only on successful
 *  completion -- never on startup.
 *
 *  `lastRestoredKey` is an allocation-tree key (the raw bytes of an ObjectId), hex-encoded
 *  because those bytes are not text. The hex is quoted so YAML reads it as a string: an
 *  unquoted `0123456789` parses as a number and a `1e5` as a float, either of which would
 *  come back as something other than the key that was written. `failedObjects` are objects
 *  the walk could not read; the checkpoint advances past them so one unreadable object cannot
 *  wedge the walk forever, and they are retried at the end of the pass. `pendingAllocations` are
 *  every allocation the pool's other stores listed as unfinalized at the peer query of any pass so
 *  far (rebuild-process 5.3): a resume sweeps them again, since one finalized meanwhile may have its tree
 *  entry behind `lastRestoredKey`, where no walk looks. Pointers are hex-encoded `ObjectPointer`s. `episode` is the
 *  Rebuilding episode that wrote it (StoreEntry.rebuildEpisode, absent for None): a rebuild resumes only its own
 *  episode's checkpoint, so one left by an earlier Rebuilding episode of the store is discarded.
 *
 *  ```
 *  store-id: 00000000-0000-0000-0000-000000000000:0
 *  episode: 55555555-5555-5555-5555-555555555555
 *  last-restored-key: '0a1b2c'
 *  failed-objects:
 *    - 22222222-2222-2222-2222-222222222222
 *  pending-allocations:
 *    - transaction-id: 44444444-4444-4444-4444-444444444444
 *      committed: true
 *      pointers: ['0a1b2c...']
 *  ```
 */
case class RebuildState(storeId: StoreId,
                        episode: Option[UUID],
                        lastRestoredKey: Option[Key],
                        failedObjects: List[ObjectId],
                        pendingAllocations: List[PendingAllocation]):

  def yamlConfig: String =
    val hex = HexFormat.of()
    val keyLine = lastRestoredKey match
      case Some(k) => s"last-restored-key: '${hex.formatHex(k.bytes)}'\n"
      case None => ""
    val failedLines =
      if failedObjects.isEmpty then
        "failed-objects: []\n"
      else
        "failed-objects:\n" + failedObjects.map(o => s"  - ${o.uuid}\n").mkString

    val pendingLines =
      if pendingAllocations.isEmpty then
        "pending-allocations: []\n"
      else
        "pending-allocations:\n" + pendingAllocations.map: p =>
          val ptrs = p.pointers.map(ptr => s"'${hex.formatHex(ptr.toArray)}'").mkString(", ")
          s"  - transaction-id: ${p.transactionId.uuid}\n    committed: ${p.committed}\n    pointers: [$ptrs]\n"
        .mkString

    val episodeLine = episode.map(e => s"episode: $e\n").getOrElse("")
    s"store-id: $storeId\n" + episodeLine + keyLine + failedLines + pendingLines


/** The checkpoint's format. Its reads and writes are `RebuildDisk`'s (`save`, `load`, `removeAll`), run as jobs on
 *  the destination device's DeviceIo */
object RebuildState extends YObject[RebuildState]:

  val stateFilename = "rebuild-state.yaml"

  /** Scratch name `RebuildDisk.save` writes before renaming onto `stateFilename`. Same directory, so the
   *  rename stays within one filesystem and is therefore atomic. */
  val tempFilename = s"$stateFilename.tmp"

  /** Beyond this many unreadable objects something systemic is wrong -- the pool below its read
   *  threshold, most likely -- and continuing only burns I/O. The pass aborts with the
   *  checkpoint intact and the next device check retries. */
  val MaxFailedObjects: Int = 10000

  private val hex = HexFormat.of()

  val storeId: Required[StoreId] = Required("store-id", StoreId.YStoreId)
  val episode: Optional[UUID] = Optional("episode", YUUID)
  val lastRestoredKey: Optional[String] = Optional("last-restored-key", YString)
  val failedObjects: Required[List[UUID]] = Required("failed-objects", YList(YUUID))
  // Required: a checkpoint without it does not load, so its pass starts over, walking the whole tree
  val pendingAllocations: Required[List[PendingAllocation]] = Required("pending-allocations", YList(YPending))

  val attrs: List[Attr] = storeId :: episode :: lastRestoredKey :: failedObjects :: pendingAllocations :: Nil

  object YPending extends YObject[PendingAllocation]:
    val transactionId: Required[UUID] = Required("transaction-id", YUUID)
    val committed: Required[Boolean] = Required("committed", YBool)
    val pointers: Required[List[String]] = Required("pointers", YList(YString))

    val attrs: List[Attr] = transactionId :: committed :: pointers :: Nil

    def create(o: Object): PendingAllocation =
      PendingAllocation(TransactionId(transactionId.get(o)), committed.get(o),
                        pointers.get(o).map(s => ObjectPointer(hex.parseHex(s))))

  def create(o: Object): RebuildState =
    RebuildState(
      storeId.get(o),
      episode.get(o),
      lastRestoredKey.get(o).map(s => Key(hex.parseHex(s))),
      failedObjects.get(o).map(u => ObjectId(u)),
      pendingAllocations.get(o))
