package org.aspen_ddp.aspen.server.rebuild

import org.aspen_ddp.aspen.common.objects.{Key, ObjectId}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.YamlFormat.*
import scribe.Logging

import java.io.File
import java.util.{HexFormat, UUID}
import scala.util.control.NonFatal

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
 *  wedge the walk forever, and they are retried at the end of the pass.
 *
 *  ```
 *  store-id: 00000000-0000-0000-0000-000000000000:0
 *  last-restored-key: '0a1b2c'
 *  failed-objects:
 *    - 22222222-2222-2222-2222-222222222222
 *  ```
 */
case class RebuildState(storeId: StoreId,
                        lastRestoredKey: Option[Key],
                        failedObjects: List[ObjectId]):

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

    s"store-id: $storeId\n" + keyLine + failedLines


object RebuildState extends YObject[RebuildState] with Logging:

  val stateFilename = "rebuild-state.yaml"

  /** Scratch name `save` writes before renaming onto `stateFilename`. Same directory, so the
   *  rename stays within one filesystem and is therefore atomic. */
  val tempFilename = s"$stateFilename.tmp"

  /** Beyond this many unreadable objects something systemic is wrong -- the pool below its read
   *  threshold, most likely -- and continuing only burns I/O. The pass aborts with the
   *  checkpoint intact and the next device check retries. */
  val MaxFailedObjects: Int = 10000

  private val hex = HexFormat.of()

  val storeId: Required[StoreId] = Required("store-id", StoreId.YStoreId)
  val lastRestoredKey: Optional[String] = Optional("last-restored-key", YString)
  val failedObjects: Required[List[UUID]] = Required("failed-objects", YList(YUUID))

  val attrs: List[Attr] = storeId :: lastRestoredKey :: failedObjects :: Nil

  def create(o: Object): RebuildState =
    RebuildState(
      storeId.get(o),
      lastRestoredKey.get(o).map(s => Key(hex.parseHex(s))),
      failedObjects.get(o).map(u => ObjectId(u)))

  def loadRebuildState(file: File): RebuildState =
    create(loadYamlFile(file))

  /** Write the checkpoint into `path`, replacing any existing one.
   *
   *  Written to a temp name alongside it and renamed into place rather than overwritten. Aspen
   *  is crash-only -- there is no clean shutdown -- so a crash between a truncate and the write
   *  that follows it is the ordinary termination mode landing in an unlucky window, not an
   *  exotic case, and it would leave an empty or partial `rebuild-state.yaml` behind. The temp
   *  file is in the same directory, hence the same filesystem, so the rename is atomic: a
   *  reader sees either the old checkpoint or the new one.
   */
  def save(path: os.Path, state: RebuildState): Unit =
    os.write.over(path / tempFilename, state.yamlConfig)
    os.move(path / tempFilename, path / stateFilename,
            replaceExisting = true, atomicMove = true)

  /** Delete the checkpoint from `path`, along with any temp file a crashed save left behind. */
  def removeAll(path: os.Path): Unit =
    os.remove(path / stateFilename)
    os.remove(path / tempFilename)

  /** Read the checkpoint from `path`, or None if there is none or it cannot be read.
   *
   *  Total by design. An unreadable checkpoint -- torn by a crash predating the atomic save
   *  above, or corrupt on disk -- is reported as "no checkpoint" rather than thrown, because
   *  `load` is the first statement of a rebuild pass and a throw there makes the store
   *  permanently unrebuildable without an operator deleting the file by hand. The cost of
   *  discarding it is a restart of the walk and nothing more: `rebuildWrite` is a plain
   *  overwrite, so re-restoring already-restored objects is free. That is the correct direction
   *  under the checkpoint rule -- lose work, never claim work that never reached stable storage.
   */
  def load(path: os.Path): Option[RebuildState] =
    val f = path / stateFilename
    if !os.exists(f) then
      None
    else
      try
        Some(loadRebuildState(f.toIO))
      catch
        case NonFatal(t) =>
          logger.warn(s"Discarding unreadable rebuild checkpoint $f; the rebuild will restart " +
                      s"from the beginning: $t")
          None
