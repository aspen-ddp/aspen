package org.aspen_ddp.aspen.server.rebuild

import org.aspen_ddp.aspen.common.objects.{Key, ObjectId}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.YamlFormat.*

import java.io.File
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


object RebuildState extends YObject[RebuildState]:

  val stateFilename = "rebuild-state.yaml"

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

  /** Write the checkpoint into `path`, replacing any existing one. */
  def save(path: os.Path, state: RebuildState): Unit =
    os.write.over(path / stateFilename, state.yamlConfig)

  /** Read the checkpoint from `path`, or None if there is none. */
  def load(path: os.Path): Option[RebuildState] =
    val f = path / stateFilename
    if os.exists(f) then
      Some(loadRebuildState(f.toIO))
    else
      None
