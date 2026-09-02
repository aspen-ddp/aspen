package org.aspen_ddp.aspen.server.rebuild

import org.aspen_ddp.aspen.client.{AspenClient, DataObjectState, KeyValueObjectState, MetadataObjectState, ObjectState}
import org.aspen_ddp.aspen.client.KeyValueObjectState.ValueState
import org.aspen_ddp.aspen.client.tkvl.KeyValueListNode
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Key, KeyValueObjectPointer, Metadata, ObjectId, ObjectPointer, ObjectType}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.StoreConfig
import org.aspen_ddp.aspen.server.store.backend.{Backend, RocksDBBackend, RocksDBConfig}
import scribe.Logging

import java.nio.file.Path
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.control.NonFatal

object RebuildingStore:
  /** Staging directory under the device root. Invisible to StoreManager.tryLoadStore for the
   *  same reason transferring-in/ is: there is no StoreConfig at this level, only one level
   *  down. That is what keeps a half-rebuilt store from being loaded and answering reads. */
  val RebuildDirectory = "rebuilding"

  /** Objects between checkpoints. Small enough that a crash costs little, large enough that
   *  the flush is not the dominant cost. */
  val CheckpointInterval: Int = 1000

  /** Length of an allocation-tree key that names an object: the raw bytes of an ObjectId. */
  val ObjectIdKeyLength: Int = 16


/** Reconstructs one store by walking its pool's allocation tree, reading each object through
 *  the client, and writing this store's slice of it into a fresh backend.
 *
 *  The store is not Active while this runs, so it participates in no transactions and receives
 *  no writes. Objects the walk has not yet reached are therefore read at their current value.
 *  The gap is the other side: an object already copied and then written before the rebuild
 *  finishes leaves a stale slice here. That is the same condition as a store that missed writes
 *  while briefly offline, and the repair-on-read path heals it for anything something reads.
 *  Cold objects are not healed; a background scrubber is deliberately out of scope.
 */
class RebuildingStore(client: AspenClient,
                      val storeId: StoreId,
                      storageDeviceId: StorageDeviceId,
                      devicePath: Path,
                      checkpointInterval: Int = RebuildingStore.CheckpointInterval,
                      testingOnlyFailKeys: Set[Key] = Set(),
                      testingOnlyOutOfSpaceKeys: Set[Key] = Set(),
                      testingOnlyTrackRestoredKeys: Boolean = false,
                      maxFailedObjects: Int = RebuildState.MaxFailedObjects) extends StoreRebuild with Logging:

  import RebuildingStore.*

  private given ExecutionContext = client.clientContext

  private val stagingPath = os.Path(devicePath) / RebuildDirectory / storeId.directoryName
  private val finalPath = os.Path(devicePath) / storeId.directoryName

  private val completionPromise: Promise[Unit] = Promise()
  def complete: Future[Unit] = completionPromise.future

  /** Keys handed to rebuildWrite, in walk order. Test hook, and off unless a test asks for it:
   *  a real store holds millions of objects, and accumulating a Key per object for the life of
   *  the rebuild would cost that memory for no production purpose. */
  private val restoredKeys = mutable.ListBuffer[Key]()
  private[rebuild] def testingOnlyRestoredKeys: List[Key] = synchronized(restoredKeys.toList)

  /** "flush" / "checkpoint" in the order they happened. Test hook: the ordering between them
   *  is the one thing that cannot be observed from disk after a successful pass. */
  private val checkpointTrace = mutable.ListBuffer[String]()
  private[rebuild] def testingOnlyCheckpointTrace: List[String] =
    synchronized(checkpointTrace.toList)

  private var backend: Backend = null
  private var restoredSinceCheckpoint = 0
  private var lastKey: Option[Key] = None
  private var failed: List[ObjectId] = Nil

  /** Abort latch: when a fatal condition fires (out of space or too many failures), we record
   *  the cause here. KeyValueListNode.walkFrom swallows a failing fn by design -- it logs and
   *  continues -- so a fatal condition has to be carried out of band. Once set, restoreObject
   *  returns Future.unit immediately, making the remainder of the walk a cheap no-op. The
   *  latched cause is checked after the walk returns, and if set, the pass fails before
   *  retryFailures() and finish() run, leaving the checkpoint in staging so the next device
   *  check resumes. */
  private var abortCause: Option[Throwable] = None

  // Key's equality is not something to bet a test seam on; compare the bytes directly.
  // Declared here, before start(), so it is initialized before the walk begins.
  private val failBytes: Set[List[Byte]] = testingOnlyFailKeys.map(_.bytes.toList)
  private val outOfSpaceBytes: Set[List[Byte]] = testingOnlyOutOfSpaceKeys.map(_.bytes.toList)

  // Started from the constructor, mirroring TransferringIn. StoreManager holds its instance
  // lock across the call, so everything expensive is inside the future.
  start()

  private def start(): Unit =
    val f =
      try
        if os.exists(finalPath) then
          // A crash between the move and the flip. The store is already whole; there is nothing to
          // rebuild, only the flip left, and that is StoreManager's job on completion.
          logger.info(s"Rebuild of $storeId: store already in place at $finalPath")
          cleanupStaging()
          Future.unit
        else
          runPass()
      catch
        case err: Throwable => Future.failed(err)

    f.onComplete: outcome =>
      closeBackend().onComplete(_ => completionPromise.tryComplete(outcome))

  private def cleanupStaging(): Unit =
    try
      if os.exists(stagingPath) then os.remove.all(stagingPath)
    catch
      case t: Throwable => logger.warn(s"Rebuild of $storeId: failed to remove $stagingPath: $t")

  /** Idempotent: finish() closes on the happy path, start()'s onComplete closes on every path. */
  private def closeBackend(): Future[Unit] =
    val b = synchronized:
      val prev = backend
      backend = null
      prev

    if b == null then
      Future.unit
    else
      b.close().recover:
        case t => logger.warn(s"Rebuild of $storeId: backend close failed: $t")

  /** One full pass: open (or reopen) the staging area, walk, retry failures, complete. */
  private def runPass(): Future[Unit] =
    // The checkpoint is never deleted on restart -- reading it is what makes a restart a
    // resume.
    val resume = RebuildState.load(stagingPath)

    os.makeDir.all(stagingPath)

    if !os.exists(stagingPath / StoreConfig.configFilename) then
      // Written up front so the directory is loadable the instant it is moved into place.
      os.write.over(stagingPath / StoreConfig.configFilename,
        StoreConfig(storeId, StoreConfig.RocksDB()).yamlConfig)

    resume.foreach: st =>
      synchronized:
        lastKey = st.lastRestoredKey
        failed = st.failedObjects
      logger.info(s"Rebuild of $storeId: resuming from ${st.lastRestoredKey}, " +
                  s"${st.failedObjects.size} objects to retry")

    for
      pstate <- client.getStoragePoolState(storeId.poolId)
      _ = synchronized:
            backend = pstate.backendConfig match
              case _: RocksDBConfig => new RocksDBBackend(stagingPath.toNIO, storeId, summon)
      pool <- client.getStoragePool(storeId.poolId)
      tree = pool.allocationTree
      _ <- resume.flatMap(_.lastRestoredKey) match
             // The resume range is inclusive of the checkpointed key. rebuildWrite is an
             // overwrite, so re-restoring that one object is free, and inclusive is the only
             // bound that cannot skip.
             case Some(k) => tree.foreachFrom(k, restoreObject)
             case None => tree.foreach(restoreObject)
      // Check the abort latch after the walk completes. If it's set, fail the pass before
      // retryFailures() and finish() run, leaving the checkpoint in staging.
      _ <- synchronized(abortCause) match
             case Some(cause) => Future.failed(cause)
             case None => Future.unit
      _ <- retryFailures()
      _ <- finish()
    yield ()

  /** Copy one object's slice of this store into the backend.
   *
   *  foreach swallows a failing fn -- it logs and continues -- so read failures are recorded
   *  here instead. The checkpoint advances past them, so a read failure cannot wedge the walk
   *  forever, and they are retried at the end of the pass. (A read that never returns can still
   *  stall the walk and hold its StoreManager rebuild slot; that is a known gap.)
   */
  private def restoreObject(node: KeyValueListNode,
                            key: Key,
                            value: ValueState): Future[Unit] =
    // If the abort latch is set, return immediately to make the remainder of the walk a cheap
    // no-op rather than thousands of repeated failures.
    if synchronized(abortCause).isDefined then
      Future.unit
    else
      // Everything below is inside the try, not just the decode: a synchronous throw out of this
      // method is absorbed by ExecutionContext.reportFailure -- walkFrom calls fn from inside its
      // own onComplete recursion -- which stops the recursion, leaves walkFrom's promise never
      // completed, and so holds the rebuild slot forever. ObjectPointer decoding is the reachable
      // way to get one: it throws BufferUnderflowException out of the Varint readers and
      // MatchError out of its non-exhaustive typeCode match. Treating an undecodable pointer as
      // one more failed object subjects it to the existing cap and retry machinery instead.
      try
        val ptr = ObjectPointer(value.value.bytes)

        if ptr.poolId != storeId.poolId then
          // The allocation tree is per-pool, but a pointer stored in it is only authoritative for
          // its own pool. Skip anything foreign rather than writing it into the wrong store.
          Future.unit
        else
          attemptRestore(key, ptr).transformWith:
            case scala.util.Success(_) =>
              recordRestored(key)
            case scala.util.Failure(err) if isOutOfSpace(err) =>
              // Distinct from a per-object read failure, and not something to accumulate 10,000
              // of: selectDeviceForRebuild checked free space at placement time against the
              // pool's recorded store size, which can be stale and can grow. There is no
              // automatic recovery in this scope -- the operator's remedy is to add capacity --
              // so log it loudly enough to alert on and set the abort latch.
              // Latch first, then checkpoint. The checkpoint is expected to throw when the disk
              // is full, and if it does, the throw must not unwind past the latch. A best-effort
              // checkpoint on a full disk is fine -- losing it costs a restart from the last good
              // one, which is bounded. Losing the latch costs the store.
              latchOutOfSpace(err)
              try checkpoint()
              catch case t: Throwable => logger.warn(s"Rebuild of $storeId: checkpoint after ENOSPC failed: $t")
              Future.unit
            case scala.util.Failure(err) =>
              recordFailedKey(key, err)
      catch
        case NonFatal(t) =>
          recordFailedKey(key, t)

  /** Latch a full destination as the abort cause and raise the operator alert.
   *
   *  Idempotent in the cause: the first fatal condition to fire owns the abort. The alert is
   *  emitted every time, because a disk that fills during the wind-down is still news.
   */
  private def latchOutOfSpace(err: Throwable): Unit =
    synchronized:
      if abortCause.isEmpty then
        abortCause = Some(err)
    logger.error(s"REBUILD OUT OF SPACE: store $storeId cannot fit on device " +
                 s"$storageDeviceId at $devicePath. The store will remain Rebuilding " +
                 s"until capacity is added. Underlying error: $err")

  /** recordFailure keyed by an allocation-tree key rather than by an ObjectId.
   *
   *  Deriving the id is itself a throw site -- ObjectId.apply reads two longs and throws
   *  BufferUnderflowException on anything shorter -- and on restoreObject's synchronous failure
   *  path that throw would escape the very catch that exists to keep the walk from wedging. A
   *  key that is not sixteen bytes is not an object entry at all: there is nothing to restore
   *  and nothing a retry could name, so it is logged and skipped rather than recorded.
   */
  private def recordFailedKey(key: Key, err: Throwable): Future[Unit] =
    if key.bytes.length == ObjectIdKeyLength then
      recordFailure(ObjectId(key.bytes), err)
    else
      logger.warn(s"Rebuild of $storeId: allocation tree entry whose ${key.bytes.length}-byte " +
                  s"key is not an object id; skipping. Underlying error: $err")
      Future.unit

  /** Best-effort detection of a full destination. Backends surface it differently -- RocksDB
   *  wraps it, the JDK throws IOException -- so this matches on the message as well as the
   *  type. A false negative only costs the slower path through recordFailure. */
  private def isOutOfSpace(err: Throwable): Boolean =
    def matches(t: Throwable): Boolean =
      val msg = Option(t.getMessage).getOrElse("").toLowerCase
      msg.contains("no space left") || msg.contains("disk full") ||
        msg.contains("insufficient space") || msg.contains("injected out of space")

    Iterator.iterate(err)(_.getCause).takeWhile(_ != null).take(8).exists(matches)

  private def attemptRestore(key: Key, ptr: ObjectPointer): Future[Unit] =
    if failBytes.contains(key.bytes.toList) then
      Future.failed(new Exception(s"injected read failure for $key"))
    else if outOfSpaceBytes.contains(key.bytes.toList) then
      Future.failed(new Exception(s"injected out of space error for $key"))
    else
      restore(ptr)

  private def restore(ptr: ObjectPointer): Future[Unit] =
    def metadataOf(os: ObjectState): (ObjectType.Value, Metadata) = os match
      case kvos: KeyValueObjectState =>
        (ObjectType.KeyValue, Metadata(kvos.revision, kvos.refcount, kvos.timestamp))
      case dos: DataObjectState =>
        (ObjectType.Data, Metadata(dos.revision, dos.refcount, dos.timestamp))
      case _: MetadataObjectState =>
        throw new Exception(s"Unsupported object type for rebuild: $ptr")

    val fos = ptr match
      case p: KeyValueObjectPointer => client.read(p)
      case p: DataObjectPointer => client.read(p)

    fos.map: os =>
      val (objectType, metadata) = metadataOf(os)
      val localData = os.getRebuildDataForStore(storeId)
      backend.rebuildWrite(os.id, objectType, metadata, localData.getOrElse(DataBuffer()))

  private def recordRestored(key: Key): Future[Unit] =
    val checkpointNow = synchronized:
      if testingOnlyTrackRestoredKeys then
        restoredKeys += key
      lastKey = Some(key)
      restoredSinceCheckpoint += 1
      if restoredSinceCheckpoint >= checkpointInterval then
        restoredSinceCheckpoint = 0
        true
      else
        false

    if checkpointNow then checkpoint()
    Future.unit

  private def recordFailure(objectId: ObjectId, err: Throwable): Future[Unit] =
    val over = synchronized:
      failed = objectId :: failed
      failed.size > maxFailedObjects

    logger.warn(s"Rebuild of $storeId: failed to read object $objectId: $err")

    if over then
      // Something systemic is wrong -- the pool below its read threshold, most likely -- and
      // continuing only burns I/O. Abort with the checkpoint intact; the next device check
      // retries. Set the abort latch so the rest of the walk is a no-op.
      // Latch first, then checkpoint. If the checkpoint throws, the throw must not unwind past
      // the latch.
      val cause = new Exception(
        s"Rebuild of $storeId aborted: more than $maxFailedObjects unreadable objects")
      synchronized:
        if abortCause.isEmpty then
          abortCause = Some(cause)
      try checkpoint()
      catch case t: Throwable => logger.warn(s"Rebuild of $storeId: checkpoint after max failures failed: $t")
      Future.unit
    else
      Future.unit

  /** Flush FIRST, then write the checkpoint. The reverse order would let a crash between the
   *  two produce a checkpoint claiming objects that never reached stable storage. */
  private def checkpoint(): Unit =
    flushBackend()
    saveCheckpoint()

  private def flushBackend(): Unit =
    backend.rebuildFlush()
    synchronized(checkpointTrace += "flush")

  private def saveCheckpoint(): Unit =
    val (k, f) = synchronized((lastKey, failed))
    RebuildState.save(stagingPath, RebuildState(storeId, k, f))
    synchronized(checkpointTrace += "checkpoint")

  /** Retry everything the walk could not read. Anything still failing leaves the store
   *  Rebuilding for the next device check to pick up. */
  private def retryFailures(): Future[Unit] =
    val pending = synchronized:
      val f = failed
      failed = Nil
      f

    if pending.isEmpty then
      Future.unit
    else
      logger.info(s"Rebuild of $storeId: retrying ${pending.size} unreadable objects")
      val pool = client.getStoragePool(storeId.poolId)
      val retries = pending.map: objectId =>
        val key = Key(objectId.toBytes)
        pool.flatMap(_.allocationTree.get(key)).flatMap:
          case Some(vs) => attemptRestore(key, ObjectPointer(vs.value.bytes))
          case None => Future.unit // deleted since the walk saw it; nothing to restore
        .recover:
          case err =>
            synchronized:
              failed = objectId :: failed
            logger.warn(s"Rebuild of $storeId: retry of $objectId failed: $err")

      Future.sequence(retries).flatMap: _ =>
        val remaining = synchronized(failed)
        if remaining.isEmpty then
          Future.unit
        else
          checkpoint()
          Future.failed(new Exception(
            s"Rebuild of $storeId incomplete: ${remaining.size} objects still unreadable"))

  /** Flush, close, drop the checkpoint, move into place. */
  private def finish(): Future[Unit] =
    backend.rebuildFlush()
    closeBackend().map: _ =>
      // The temp file only exists if a crash caught a save between its write and its rename, and
      // only survives if this pass never checkpointed. Dropping it here keeps it out of the
      // directory that is about to become a live store.
      RebuildState.removeAll(stagingPath)

      os.move(stagingPath, finalPath)
      logger.info(s"Rebuild of $storeId: store in place at $finalPath")
