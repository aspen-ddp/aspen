package org.aspen_ddp.aspen.server.rebuild

import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.host.Phase
import org.aspen_ddp.aspen.server.store.backend.BackendOpener

import java.util.UUID
import scala.concurrent.Future

/** Reconstruction of one store from the rest of its pool.
 *
 *  `complete` resolves once the rebuilt store is in place in `<device>/rebuilt/`, Rebuilt with its episode, with the
 *  copy id it took there. Recording the copy, flipping the device entry from Rebuilding to Active and moving the copy
 *  Live are Host's job, exactly as they are for a transfer in.
 */
trait StoreRebuild:
  val storeId: StoreId
  def complete: Future[UUID]

  /** Stops the rebuild, leaving its staging directory and checkpoint in place as a crash would
   *  so a later rebuild resumes. `complete` then fails with `RebuildingStore.RebuildCancelled`
   *  (unless it had already finished). */
  def cancel(): Unit

/** Constructs a store rebuild, whose staging store is opened through `backends`. The default `Filesystem` factory
 *  produces the real implementation. Tests inject a factory that records the request and resolves on command.
 *  `existingCopy` is the copy of the store on the device outside pending-delete/, with its phase and path, as the copy
 *  registry recorded it when the rebuild was started (StoreRebuilds.startRebuild, on the control thread) */
trait StoreRebuildFactory:
  def createRebuild(client: AspenClient,
                    storeId: StoreId,
                    storageDeviceId: StorageDeviceId,
                    devicePath: os.Path,
                    backends: BackendOpener,
                    episode: Option[UUID],
                    existingCopy: Option[(Phase, os.Path)]): StoreRebuild

object StoreRebuildFactory:
  object Filesystem extends StoreRebuildFactory:
    def createRebuild(client: AspenClient,
                      storeId: StoreId,
                      storageDeviceId: StorageDeviceId,
                      devicePath: os.Path,
                      backends: BackendOpener,
                      episode: Option[UUID],
                      existingCopy: Option[(Phase, os.Path)]): StoreRebuild =
      new RebuildingStore(client, storeId, storageDeviceId, devicePath, backends, existingCopy, episode)
