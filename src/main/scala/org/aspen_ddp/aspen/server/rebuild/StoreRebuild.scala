package org.aspen_ddp.aspen.server.rebuild

import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.store.StoreId

import scala.concurrent.Future

/** Reconstruction of one store from the rest of its pool.
 *
 *  `complete` resolves once the rebuilt store is in place at
 *  `<device>/<storeId.directoryName>`. Flipping the device entry from Rebuilding to Active and
 *  loading the store are Host's job, exactly as they are for a transfer in.
 */
trait StoreRebuild:
  val storeId: StoreId
  def complete: Future[Unit]

  /** Stops the rebuild, leaving its staging directory and checkpoint in place as a crash would
   *  so a later rebuild resumes. `complete` then fails with `RebuildingStore.RebuildCancelled`
   *  (unless it had already finished). */
  def cancel(): Unit

/** Constructs a store rebuild. The default `Filesystem` factory produces the real
 *  implementation. Tests inject a factory that records the request and resolves on command. */
trait StoreRebuildFactory:
  def createRebuild(client: AspenClient,
                    storeId: StoreId,
                    storageDeviceId: StorageDeviceId,
                    devicePath: os.Path): StoreRebuild

object StoreRebuildFactory:
  object Filesystem extends StoreRebuildFactory:
    def createRebuild(client: AspenClient,
                      storeId: StoreId,
                      storageDeviceId: StorageDeviceId,
                      devicePath: os.Path): StoreRebuild =
      new RebuildingStore(client, storeId, storageDeviceId, devicePath)
