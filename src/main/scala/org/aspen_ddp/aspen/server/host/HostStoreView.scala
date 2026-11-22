package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.store.StoreId

/** An immutable snapshot of what the host serves, for readers on other threads (rule 3 in HostThreading.scala).
 *  DeviceControl.publishView builds it on the control thread after every change to the state it reflects and
 *  publishes it through a `@volatile` field; readers take `DeviceControl.currentView`.
 *
 *  It may lag the control thread slightly in production. Everything routed by it is rechecked when handled: a repair
 *  of a store that has left fails with StoreNotHosted there.
 *
 *  @param served the stores loaded on this host
 *  @param offline the stores marked silent: their messages are dropped rather than answered UnknownStore
 *  @param repairable the served stores a repair scan may run against (HostCopies.repairable): not closing, with no
 *         rebuild or transfer out here, and not being sent out
 *  @param devicePaths the directory of each loaded storage device
 *  @param storeDevices the device each served store is loaded from (the repair fence: RepairTarget.localDevice) */
final case class HostStoreView(served: Set[StoreId],
                               offline: Set[StoreId],
                               repairable: Set[StoreId],
                               devicePaths: Map[StorageDeviceId, os.Path],
                               storeDevices: Map[StoreId, StorageDeviceId])

object HostStoreView:
  val Empty: HostStoreView = HostStoreView(Set(), Set(), Set(), Map(), Map())

  /** The view of these inputs. `repairable` is taken as given (HostCopies.repairable): a rebuild already reads
   *  everything, and a store transferring out is about to stop being ours. */
  def of(served: Set[StoreId],
         offline: Set[StoreId],
         repairable: Set[StoreId],
         devicePaths: Map[StorageDeviceId, os.Path],
         storeDevices: Map[StoreId, StorageDeviceId]): HostStoreView =
    HostStoreView(served, offline, repairable, devicePaths, storeDevices)
