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
 *  @param offline the stores marked offline: their messages are dropped rather than answered UnknownStore
 *  @param repairable the served stores that are not offline, not rebuilding and not transferring out
 *  @param devicePaths the directory of each loaded storage device */
final case class HostStoreView(served: Set[StoreId],
                               offline: Set[StoreId],
                               repairable: Set[StoreId],
                               devicePaths: Map[StorageDeviceId, os.Path])

object HostStoreView:
  val Empty: HostStoreView = HostStoreView(Set(), Set(), Set(), Map())

  /** The view of these inputs. A store is repairable when it is served and neither offline, rebuilding nor
   *  transferring out: a rebuild already reads everything, and a store transferring out is about to stop being ours. */
  def of(served: Set[StoreId],
         offline: Set[StoreId],
         rebuilding: Set[StoreId],
         transferringOut: Set[StoreId],
         devicePaths: Map[StorageDeviceId, os.Path]): HostStoreView =
    HostStoreView(served, offline, served -- offline -- rebuilding -- transferringOut, devicePaths)
