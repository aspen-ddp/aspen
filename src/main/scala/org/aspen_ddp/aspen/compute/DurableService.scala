package org.aspen_ddp.aspen.compute

import org.aspen_ddp.aspen.common.network.ServiceMessage

/** A long-running service that runs on exactly one host at a time and moves to another if that host fails.
 *
 *  Services are registered in the services TKVL rooted in the Radicle. Each entry names the service type, the
 *  owning host and its lease expiry, and points to the service's state object. A DurableServiceExecutor runs on
 *  every host; the implementation is SimpleDurableServiceExecutor.
 *
 *  Ownership:
 *    - A host claims an unclaimed or expired entry by writing its own HostId and a new lease expiry, conditional
 *      on the entry's revision. Of several hosts racing for one entry, exactly one commits.
 *    - Only hosts with a DurableServiceFactory registered for the entry's type attempt the claim.
 *    - The owner renews the lease every few seconds. A host that finds the entry no longer names it, or its lease
 *      expired, calls shutdown() on its instance.
 *
 *  Scanning (how orphaned services are found):
 *    - Every host periodically reads the whole TKVL looking for entries to claim. That read goes through the
 *      Radicle, so its total cost grows with the number of hosts scanning.
 *    - The goal is that some host in the cluster scans roughly every 15 seconds, so an expired lease is noticed
 *      about that long after it expires. More frequent scanning only adds load.
 *    - So each host draws its delay to the next scan uniformly from [M/2, 3M/2], with M = hostCount * 15s.
 *      If each of N hosts scans on average every M, the cluster as a whole scans on average every M/N = 15s.
 *    - The window never drops below 10-30s, which is also what is used when the host count is unknown (0), so
 *      small clusters scan as often as a single host would.
 *    - The host count is the network layer's count of known hosts (MetadataManager's host map, which is seeded
 *      from and written through to the NetworkCache). It is re-read hourly. A changed count applies from the next
 *      scan; the pending scan is re-armed only when it is due later than the new window allows (the cluster
 *      shrank).
 *    - A consequence: on a large cluster a host's first scan after starting can be hours away. Claiming is
 *      cluster-wide, so another host picks the services up in the meantime.
 *
 *  See SimpleDurableServiceExecutor.scanWindow for the calculation; the intervals are constructor parameters.
 */
trait DurableService:
  /** Called by the executor when this host loses its lease.
   *  The service must stop operating immediately. */
  def shutdown(): Unit

  /** Called by the executor when a ServiceMessage arrives for this service.
   *  Default is a no-op; services that consume messages override this. */
  def receiveMessage(msg: ServiceMessage): Unit = ()
