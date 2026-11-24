package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.client.{AspenClient, StopRetrying}
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.common.objects.Insert
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision

import scala.concurrent.{ExecutionContext, Future}

/** Writes a device's health into its StorageDeviceState record (device hang detection, spec section 5.3). The judge
 *  (DeviceHealthJudge) starts a publish while the device's health differs from the one last published */
trait DeviceHealthPublisher:
  /** Reads the device's record and writes `current()` (read inside the transaction body) into it, unless the
   *  record already holds it or must not be written. Never runs on the device. */
  def publish(id: StorageDeviceId, current: () => DeviceHealth): Future[DeviceHealthPublisher.Outcome]

object DeviceHealthPublisher:
  enum Outcome:
    /** Committed */
    case Written(h: DeviceHealth)
    /** The record already held `h`: no transaction */
    case Unchanged(h: DeviceHealth)
    /** The record's status is Failed, or it names another host: no transaction */
    case Skipped

/** Publishes through `client`, on its client context. The write is an Insert of StateKey under a KeyRevision on it,
 *  as StorageDeviceUsageManager.updateDeviceUsage writes usage, so a racing writer of the record collides and
 *  retries instead of losing either's fields (each writes `state.copy`). Each attempt reads `current()` afresh, so a
 *  later transition never loses to an earlier publish. An Unchanged or Skipped attempt adds nothing to its
 *  transaction, and an empty transaction commits without being sent */
final class ClientDeviceHealthPublisher(client: AspenClient, hostId: HostId) extends DeviceHealthPublisher:
  import DeviceHealthPublisher.Outcome

  def publish(id: StorageDeviceId, current: () => DeviceHealth): Future[Outcome] =
    given ExecutionContext = client.clientContext
    client.transactUntilSuccessfulWithRecovery(StopRetrying.unlessCollision): tx =>
      for
        ptr <- client.getStorageDevicePointer(id)
        kvos <- client.read(ptr)
      yield
        val state = StorageDeviceState(kvos)
        val h = current()
        if state.isFailed || state.hostId != hostId then
          Outcome.Skipped
        else if state.health == h.status && (h.since == HLCTimestamp.Zero || state.healthSince == h.since) then
          Outcome.Unchanged(h)
        else
          val since = if h.since == HLCTimestamp.Zero then HLCTimestamp.now else h.since
          val updated = state.copy(health = h.status, healthSince = since, healthEvidence = h.evidence)
          val requirements = List(KeyRevision(StorageDeviceState.StateKey,
            kvos.contents(StorageDeviceState.StateKey).revision))
          tx.update(ptr, None, None, requirements, List(Insert(StorageDeviceState.StateKey, updated.encode())))
          Outcome.Written(h)
