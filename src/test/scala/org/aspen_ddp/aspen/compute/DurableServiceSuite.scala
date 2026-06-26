package org.aspen_ddp.aspen.compute

import org.aspen_ddp.aspen.common.{HLCTimestamp, Radicle}
import org.aspen_ddp.aspen.common.objects.KeyValueObjectPointer
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class ServiceEntrySpec extends AnyFunSuite with Matchers:

  test("ServiceEntry round-trip encode/decode"):
    val typeUUID = UUID.randomUUID()
    val hostId = UUID.randomUUID()
    val leaseExpiry = HLCTimestamp(12345678L)
    val statePointer = KeyValueObjectPointer(Radicle.objectId, Radicle.poolId)
    val entry = ServiceEntry(typeUUID, hostId, leaseExpiry, statePointer)
    val decoded = ServiceEntry.decode(entry.encode())
    decoded.typeUUID shouldBe typeUUID
    decoded.hostId shouldBe hostId
    decoded.leaseExpiry shouldBe leaseExpiry
    decoded.statePointer.id shouldBe statePointer.id
    decoded.statePointer.poolId shouldBe statePointer.poolId
    decoded.isClaimed shouldBe true

  test("ServiceEntry zero hostId represents unclaimed"):
    val entry = ServiceEntry(UUID.randomUUID(), new UUID(0, 0), HLCTimestamp.Zero,
      KeyValueObjectPointer(Radicle.objectId, Radicle.poolId))
    val decoded = ServiceEntry.decode(entry.encode())
    decoded.hostId shouldBe new UUID(0, 0)
    decoded.leaseExpiry shouldBe HLCTimestamp.Zero
    decoded.isClaimed shouldBe false
