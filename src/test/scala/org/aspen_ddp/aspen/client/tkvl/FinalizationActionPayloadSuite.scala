package org.aspen_ddp.aspen.client.tkvl

import java.util.UUID

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Key, KeyValueObjectPointer, ObjectId, ObjectPointer}
import org.aspen_ddp.aspen.common.pool.PoolId

import scala.concurrent.Future

class FinalizationActionPayloadSuite extends IntegrationTestSuite:

  private val poolId = PoolId(new UUID(0, 5))
  private val minimum = Key(Array[Byte](7, 8))
  private val kvNode = KeyValueObjectPointer(ObjectId(new UUID(1, 2)), poolId, Array[Byte](4, 5))
  private val dataNode = DataObjectPointer(ObjectId(new UUID(3, 4)), poolId, Array[Byte](1, 2, 3))

  // Offset of the kind byte: root manager type UUID, then the tier
  private val kindOffset = 8 + 8 + 4

  private def mgr = new KVObjectRootManager(client, Key(Array[Byte](0)), radicle)

  private def checkPointer(decoded: ObjectPointer, expected: ObjectPointer): Unit =
    assert(TreeKind.of(decoded) == TreeKind.of(expected))
    assert(decoded.id == expected.id)
    assert(decoded.poolId == expected.poolId)
    assert(decoded.storePointer.toList == expected.storePointer.toList)

  private def split(node: ObjectPointer): SplitFinalizationAction =
    val payload = SplitFinalizationAction.encode(mgr, 3, minimum, node)
    assert(payload(kindOffset) == TreeKind.of(node).code)
    SplitFinalizationAction.createFinalizationAction(client, null, payload).asInstanceOf[SplitFinalizationAction]

  private def join(node: ObjectPointer): JoinFinalizationAction =
    val payload = JoinFinalizationAction.encode(mgr, 3, minimum, node)
    assert(payload(kindOffset) == TreeKind.of(node).code)
    JoinFinalizationAction.createFinalizationAction(client, null, payload).asInstanceOf[JoinFinalizationAction]

  for node <- List(kvNode, dataNode) do
    val kind = TreeKind.of(node)

    atest(s"Split payload round-trips a $kind node"):
      val a = split(node)
      assert(a.tier == 3)
      assert(a.newMinimum.bytes.toList == minimum.bytes.toList)
      checkPointer(a.newNode, node)
      assert(a.rootManager.encode().toList == mgr.encode().toList)
      Future.successful(succeed)

    atest(s"Join payload round-trips a $kind node"):
      val a = join(node)
      assert(a.tier == 3)
      assert(a.deleteMinimum.bytes.toList == minimum.bytes.toList)
      checkPointer(a.deleteNode, node)
      assert(a.rootManager.encode().toList == mgr.encode().toList)
      Future.successful(succeed)

  atest("A payload whose kind does not match its node pointer is rejected"):
    val splitPayload = SplitFinalizationAction.encode(mgr, 1, minimum, kvNode)
    splitPayload(kindOffset) = TreeKind.DataObjects.code
    val joinPayload = JoinFinalizationAction.encode(mgr, 1, minimum, dataNode)
    joinPayload(kindOffset) = TreeKind.KeyValueObjects.code
    assertThrows[IllegalArgumentException](SplitFinalizationAction.createFinalizationAction(client, null, splitPayload))
    assertThrows[IllegalArgumentException](JoinFinalizationAction.createFinalizationAction(client, null, joinPayload))
    Future.successful(succeed)
