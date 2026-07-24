package org.aspen_ddp.aspen.common.network

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.network.implementations.zmqnet.ProtobufMessageCodec

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class ExecuteSystemTaskCodecSuite extends IntegrationTestSuite:

  atest("ExecuteSystemTask round-trips through the wire codec"):
    given ExecutionContext = executionContext
    val msg = ExecuteSystemTask(
      toHost = HostId(UUID.randomUUID()),
      fromClient = client.clientId,
      taskId = UUID.randomUUID(),
      taskStatePointer = net.radicle)  // net.radicle is a valid KeyValueObjectPointer

    val decoded = ProtobufMessageCodec.decodeMessage(ProtobufMessageCodec.encodeMessage(msg)).get
      .asInstanceOf[ExecuteSystemTask]

    Future.successful:
      decoded.toHost shouldBe msg.toHost
      decoded.fromClient shouldBe msg.fromClient
      decoded.taskId shouldBe msg.taskId
      decoded.taskStatePointer.toArray shouldBe msg.taskStatePointer.toArray
