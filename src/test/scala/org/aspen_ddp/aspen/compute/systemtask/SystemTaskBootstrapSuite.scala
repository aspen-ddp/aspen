package org.aspen_ddp.aspen.compute.systemtask

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.objects.Key
import org.aspen_ddp.aspen.compute.ServiceEntry

import scala.concurrent.ExecutionContext

class SystemTaskBootstrapSuite extends IntegrationTestSuite:

  atest("bootstrap registers SystemTaskExecutorService with an empty task tree"):
    given ExecutionContext = executionContext
    val servicesTkvl = TieredKeyValueList(client,
      KVObjectRootManager(client, Radicle.ServicesTreeKey, Radicle.pointer))
    for
      entryVs <- servicesTkvl.get(Key(SystemTaskExecutorService.ServiceUUID))
      entry = ServiceEntry.decode(entryVs.get.value.bytes)
      statePtr = entry.statePointer
      stateKvos <- client.read(statePtr)
      // The task tree hosted in the service state object must be present and empty.
      taskTkvl = TieredKeyValueList(client,
        KVObjectRootManager(client, SystemTaskExecutorService.TaskTreeKey, statePtr))
      count <- {
        var n = 0
        taskTkvl.foreach((_, _, _) => { n += 1; scala.concurrent.Future.unit }).map(_ => n)
      }
    yield
      entry.typeUUID shouldBe SystemTaskExecutorService.ServiceTypeUUID
      stateKvos.contents.contains(SystemTaskExecutorService.TaskTreeKey) shouldBe true
      count shouldBe 0
