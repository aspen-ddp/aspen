package org.aspen_ddp.aspen.common.metadata.management

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.TypeFactories
import org.aspen_ddp.aspen.compute.DurableTaskFactory

import scala.concurrent.ExecutionContext

class MigratePoolToSetSuite extends IntegrationTestSuite:

  atest("MigratePoolToSetDurableTask is registered in the client type registry"):
    given ExecutionContext = executionContext
    // SystemTaskExecutorService reconstructs a task by looking its type UUID up here; an
    // unregistered factory makes prepareSystemDurableTask StopRetrying.
    val factory = client.typeRegistry.getType[DurableTaskFactory](
      MigratePoolToSetDurableTask.typeUUID)
    factory should be(Some(MigratePoolToSetDurableTask))
    TypeFactories.factories should contain(MigratePoolToSetDurableTask)
