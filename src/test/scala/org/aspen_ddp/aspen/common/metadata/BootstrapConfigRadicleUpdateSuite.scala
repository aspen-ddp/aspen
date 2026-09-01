package org.aspen_ddp.aspen.common.metadata

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class BootstrapConfigRadicleUpdateSuite extends IntegrationTestSuite:

  atest("prepRadicleUpdate rewrites the radicle config to name the new host"):
    given ExecutionContext = executionContext
    val storeId = StoreId(PoolId.BootstrapPoolId, 0.toByte)
    for
      newHostId <- client.createHost("second-host", "127.0.0.1", 5100, 5101, 5102)
      _ <- waitForTransactionsToComplete()
      poolCfg <- client.getStoragePoolState(PoolId.BootstrapPoolId)
      _ <- client.transactUntilSuccessful: tx =>
             given Transaction = tx
             BootstrapConfig.prepRadicleUpdate(client, storeId, poolCfg, newHostId)
      _ <- waitForTransactionsToComplete()
      yaml <- client.getBootstrapConfig()
    yield
      val cfg = BootstrapConfig.parseBootstrapConfig(yaml)
      // Store 0 has moved to the new host; the other two are untouched.
      val hostOfStore0 = cfg.hosts.find(_.stores.contains(storeId)).map(_.hostId)
      hostOfStore0 should be(Some(newHostId))
      cfg.hosts.map(_.stores.length).sum should be(3)

  atest("prepRadicleUpdate is a no-op for a non-bootstrap pool"):
    given ExecutionContext = executionContext
    val storeId = StoreId(PoolId(UUID.randomUUID()), 0.toByte)
    for
      before <- client.getBootstrapConfig()
      poolCfg <- client.getStoragePoolState(PoolId.BootstrapPoolId)
      staged <- {
        given Transaction = client.newTransaction()
        BootstrapConfig.prepRadicleUpdate(client, storeId, poolCfg, HostId.BootstrapHostId)
      }
      after <- client.getBootstrapConfig()
    yield
      staged should be(())
      after should be(before)
