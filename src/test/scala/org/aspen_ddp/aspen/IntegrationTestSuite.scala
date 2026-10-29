package org.aspen_ddp.aspen

import org.aspen_ddp.aspen.client.{AspenClient, RegisteredTypeFactory}
import org.aspen_ddp.aspen.common.ida.{IDA, Replication}
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceState, StoragePoolState}
import org.aspen_ddp.aspen.common.objects.{Insert, KeyValueObjectPointer}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.{DoesNotExist, KeyRevision}
import org.aspen_ddp.aspen.common.util.long2byte
import org.scalatest.{FutureOutcome, Tag, compatible}
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.{Duration, MILLISECONDS}

class IntegrationTestSuite  extends AsyncFunSuite with Matchers { //with BeforeAndAfter {
  var net: TestNetwork = scala.compiletime.uninitialized
  var client: AspenClient = scala.compiletime.uninitialized
  var radicle: KeyValueObjectPointer = scala.compiletime.uninitialized
  var testName: String = "NO_TEST"

  /** Override to supply additional type factories to the TestNetwork's TypeRegistry. */
  def userTypeFactories: List[RegisteredTypeFactory] = Nil

  /** Override to bootstrap the test network with a different IDA. The store count follows the
    * width, so no other change is needed to run a suite against a wider pool. */
  def testIda: IDA = Replication(3, 2)

  /** Override to give the bootstrap pool a maxSliceSize. */
  def testPoolMaxSliceSize: Option[Int] = None

  /** Override to give the bootstrap pool's backend a maxSliceSize (RocksDBConfig.maxSliceSize). */
  def testBackendMaxSliceSize: Option[Int] = None

  /** Every store's effective slice size limit (what Host derives from the store config file). Defaults to the
    * smaller of the pool and backend limits, as Host computes it; override only to model misconfigured stores.
    */
  def testStoreMaxSliceSize: Option[Long] =
    (testPoolMaxSliceSize.toList ++ testBackendMaxSliceSize.toList).minOption.map(_.toLong)

  def subFixtureSetup(): Unit = {}
  def subFixtureTeardown(): Unit = ()

  inline def atest(testName: String, testTags: Tag*)(testFun: => Future[compatible.Assertion]): Unit = {
    test(testName, testTags*):
      testFun.flatMap: result =>
        net.waitForTransactionsToComplete().map: _ =>
          result
  }

  override def withFixture(test: NoArgAsyncTest): FutureOutcome = {
    net = new TestNetwork(executionContext, userTypeFactories, testIda, testPoolMaxSliceSize, testStoreMaxSliceSize,
      testBackendMaxSliceSize)
    client = net.client
    testName = test.name
    radicle = net.radicle
    client.setSystemAttribute("unittest.name", test.name)

    subFixtureSetup()

    complete {
      super.withFixture(test)
    } lastly {

      subFixtureTeardown()
      client.shutdown()

      net = null
      client = null
      radicle = null
      testName = "NO_TEST"
    }
  }

  def waitForTransactionsToComplete(): Future[Unit] = net.waitForTransactionsToComplete()

  def handleEvents(): Unit = net.handleEvents()

  /** Rewrite one device's state object in a single revision-guarded transaction. Metadata only:
    * no Host reacts to it. */
  def setDeviceState(id: StorageDeviceId)(f: StorageDeviceState => StorageDeviceState): Future[Unit] =
    given scala.concurrent.ExecutionContext = executionContext
    client.transactUntilSuccessful: tx =>
      for
        ptr <- client.getStorageDevicePointer(id)
        kvos <- client.read(ptr)
      yield
        val updated = f(StorageDeviceState(kvos))
        tx.update(ptr, None, None,
          List(KeyRevision(StorageDeviceState.StateKey, kvos.contents(StorageDeviceState.StateKey).revision)),
          List(Insert(StorageDeviceState.StateKey, updated.encode())))

  /** Record `size` as the store's size in its pool's per-store usage accounting, as the host's
    * usage tracker would. A store index no host reports on keeps the tracker from overwriting it. */
  def setStoreSize(storeId: StoreId, size: Long): Future[Unit] =
    given scala.concurrent.ExecutionContext = executionContext
    val key = StoragePoolState.getStoreUsageKey(storeId.poolIndex)
    client.transactUntilSuccessful: tx =>
      for
        ptr <- client.getStoragePoolPointer(storeId.poolId)
        kvos <- client.read(ptr)
      yield
        val req = kvos.contents.get(key) match
          case Some(vs) => KeyRevision(key, vs.revision)
          case None => DoesNotExist(key)
        tx.update(ptr, None, None, List(req), List(Insert(key, long2byte(size))))

  /** Writes what shutdownStorageDevice writes -- Active stores Offline, the device ShuttingDown --
    * without its pool write-threshold check. For tests whose device holds a pool's only stores,
    * such as the bootstrap device, which the real request refuses to shut down. No nudge. */
  def forceShuttingDown(id: StorageDeviceId): Future[Unit] =
    setDeviceState(id)(_.withActiveStoresOffline.copy(status = StorageDeviceState.DeviceStatus.ShuttingDown))

}
