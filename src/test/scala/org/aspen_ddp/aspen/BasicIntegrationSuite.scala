package org.aspen_ddp.aspen

import org.aspen_ddp.aspen.client.{KeyValueObjectState, Transaction}
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.objects.{Insert, Key, ObjectRevision, ObjectRevisionGuard, Value}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate

import scala.concurrent.Future
import scala.language.implicitConversions

class BasicIntegrationSuite extends IntegrationTestSuite {

  test("Read radicle") {
    client.read(radicle).map( kvos => kvos.contents.isEmpty should be (false) )
  }

  test("Insert key value pair into radicle") {
    val key = Key(Array[Byte](100))
    val value = Value(Array[Byte](2))

    def update(kvos: KeyValueObjectState): Future[Unit] = {
      val tx = client.newTransaction()
      tx.update(radicle,
        Some(kvos.revision),
        None,
        List(KeyValueUpdate.DoesNotExist(key)),
        List(Insert(key, value.bytes)))

      tx.commit().map(_=>())
    }

    for {
      ikvos <- client.read(radicle)
      _ <- update(ikvos)
      kvos <- client.read(radicle)
    } yield {
      kvos.contents.isEmpty should be (false)
      kvos.contents.contains(key) should be (true)
      kvos.contents(key).value.bytes.length should be (1)
      kvos.contents(key).value.bytes(0) should be (2)
    }
  }

  test("Allocate data object") {
    val key = Key(Array[Byte](100))
    val value = Value(Array[Byte](2))

    given tx: Transaction = client.newTransaction()

    for {
      ikvos <- client.read(radicle)

      _ = tx.update(radicle,
        Some(ikvos.revision),
        None,
        List(KeyValueUpdate.DoesNotExist(key)),
        List(Insert(key, value.bytes)))

      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.get.createAllocator(Replication(3,2))

      dp <- alloc.allocateDataObject(ObjectRevisionGuard(radicle, ikvos.revision), Array[Byte](0))

      _ <- tx.commit().map(_=>())

      kvos <- client.read(radicle)
      dos <- client.read(dp)
    } yield {
      kvos.contents.isEmpty should be (false)
      kvos.contents.contains(key) should be (true)
      kvos.contents(key).value.bytes.length should be (1)
      kvos.contents(key).value.bytes(0) should be (2)
      dos.data.size should be (1)
    }
  }

  test("Allocate KeyValue object") {
    val key = Key(Array[Byte](100))
    val value = Value(Array[Byte](2))

    given tx: Transaction = client.newTransaction()

    tx.update(radicle,
      None,
      None,
      List(KeyValueUpdate.DoesNotExist(key)),
      List(Insert(key, value.bytes)))

    for {
      ikvos <- client.read(radicle)

      pool <- client.getStoragePool(Radicle.poolId)

      alloc = pool.get.createAllocator(Replication(3,2))

      kp <- alloc.allocateKeyValueObject(ObjectRevisionGuard(radicle, ikvos.revision), Map(key -> value))

      _ <- tx.commit().map(_=>())

      kvos <- client.read(radicle)
      kvos2 <- client.read(kp)
    } yield {
      kvos.contents.isEmpty should be (false)
      kvos.contents.contains(key) should be (true)
      kvos.contents(key).value.bytes.length should be (1)
      kvos.contents(key).value.bytes(0) should be (2)

      kvos2.contents.isEmpty should be (false)
      kvos2.contents.contains(key) should be (true)
      kvos2.contents(key).value.bytes.length should be (1)
      kvos2.contents(key).value.bytes(0) should be (2)
    }
  }

  test("Allocate and update data object") {
    val key = Key(Array[Byte](100))
    val value = Value(Array[Byte](2))

    given tx: Transaction = client.newTransaction()

    for {
      ikvos <- client.read(radicle)

      _ = tx.update(radicle,
        Some(ikvos.revision),
        None,
        List(KeyValueUpdate.DoesNotExist(key)),
        List(Insert(key, value.bytes)))

      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.get.createAllocator(Replication(3,2))

      dp <- alloc.allocateDataObject(ObjectRevisionGuard(radicle, ikvos.revision), Array[Byte](0))

      _ <- tx.commit().map(_=>())

      kvos <- client.read(radicle)

      tx2 = client.newTransaction()
      _ = tx2.overwrite(dp, ObjectRevision(tx.id), Array[Byte](5,6))

      _ <- tx2.commit()

      dos <- client.read(dp)
    } yield {
      kvos.contents.isEmpty should be (false)
      kvos.contents.contains(key) should be (true)
      kvos.contents(key).value.bytes.length should be (1)
      kvos.contents(key).value.bytes(0) should be (2)
      dos.data.size should be (2)
      dos.data.get(0) should be (5)
      dos.data.get(1) should be (6)
    }
  }
}
