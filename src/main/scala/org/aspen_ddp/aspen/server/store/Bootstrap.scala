package org.aspen_ddp.aspen.server.store

import java.util.UUID
import org.aspen_ddp.aspen.client.{Host, HostId, StoragePool}
import org.aspen_ddp.aspen.client.internal.pool.SimpleStoragePool
import org.aspen_ddp.aspen.client.tkvl.{BootstrapPoolNodeAllocator, Root}
import org.aspen_ddp.aspen.common.{HLCTimestamp, Radicle}
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.objects.{ByteArrayKeyOrdering, Insert, Key, KeyValueObjectPointer, KeyValueOperation, LexicalKeyOrdering, Metadata, ObjectId, ObjectRefcount, ObjectRevision, ObjectType, Value}
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.server.store.backend.Backend
import org.aspen_ddp.aspen.common.util.uuid2byte

object Bootstrap {

    def initialize(ida: IDA, 
                   stores: List[Backend], 
                   hostNames: List[(String, UUID)]): KeyValueObjectPointer = {
      
      require( ida.width == stores.length )

      val bootstrapMetadata = Metadata(
        ObjectRevision(TransactionId(new UUID(0,0))),
        ObjectRefcount(1,1),
        HLCTimestamp.now
      )

      def allocate(content: List[(Key, Array[Byte])] = Nil,
                   objectId: Option[ObjectId] = None): KeyValueObjectPointer = {

        val oid = objectId.getOrElse(ObjectId(UUID.randomUUID()))

        val contents = content.map { t =>
          t._1 -> new ValueState(Value(t._2), bootstrapMetadata.revision, bootstrapMetadata.timestamp, None)
        }.toMap

        val storePointers = KVObjectState.encodeIDA(ida, None, None, None, None, contents).zip(stores).map { t =>
          val (storeData, store) = t

          store.bootstrapAllocate(oid, ObjectType.KeyValue, bootstrapMetadata, storeData)
        }

        KeyValueObjectPointer(oid, Radicle.poolId, None, ida, storePointers)
      }

      def overwrite(pointer: KeyValueObjectPointer,
                    content: List[(Key, Array[Byte])]): Unit = {

        val contents = content.map { t =>
          t._1 -> new ValueState(Value(t._2), bootstrapMetadata.revision, bootstrapMetadata.timestamp, None)
        }.toMap

        KVObjectState.encodeIDA(ida, None, None, None, None, contents).zip(stores).foreach { t =>
          val (storeData, store) = t
          val sp = pointer.getStorePointer(store.storeId).get
          store.bootstrapOverwrite(pointer.id, sp, storeData)
        }
      }

      val errTreeRoot = allocate()
      val allocTreeRoot = allocate()

      val storeHosts = (0 until ida.width).map(idx => HostId(new UUID(0,idx))).toArray

      val poolConfig = SimpleStoragePool.encode(Radicle.poolId, "bootstrap", ida.width, ida, storeHosts, None)
      val errorTree = Root(0, ByteArrayKeyOrdering, Some(errTreeRoot), BootstrapPoolNodeAllocator).encode()
      val allocTree = Root(0, ByteArrayKeyOrdering, Some(allocTreeRoot), BootstrapPoolNodeAllocator).encode()

      val pool = allocate(List(StoragePool.ConfigKey -> poolConfig,
                               StoragePool.ErrorTreeKey -> errorTree,
                               StoragePool.AllocationTreeKey -> allocTree))

      val poolTreeRoot = allocate(List(Key(Radicle.poolId.uuid) -> pool.toArray))
      val poolTree = Root(0,
        ByteArrayKeyOrdering,
        Some(poolTreeRoot),
        BootstrapPoolNodeAllocator)

      val poolNameTreeRootObj = allocate()
      val poolNameTreeRoot = Root(0, LexicalKeyOrdering, Some(poolNameTreeRootObj), BootstrapPoolNodeAllocator).encode()
      val poolNameTree = Root(0,
        ByteArrayKeyOrdering,
        Some(poolNameTreeRootObj),
        BootstrapPoolNodeAllocator)

      val hostsTreeRootObj = allocate()
      val hostsTreeRoot = Root(0, ByteArrayKeyOrdering, Some(hostsTreeRootObj), BootstrapPoolNodeAllocator).encode()
      val hostsTree = Root(0,
        ByteArrayKeyOrdering,
        Some(hostsTreeRootObj),
        BootstrapPoolNodeAllocator)
      
      val hostsNameTreeRootObj = allocate(
        hostNames.map((name, uuid) => (Key(name), uuid2byte(uuid)))
      )
      val hostsNameTreeRoot = Root(0, LexicalKeyOrdering, Some(hostsNameTreeRootObj), BootstrapPoolNodeAllocator).encode()
      val hostsNameTree = Root(0,
        ByteArrayKeyOrdering,
        Some(hostsNameTreeRootObj),
        BootstrapPoolNodeAllocator)

      val radicleContent: List[(Key, Array[Byte])] = List(
        Radicle.PoolTreeKey -> poolTree.encode(),
        Radicle.PoolNameTreeKey -> poolNameTree.encode(),
        Radicle.HostsTreeKey -> hostsTree.encode(),
        Radicle.HostsNameTreeKey -> hostsNameTree.encode())

      val radicle = allocate(radicleContent, Some(Radicle.objectId))

      overwrite(allocTreeRoot, List(
        Key(errTreeRoot.id.uuid) -> errTreeRoot.toArray,
        Key(allocTreeRoot.id.uuid) -> allocTreeRoot.toArray,
        Key(pool.id.uuid) -> pool.toArray,
        Key(poolTreeRoot.id.uuid) -> poolTreeRoot.toArray,
        Key(poolNameTreeRootObj.id.uuid) -> poolNameTreeRootObj.toArray,
        Key(hostsTreeRootObj.id.uuid) -> hostsTreeRootObj.toArray,
        Key(hostsNameTreeRootObj.id.uuid) -> hostsNameTreeRootObj.toArray,
        Key(radicle.id.uuid) -> radicle.toArray
      ))

      if false then
        println(s"ErrorTreeRoot: ${errTreeRoot.id.uuid}")
        println(s"AllocTreeRoot: ${allocTreeRoot.id.uuid}")
        println(s"Radicle Pool : ${pool.id.uuid}")
        println(s"PoolTreeRoot : ${poolTreeRoot.id.uuid}")
        println(s"Radicle      : ${radicle.id.uuid}")

      radicle
    }



}
