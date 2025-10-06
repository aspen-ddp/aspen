package org.aspen_ddp.aspen.server.store

import java.util.UUID
import org.aspen_ddp.aspen.client.{Host, HostId, StorageDevice, StoragePool}
import org.aspen_ddp.aspen.client.internal.pool.SimpleStoragePool
import org.aspen_ddp.aspen.client.tkvl.{BootstrapPoolNodeAllocator, Root}
import org.aspen_ddp.aspen.common.{HLCTimestamp, Radicle}
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.objects.{ByteArrayKeyOrdering, Insert, Key, KeyValueObjectPointer, KeyValueOperation, LexicalKeyOrdering, Metadata, ObjectId, ObjectRefcount, ObjectRevision, ObjectType, Value}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.server.store.backend.Backend
import org.aspen_ddp.aspen.common.util.uuid2byte
import org.aspen_ddp.aspen.demo.BootstrapConfig

import java.nio.charset.StandardCharsets

object Bootstrap:

  def initialize(aspenSystemId: UUID,
                 ida: IDA,
                 bootstrapHost: Host,
                 bootstrapStorageDevice: StorageDevice,
                 stores: List[Backend]): KeyValueObjectPointer = {

    require( ida.width == stores.length )
    
    val bootstrapConfig = BootstrapConfig.generateBootstrapConfig(
      aspenSystemId = aspenSystemId,
      ida = ida, 
      hosts = List(bootstrapHost),
      storeMap = stores.map(backend => backend.storeId -> bootstrapHost.hostId)
    )

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

    val storeHosts = (0 until ida.width).map(_ => bootstrapHost.hostId).toArray

    val poolConfig = SimpleStoragePool.encode(Radicle.poolId, "bootstrap", ida.width, ida, storeHosts, None)
    val errorTree = Root(0, ByteArrayKeyOrdering, Some(errTreeRoot), BootstrapPoolNodeAllocator).encode()
    val allocTree = Root(0, ByteArrayKeyOrdering, Some(allocTreeRoot), BootstrapPoolNodeAllocator).encode()

    val poolPointer = allocate(List(
      StoragePool.ConfigKey -> poolConfig,
      StoragePool.ErrorTreeKey -> errorTree,
      StoragePool.AllocationTreeKey -> allocTree
    ))
    val poolTreeRoot = allocate(List(
      Key(Radicle.poolId.uuid) -> poolPointer.toArray
    ))
    val poolTree = Root(0,
      ByteArrayKeyOrdering,
      Some(poolTreeRoot),
      BootstrapPoolNodeAllocator)

    val poolNameTreeRootObj = allocate(List(
      Key("bootstrap") -> uuid2byte(Radicle.poolId.uuid)
    ))
    val poolNameTree = Root(0,
      ByteArrayKeyOrdering,
      Some(poolNameTreeRootObj),
      BootstrapPoolNodeAllocator)

    val storageDeviceTreeRootObj = allocate(List(
      Key(bootstrapStorageDevice.storageDeviceId.uuid) -> bootstrapStorageDevice.encode()
    ))
    val storageDeviceTree = Root(0,
      ByteArrayKeyOrdering,
      Some(storageDeviceTreeRootObj),
      BootstrapPoolNodeAllocator)

    val hostsTreeRootObj = allocate(List(
      Key(bootstrapHost.hostId.uuid) -> bootstrapHost.encode()
    ))
    val hostsTree = Root(0,
      ByteArrayKeyOrdering,
      Some(hostsTreeRootObj),
      BootstrapPoolNodeAllocator)

    val hostsNameTreeRootObj = allocate(List(
      Key(bootstrapHost.name) -> uuid2byte(bootstrapHost.hostId.uuid)
    ))
    val hostsNameTree = Root(0,
      LexicalKeyOrdering,
      Some(hostsNameTreeRootObj),
      BootstrapPoolNodeAllocator)

    val radicleContent: List[(Key, Array[Byte])] = List(
      Radicle.PoolTreeKey -> poolTree.encode(),
      Radicle.PoolNameTreeKey -> poolNameTree.encode(),
      Radicle.StorageDeviceTreeKey -> storageDeviceTree.encode(),
      Radicle.HostsTreeKey -> hostsTree.encode(),
      Radicle.HostsNameTreeKey -> hostsNameTree.encode(),
      Radicle.BootstrapConfigKey -> bootstrapConfig.getBytes(StandardCharsets.UTF_8),
      Radicle.SystemIdKey -> uuid2byte(aspenSystemId)
    )

    val radicle = allocate(radicleContent, Some(Radicle.objectId))

    overwrite(allocTreeRoot, List(
      Key(errTreeRoot.id.uuid) -> errTreeRoot.toArray,
      Key(allocTreeRoot.id.uuid) -> allocTreeRoot.toArray,
      Key(poolPointer.id.uuid) -> poolPointer.toArray,
      Key(poolTreeRoot.id.uuid) -> poolTreeRoot.toArray,
      Key(poolNameTreeRootObj.id.uuid) -> poolNameTreeRootObj.toArray,
      Key(storageDeviceTreeRootObj.id.uuid) -> storageDeviceTreeRootObj.toArray,
      Key(hostsTreeRootObj.id.uuid) -> hostsTreeRootObj.toArray,
      Key(hostsNameTreeRootObj.id.uuid) -> hostsNameTreeRootObj.toArray,
      Key(radicle.id.uuid) -> radicle.toArray
    ))

    if false then
      println(s"ErrorTreeRoot: ${errTreeRoot.id.uuid}")
      println(s"AllocTreeRoot: ${allocTreeRoot.id.uuid}")
      println(s"Radicle Pool : ${poolPointer.id.uuid}")
      println(s"PoolTreeRoot : ${poolTreeRoot.id.uuid}")
      println(s"Radicle      : ${radicle.id.uuid}")

    radicle
  }

