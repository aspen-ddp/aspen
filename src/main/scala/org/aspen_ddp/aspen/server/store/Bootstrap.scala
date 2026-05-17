package org.aspen_ddp.aspen.server.store

import java.util.UUID
import org.aspen_ddp.aspen.client.StoragePool
import org.aspen_ddp.aspen.client.internal.pool.SimpleStoragePool
import org.aspen_ddp.aspen.client.tkvl.{BootstrapPoolNodeAllocator, Root}
import org.aspen_ddp.aspen.common.{HLCTimestamp, Radicle}
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.metadata.{HostId, HostState, StorageDeviceState, StoragePoolState}
import org.aspen_ddp.aspen.common.objects.{ByteArrayKeyOrdering, Key, KeyValueObjectPointer, LexicalKeyOrdering, Metadata, ObjectId, ObjectRefcount, ObjectRevision, ObjectType, Value}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.server.store.backend.{Backend, RocksDBConfig}
import org.aspen_ddp.aspen.common.util.uuid2byte
import org.aspen_ddp.aspen.demo.BootstrapConfig

import java.nio.charset.StandardCharsets

object Bootstrap:

  def initialize(aspenSystemId: UUID,
                 ida: IDA,
                 bootstrapHostState: HostState,
                 bootstrapStorageDeviceState: StorageDeviceState,
                 stores: List[Backend]): KeyValueObjectPointer = {

    require( ida.width == stores.length )

    val bootstrapConfig = BootstrapConfig.generateBootstrapConfig(
      aspenSystemId = aspenSystemId,
      ida = ida,
      hostStates = List(bootstrapHostState),
      storeMap = stores.map(backend => backend.storeId -> bootstrapHostState.hostId)
    )

    val bootstrapMetadata = Metadata(
      ObjectRevision(TransactionId(new UUID(0,0))),
      ObjectRefcount(1,1),
      HLCTimestamp.now
    )

    var allocTreeContent: List[(Key, Array[Byte])] = Nil

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

      val p = KeyValueObjectPointer(oid, Radicle.poolId, None, ida, storePointers)

      allocTreeContent = (Key(p.id.uuid) -> p.toArray) :: allocTreeContent

      p
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

    val storeEntrys = (0 until ida.width).map{ _ => 
      StoragePoolState.StoreEntry(bootstrapHostState.hostId, bootstrapStorageDeviceState.storageDeviceId)
    }.toArray
    
    val poolState = StoragePoolState(
      PoolId.BootstrapPoolId, 
      PoolId.BootstrapPoolName, 
      ida, 
      None, 
      storeEntrys,
      RocksDBConfig()).encode()
    val errorTree = Root(0, ByteArrayKeyOrdering, Some(errTreeRoot), BootstrapPoolNodeAllocator).encode()
    val allocTree = Root(0, ByteArrayKeyOrdering, Some(allocTreeRoot), BootstrapPoolNodeAllocator).encode()

    val poolPointer = allocate(List(
      StoragePoolState.ConfigKey -> poolState,
      StoragePoolState.ErrorTreeKey -> errorTree,
      StoragePoolState.AllocationTreeKey -> allocTree
    ))
    val poolTreeRoot = allocate(List(
      Key(Radicle.poolId.uuid) -> poolPointer.toArray
    ))
    val poolTree = Root(0,
      ByteArrayKeyOrdering,
      Some(poolTreeRoot),
      BootstrapPoolNodeAllocator)

    val poolNameTreeRootObj = allocate(List(
      Key(PoolId.BootstrapPoolName) -> uuid2byte(Radicle.poolId.uuid)
    ))
    val poolNameTree = Root(0,
      LexicalKeyOrdering,
      Some(poolNameTreeRootObj),
      BootstrapPoolNodeAllocator)

    val storageDevicePtr = allocate(List(
      StorageDeviceState.StateKey ->  bootstrapStorageDeviceState.encode()
    ))
    val storageDeviceTreeRootObj = allocate(List(
      Key(bootstrapStorageDeviceState.storageDeviceId.uuid) -> storageDevicePtr.toArray
    ))
    val storageDeviceTree = Root(0,
      ByteArrayKeyOrdering,
      Some(storageDeviceTreeRootObj),
      BootstrapPoolNodeAllocator)

    val hostPtr = allocate(List(
      HostState.StateKey -> bootstrapHostState.encode()
    ))
    val hostsTreeRootObj = allocate(List(
      Key(bootstrapHostState.hostId.uuid) -> hostPtr.toArray
    ))
    val hostsTree = Root(0,
      ByteArrayKeyOrdering,
      Some(hostsTreeRootObj),
      BootstrapPoolNodeAllocator)

    val hostsNameTreeRootObj = allocate(List(
      Key(bootstrapHostState.name) -> uuid2byte(bootstrapHostState.hostId.uuid)
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

    overwrite(allocTreeRoot, allocTreeContent)

    if false then
      println(s"ErrorTreeRoot: ${errTreeRoot.id.uuid}")
      println(s"AllocTreeRoot: ${allocTreeRoot.id.uuid}")
      println(s"Radicle Pool : ${poolPointer.id.uuid}")
      println(s"PoolTreeRoot : ${poolTreeRoot.id.uuid}")
      println(s"Radicle      : ${radicle.id.uuid}")

    radicle
  }

