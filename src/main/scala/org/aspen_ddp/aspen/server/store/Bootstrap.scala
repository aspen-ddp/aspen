package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.client.registries.NamespacedUUIDRegistry

import java.util.UUID
import java.nio.charset.StandardCharsets
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

    val errTreeRoot = allocate()
    val allocTreeRoot = allocate()
    val errorTree = Root(0, ByteArrayKeyOrdering, Some(errTreeRoot), BootstrapPoolNodeAllocator).encode()
    val allocTree = Root(0, ByteArrayKeyOrdering, Some(allocTreeRoot), BootstrapPoolNodeAllocator).encode()

    val poolPointer = allocate(List(
      StoragePoolState.ConfigKey -> poolState,
      StoragePoolState.ErrorTreeKey -> errorTree,
      StoragePoolState.AllocationTreeKey -> allocTree
    ))
    
    val storageDevicePtr = allocate(List(
      StorageDeviceState.StateKey ->  bootstrapStorageDeviceState.encode()
    ))

    val hostPtr = allocate(List(
      HostState.StateKey -> bootstrapHostState.encode()
    ))
    
    // Create registry trees 
    val objectRegistryRoot = allocate(List(
      Key(Radicle.poolId.uuid) -> poolPointer.toArray,
      Key(bootstrapStorageDeviceState.storageDeviceId.uuid) -> storageDevicePtr.toArray,
      Key(bootstrapHostState.hostId.uuid) -> hostPtr.toArray
    ))
    val objectRegistryTree = Root(0,
      ByteArrayKeyOrdering,
      Some(objectRegistryRoot),
      BootstrapPoolNodeAllocator)

    val namespacedRegistryRoot = allocate(List(
      NamespacedUUIDRegistry.makeKey("pool", PoolId.BootstrapPoolName) -> uuid2byte(Radicle.poolId.uuid),
      NamespacedUUIDRegistry.makeKey("host", bootstrapHostState.name) -> uuid2byte(bootstrapHostState.hostId.uuid)
    ))
    val namespacedRegistryTree = Root(0,
      LexicalKeyOrdering,
      Some(namespacedRegistryRoot),
      BootstrapPoolNodeAllocator)

    val radicleContent: List[(Key, Array[Byte])] = List(
      Radicle.BootstrapConfigKey -> bootstrapConfig.getBytes(StandardCharsets.UTF_8),
      Radicle.SystemIdKey -> uuid2byte(aspenSystemId),
      Radicle.ObjectRegistryKey -> objectRegistryTree.encode(),
      Radicle.NamespacedRegistryKey -> namespacedRegistryTree.encode()
    )

    val radicle = allocate(radicleContent, Some(Radicle.objectId))

    overwrite(allocTreeRoot, allocTreeContent)

    radicle
  }

