package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.client.PoolObjectAllocatorId
import org.aspen_ddp.aspen.client.registries.NamespacedUUIDRegistry

import java.util.UUID
import java.nio.charset.StandardCharsets
import org.aspen_ddp.aspen.client.tkvl.{NodeAllocator, Root}
import org.aspen_ddp.aspen.common.{HLCTimestamp, Radicle}
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.metadata.{HostId, HostState, StorageDeviceState, StoragePoolState}
import org.aspen_ddp.aspen.common.objects.{ByteArrayKeyOrdering, Key, KeyOrdering, KeyValueObjectPointer, LexicalKeyOrdering, Metadata, ObjectId, ObjectRefcount, ObjectRevision, ObjectType, Value}
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
      ObjectRevision(TransactionId(UUID.randomUUID())),
      ObjectRefcount(1,1),
      HLCTimestamp.now
    )
    
    val treeNodeSize =  1 * 1024 * 1024
    
    val bootstrapNodeAllocator = new NodeAllocator(null, List((PoolObjectAllocatorId(Radicle.poolId), treeNodeSize)))

    var allocTreeContent: List[(Key, Array[Byte])] = Nil

    def allocate(content: List[(Key, Array[Byte])] = Nil,
                 objectId: Option[ObjectId] = None): KeyValueObjectPointer = {

      val oid = objectId.getOrElse(ObjectId(UUID.randomUUID()))

      val contents = content.map { t =>
        t._1 -> new ValueState(Value(t._2), bootstrapMetadata.revision, bootstrapMetadata.timestamp, None)
      }.toMap

      KVObjectState.encodeIDA(ida, None, None, None, None, contents).zip(stores).foreach { t =>
        val (storeData, store) = t

        store.bootstrapAllocate(oid, ObjectType.KeyValue, bootstrapMetadata, storeData)
      }

      val p = KeyValueObjectPointer(oid, Radicle.poolId)

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
        store.bootstrapOverwrite(pointer.id, storeData)
      }
    }

    def allocateTree(ordering: KeyOrdering, content: (Key, Array[Byte])*): Root =
      val rootObject = allocate(content.toList)
      Root(0, ordering, Some(rootObject), bootstrapNodeAllocator)

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
    val errorTree = Root(0, ByteArrayKeyOrdering, Some(errTreeRoot), bootstrapNodeAllocator).encode()
    val allocTree = Root(0, ByteArrayKeyOrdering, Some(allocTreeRoot), bootstrapNodeAllocator).encode()

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
    
    val objectRegistryTree = allocateTree(ByteArrayKeyOrdering) 
    
    val namespacedRegistryTree = allocateTree(
      LexicalKeyOrdering,
      NamespacedUUIDRegistry.makeKey("pool", PoolId.BootstrapPoolName) -> uuid2byte(Radicle.poolId.uuid),
      NamespacedUUIDRegistry.makeKey("host", bootstrapHostState.name) -> uuid2byte(bootstrapHostState.hostId.uuid)
    )
    
    // Create Metadata Trees
    val storagePoolsTree = allocateTree(
      ByteArrayKeyOrdering,
      Key(Radicle.poolId.uuid) -> poolPointer.toArray,
    )
    
    val allocationGroupsTree = allocateTree(ByteArrayKeyOrdering)

    val hostsTree = allocateTree(
      ByteArrayKeyOrdering,
      Key(bootstrapHostState.hostId.uuid) -> hostPtr.toArray
    )

    val storageDevicesTree =allocateTree(
      ByteArrayKeyOrdering,
      Key(bootstrapStorageDeviceState.storageDeviceId.uuid) -> storageDevicePtr.toArray
    )

    val radicleContent: List[(Key, Array[Byte])] = List(
      Radicle.BootstrapConfigKey -> bootstrapConfig.getBytes(StandardCharsets.UTF_8),
      Radicle.SystemIdKey -> uuid2byte(aspenSystemId),
      Radicle.ObjectRegistryKey -> objectRegistryTree.encode(),
      Radicle.NamespacedRegistryKey -> namespacedRegistryTree.encode(),
      Radicle.StoragePoolsTreeKey -> storagePoolsTree.encode(),
      Radicle.AllocationGroupsTreeKey -> allocationGroupsTree.encode(),
      Radicle.HostsTreeKey -> hostsTree.encode(),
      Radicle.StorageDevicesTreeKey -> storageDevicesTree.encode(),
    )
 
    val radicle = allocate(radicleContent, Some(Radicle.objectId))

    overwrite(allocTreeRoot, allocTreeContent)

    radicle
  }

