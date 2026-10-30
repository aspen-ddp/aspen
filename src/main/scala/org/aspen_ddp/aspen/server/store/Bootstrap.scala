package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.client.PoolObjectAllocatorId
import org.aspen_ddp.aspen.client.registries.NamespacedUUIDRegistry

import java.util.UUID
import java.nio.charset.StandardCharsets
import org.aspen_ddp.aspen.client.tkvl.{NodeAllocator, Root, SinglePoolNodeAllocator}
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp, Radicle}
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.network.tls.{CertificateAuthority, Pem}
import org.aspen_ddp.aspen.common.allocation_group.AllocationGroupId
import org.aspen_ddp.aspen.common.metadata.{AllocationGroupState, HostId, HostState, IDAPolicy, StorageDeviceSetId, StorageDeviceSetState, StorageDeviceState, StoragePoolState, fixed_ids}
import org.aspen_ddp.aspen.common.config.BootstrapConfig
import org.aspen_ddp.aspen.common.objects.{ByteArrayKeyOrdering, DataObjectPointer, Key, KeyOrdering, KeyValueObjectPointer, LexicalKeyOrdering, Metadata, ObjectId, ObjectRefcount, ObjectRevision, ObjectType, Value}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.server.store.backend.{Backend, RocksDBConfig}
import org.aspen_ddp.aspen.common.util.uuid2byte
import org.aspen_ddp.aspen.common.network.snapshot.NetworkCachingService
import org.aspen_ddp.aspen.common.rebalancing.RebalancingDurableService
import org.aspen_ddp.aspen.compute.ServiceEntry
import org.aspen_ddp.aspen.compute.systemtask.SystemTaskExecutorService

object Bootstrap:

  /** @param maxSliceSize        the bootstrap pool's maxSliceSize (see StoragePoolState)
    * @param backendMaxSliceSize the bootstrap pool backend's own limit (RocksDBConfig.maxSliceSize)
    *
    * Both must be at least `ida.minSliceSize`. The stores' enforced limits come from their store config files,
    * which the caller writes, and are not set here; they must match the pool state's `effectiveMaxSliceSize`.
    *
    * Also creates the initial metadata pool, which shares the bootstrap pool's IDA, limits and device, and the
    * default metadata group containing it. Its stores start empty, so nothing is written to them, but
    * `bootstrapStorageDeviceState` must list them.
    */
  def initialize(aspenSystemId: UUID,
                 ida: IDA,
                 bootstrapHostState: HostState,
                 bootstrapStorageDeviceState: StorageDeviceState,
                 stores: List[Backend],
                 ca: CertificateAuthority,
                 maxSliceSize: Option[Int] = None,
                 backendMaxSliceSize: Option[Int] = None): KeyValueObjectPointer = {

    require( ida.width == stores.length )
    maxSliceSize.foreach: limit =>
      require(limit >= ida.minSliceSize, s"maxSliceSize ($limit) must be at least ${ida.minSliceSize} for $ida")
    backendMaxSliceSize.foreach: limit =>
      require(limit >= ida.minSliceSize,
        s"backend maxSliceSize ($limit) must be at least ${ida.minSliceSize} for $ida")
    for i <- 0 until ida.width do
      val storeId = StoreId(PoolId.InitialMetadataPoolId, i.toByte)
      require(bootstrapStorageDeviceState.stores.contains(storeId),
        s"The bootstrap storage device must list initial metadata pool store $storeId")

    val storeEntrys = (0 until ida.width).map{ _ => 
      StoragePoolState.StoreEntry(bootstrapHostState.hostId, bootstrapStorageDeviceState.storageDeviceId)
    }.toArray
    
    val bootstrapPoolState = StoragePoolState(
      PoolId.BootstrapPoolId,
      PoolId.BootstrapPoolName,
      ida,
      maxSliceSize,
      storeEntrys,
      RocksDBConfig(backendMaxSliceSize),
      StorageDeviceSetId.BootstrapStorageDeviceSetId)

    val metadataPoolState = StoragePoolState(
      PoolId.InitialMetadataPoolId,
      PoolId.InitialMetadataPoolName,
      ida,
      maxSliceSize,
      storeEntrys.clone(),
      RocksDBConfig(backendMaxSliceSize),
      StorageDeviceSetId.BootstrapStorageDeviceSetId,
      allocationGroups = List(AllocationGroupId.DefaultMetadataGroupId.uuid),
      isMetadataPool = true)

    // The group's limit is checked against its members' effective limits, so it uses the pool's effective one
    val metadataGroupState = AllocationGroupState(
      AllocationGroupId.DefaultMetadataGroupId,
      0,
      AllocationGroupId.DefaultMetadataGroupName,
      List(AllocationGroupState.Member(AllocationGroupState.MemberType.Pool, PoolId.InitialMetadataPoolId.uuid, 0, 0)),
      Nil,
      metadataPoolState.effectiveMaxSliceSize,
      if metadataPoolState.effectiveMaxSliceSize.isDefined then IDAPolicy.ExactMatch else IDAPolicy.AllowAll,
      metadataPoolState.effectiveMaxSliceSize.map(_ => ida),
      isMetadataGroup = true)

    val bootstrapConfig = BootstrapConfig.generateBootstrapConfig(
      aspenSystemId = aspenSystemId,
      ida = ida,
      hostStates = List(bootstrapHostState),
      storeMap = stores.map(backend => backend.storeId -> bootstrapHostState.hostId),
      caCertificate = Pem.encodeCertificate(ca.certificate)
    )

    val bootstrapMetadata = Metadata(
      ObjectRevision(TransactionId(UUID.randomUUID())),
      ObjectRefcount(1,1),
      HLCTimestamp.now
    )
    
    // The bootstrap trees' nodes live in the bootstrap pool, so their node size must fit its slice limit
    val treeNodeSize = NodeAllocator.fittingNodeSize(SinglePoolNodeAllocator.DefaultMaxNodeSize,
      NodeAllocator.nodeSizeLimit(ida, bootstrapPoolState.effectiveMaxSliceSize))
    
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

    def allocateData(content: Array[Byte],
                     objectId: Option[ObjectId] = None): DataObjectPointer =
      val oid = objectId.getOrElse(ObjectId(UUID.randomUUID()))

      ida.encode(content).zip(stores).foreach { t =>
        val (storeData, store) = t
        store.bootstrapAllocate(oid, ObjectType.Data, bootstrapMetadata, DataBuffer(storeData))
      }

      val p = DataObjectPointer(oid, Radicle.poolId)

      allocTreeContent = (Key(p.id.uuid) -> p.toArray) :: allocTreeContent

      p

    val poolState = bootstrapPoolState.encode()

    val errTreeRoot = allocate()
    val allocTreeRoot = allocate()
    val errorTree = Root(0, ByteArrayKeyOrdering, Some(errTreeRoot), bootstrapNodeAllocator).encode()
    val allocTree = Root(0, ByteArrayKeyOrdering, Some(allocTreeRoot), bootstrapNodeAllocator).encode()

    val poolPointer = allocate(List(
      StoragePoolState.ConfigKey -> poolState,
      StoragePoolState.ErrorTreeKey -> errorTree,
      StoragePoolState.AllocationTreeKey -> allocTree
    ))

    // A metadata pool's own trees live in the bootstrap pool
    val metadataErrTreeRoot = allocate()
    val metadataAllocTreeRoot = allocate()
    val metadataPoolPointer = allocate(List(
      StoragePoolState.ConfigKey -> metadataPoolState.encode(),
      StoragePoolState.ErrorTreeKey ->
        Root(0, ByteArrayKeyOrdering, Some(metadataErrTreeRoot), bootstrapNodeAllocator).encode(),
      StoragePoolState.AllocationTreeKey ->
        Root(0, ByteArrayKeyOrdering, Some(metadataAllocTreeRoot), bootstrapNodeAllocator).encode()
    ))

    val metadataGroupPtr = allocateData(metadataGroupState.toBytes)
    
    val storageDevicePtr = allocate(List(
      StorageDeviceState.StateKey ->  bootstrapStorageDeviceState.encode()
    ))

    val hostPtr = allocate(List(
      HostState.StateKey -> bootstrapHostState.encode()
    ))

    val bootstrapDeviceSet = StorageDeviceSetState(
      setId = StorageDeviceSetId.BootstrapStorageDeviceSetId,
      name = "bootstrap",
      level = 0,
      parent = None,
      memberDevices = List(bootstrapStorageDeviceState.storageDeviceId),
      memberSets = Nil,
      assignedPools = List(PoolId.BootstrapPoolId, PoolId.InitialMetadataPoolId)
    )
    
    // Create registry trees 
    
    val objectRegistryTree = allocateTree(ByteArrayKeyOrdering) 
    
    val namespacedRegistryTree = allocateTree(
      LexicalKeyOrdering,
      NamespacedUUIDRegistry.makeKey("pool", PoolId.BootstrapPoolName) -> uuid2byte(Radicle.poolId.uuid),
      NamespacedUUIDRegistry.makeKey("pool", PoolId.InitialMetadataPoolName) ->
        uuid2byte(PoolId.InitialMetadataPoolId.uuid),
      NamespacedUUIDRegistry.makeKey("group", AllocationGroupId.DefaultMetadataGroupName) ->
        uuid2byte(AllocationGroupId.DefaultMetadataGroupId.uuid),
      NamespacedUUIDRegistry.makeKey("host", bootstrapHostState.name) -> uuid2byte(bootstrapHostState.hostId.uuid),
      NamespacedUUIDRegistry.makeKey("device-set", bootstrapDeviceSet.name) -> uuid2byte(fixed_ids.BootstrapStorageDeviceSetId.uuid),
    )
    
    // Create Metadata Trees
    val storagePoolsTree = allocateTree(
      ByteArrayKeyOrdering,
      Key(Radicle.poolId.uuid) -> poolPointer.toArray,
      Key(PoolId.InitialMetadataPoolId.uuid) -> metadataPoolPointer.toArray,
    )
    
    val allocationGroupsTree = allocateTree(
      ByteArrayKeyOrdering,
      Key(AllocationGroupId.DefaultMetadataGroupId.uuid) -> metadataGroupPtr.toArray
    )

    val hostsTree = allocateTree(
      ByteArrayKeyOrdering,
      Key(bootstrapHostState.hostId.uuid) -> hostPtr.toArray
    )

    val storageDevicesTree =allocateTree(
      ByteArrayKeyOrdering,
      Key(bootstrapStorageDeviceState.storageDeviceId.uuid) -> storageDevicePtr.toArray
    )
    
    val storageDeviceSetPtr = allocateData(bootstrapDeviceSet.toBytes)

    val storageDeviceSetsTree = allocateTree(
      ByteArrayKeyOrdering,
      Key(StorageDeviceSetId.BootstrapStorageDeviceSetId.uuid) -> storageDeviceSetPtr.toArray
    )

    // The RebalancingDurableService is a system-critical singleton that must always be running.
    // Register it directly in the services tree as an unclaimed entry so the first host to scan
    // the tree will claim and run it. This avoids relying on any per-host registration step.
    val rebalancingServicePtr = allocate(RebalancingDurableService.initialServiceState.toList)
    val rebalancingServiceEntry = ServiceEntry(
      RebalancingDurableService.ServiceTypeUUID,
      ServiceEntry.UnclaimedHostId,
      HLCTimestamp.Zero,
      rebalancingServicePtr)

    // The SystemTaskExecutorService is likewise a system-critical singleton. Its state object
    // holds an (initially empty) TKVL of pending system tasks.
    val systemTaskTree = allocateTree(ByteArrayKeyOrdering)
    val systemTaskServicePtr = allocate(List(
      SystemTaskExecutorService.TaskTreeKey -> systemTaskTree.encode()
    ))
    val systemTaskServiceEntry = ServiceEntry(
      SystemTaskExecutorService.ServiceTypeUUID,
      ServiceEntry.UnclaimedHostId,
      HLCTimestamp.Zero,
      systemTaskServicePtr)

    // The NetworkCachingService writes the daily host/store snapshots into two DataObjects that
    // are allocated here, empty, so the service only ever overwrites them.
    val hostSnapshotPtr = allocateData(NetworkCachingService.emptyHostSnapshot)
    val storeSnapshotPtr = allocateData(NetworkCachingService.emptyStoreSnapshot)
    val networkCachingServicePtr = allocate(
      NetworkCachingService.initialServiceState(hostSnapshotPtr, storeSnapshotPtr).toList)
    val networkCachingServiceEntry = ServiceEntry(
      NetworkCachingService.ServiceTypeUUID,
      ServiceEntry.UnclaimedHostId,
      HLCTimestamp.Zero,
      networkCachingServicePtr)

    val servicesTree = allocateTree(
      ByteArrayKeyOrdering,
      Key(RebalancingDurableService.ServiceUUID) -> rebalancingServiceEntry.encode(),
      Key(SystemTaskExecutorService.ServiceUUID) -> systemTaskServiceEntry.encode(),
      Key(NetworkCachingService.ServiceUUID) -> networkCachingServiceEntry.encode()
    )

    val caCertificatePtr = allocateData(Pem.encodeCertificate(ca.certificate).getBytes(StandardCharsets.UTF_8))
    val caPrivateKeyPtr = allocateData(Pem.encodePrivateKey(ca.privateKey).getBytes(StandardCharsets.UTF_8))

    val radicleContent: List[(Key, Array[Byte])] = List(
      Radicle.BootstrapConfigKey -> bootstrapConfig.getBytes(StandardCharsets.UTF_8),
      Radicle.SystemIdKey -> uuid2byte(aspenSystemId),
      Radicle.ObjectRegistryKey -> objectRegistryTree.encode(),
      Radicle.NamespacedRegistryKey -> namespacedRegistryTree.encode(),
      Radicle.StoragePoolsTreeKey -> storagePoolsTree.encode(),
      Radicle.AllocationGroupsTreeKey -> allocationGroupsTree.encode(),
      Radicle.HostsTreeKey -> hostsTree.encode(),
      Radicle.StorageDevicesTreeKey -> storageDevicesTree.encode(),
      Radicle.StorageDeviceSetsTreeKey -> storageDeviceSetsTree.encode(),
      Radicle.ServicesTreeKey -> servicesTree.encode(),
      Radicle.CACertificateKey -> caCertificatePtr.toArray,
      Radicle.CAPrivateKeyKey -> caPrivateKeyPtr.toArray,
      Radicle.DefaultStoragePoolMetadataAllocatorKey -> uuid2byte(AllocationGroupId.DefaultMetadataGroupId.uuid),
    )
 
    val radicle = allocate(radicleContent, Some(Radicle.objectId))

    overwrite(allocTreeRoot, allocTreeContent)

    radicle
  }

