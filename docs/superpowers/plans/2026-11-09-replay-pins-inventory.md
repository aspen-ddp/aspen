# Replay pins: empirical inventory of pinless transactions

Produced by running `sbt test` (3514 tests, 187 suites) with the pin check forced on and logging instead of throwing, one line per distinct creation stack. Throwing hangs the run: `transactUntilSuccessful` retries `UnpinnedTransaction` forever. Origin is the builder's creation stack with the generic wrappers (`transact`, retry strategies, `newTransaction`) removed, so a product site is the first non-test frame.

Test-site granularity is suite plus the helper or test-body line that built the transaction; test names are not recoverable from the stack (they are `testFun$proxyN` lambdas). Task 12 will surface them by failing.

## Product sites

| Building call site (file:line from stack) | Suites seen in | Analysis |
|---|---|---|
| `amoebafs.Directory.retryUntilSuccessfulOr:57` | DirectorySuite | in analysis: amoebafs directory insert/unlink/rename (Task 11) |
| `amoebafs.impl.simple.SimpleFileSystem$.create:76` | DirectorySuite, FileSystemRegistrationSuite, LimitedPoolFileSystemSuite (+3 more) | NOT IN ANALYSIS: filesystem creation (allocation-only plus registration); candidate replaySafe |
| `client.AspenClient.newRepairTransaction:97` | RepairProofsSuite, StoreRepairerSuite | in analysis: RepairProofs, exempt (my call) |
| `client.internal.BaseAspenClient.createAllocationGroup:358` | AllocationGroupStateSuite, DiskCatalogSuite, GroupObjectAllocatorSuite (+3 more) | NOT IN ANALYSIS: admin creation (allocation-only or DoesNotExist registry insert); candidate: replaySafe or node revision |
| `client.internal.BaseAspenClient.createHost:417` | BootstrapConfigRadicleUpdateSuite, DeviceAdoptionSuite, DeviceCrlSuite (+3 more) | NOT IN ANALYSIS: admin creation (allocation-only or DoesNotExist registry insert); candidate: replaySafe or node revision |
| `client.internal.BaseAspenClient.createStorageDeviceSet:392` | ClientListingSuite, FailedStorageDeviceSuite, FsifIntegrationSuite (+8 more) | NOT IN ANALYSIS: admin creation (allocation-only or DoesNotExist registry insert); candidate: replaySafe or node revision |
| `client.internal.BaseAspenClient.createSystemDurableTask:640` | CreateSystemDurableTaskSuite, HostSystemTaskSuite, SystemTaskExecutorServiceSuite (+1 more) | NOT IN ANALYSIS: admin creation (allocation-only or DoesNotExist registry insert); candidate: replaySafe or node revision |
| `client.internal.BaseAspenClient.failStorageDevice:737` | FailedStorageDeviceSuite, MigratePoolToSetSuite, RebuildEndToEndSuite (+1 more) | NOT IN ANALYSIS: admin transaction |
| `client.internal.allocation.AllocationFinalizationAction.execute$$anonfun$1$$anonfun$1:27` | AbortStoreTransferSuite, AdaptiveThreadedHostSuite, AllocationGroupStateSuite (+120 more) | in analysis: lockRevision on new object (Task 10) |
| `client.internal.allocation.DeletionFinalizationAction.$anonfun$1$$anonfun$1:27` | DataObjectNodeEncodingSuite, DataObjectTKVLSuite, DirectorySuite (+26 more) | in analysis: KeyRevision (Task 10) |
| `client.internal.transaction.MissedUpdateMarker.writeBatch$$anonfun$2$$anonfun$1:162` | AbortStoreTransferSuite, AllocationGroupStateSuite, BasicIntegrationSuite (+103 more) | in analysis: exempt (user) |
| `client.registries.Registry.register$$anonfun$1$$anonfun$1:56` | NamespacedUUIDRegistrySuite, NetworkCachingServiceSuite | in analysis: node revision (decision 4, Registry.prepareRegister) |
| `client.tkvl.JoinFinalizationAction.tx$lzyINIT2$1:73` | JoinFinalizationActionSuite, ReedSolomonTKVLBatchSuite, ReedSolomonTreeKindParitySuite (+5 more) | in analysis: KeyRevision |
| `client.tkvl.KeyValueListNode$.$anonfun$29:1113` | KeyValueListSuite, ReedSolomonTreeKindParitySuite, SimpleFileContentSuite (+2 more) | in analysis: KV TKVL deleteOneAtATime / blind set |
| `client.tkvl.SplitFinalizationAction.tx$lzyINIT2$1:48` | KVObjectRootManagerSuite, NodeAllocatorRandomAccessFillSuite, ReedSolomonTKVLBatchSuite (+8 more) | in analysis: KeyRevision plus lock |
| `compute.impl.SimpleDurableServiceExecutor.registerService$$anonfun$1$$anonfun$1:268` | DurableServiceSuite | in analysis: node revision (decision 4) |
| `compute.impl.SimpleTaskExecutor.allocateTask:96` | AllocationGroupStateSuite, DirectorySuite, LimitedPoolFileSystemSuite (+7 more) | in analysis: node revision (decision 4) |
| `compute.impl.SimpleTaskExecutor.deallocateTask:108` | AllocationGroupStateSuite, DirectorySuite, LimitedPoolFileSystemSuite (+6 more) | in analysis: task object revision |
| `compute.systemtask.SystemTaskServiceState$.enroll:29` | SystemTaskServiceStateSuite | in analysis: KeyRevision/node pin |
| `compute.systemtask.SystemTaskServiceState$.remove:56` | SystemTaskServiceStateSuite | in analysis: KeyRevision/node pin |
| `server.host.HostServices.initializeTaskExecutor$$anonfun$1$$anonfun$1$$anonfun$2:84` | AbortStoreTransferSuite, AllocationGroupStateSuite, BasicIntegrationSuite (+116 more) | NOT IN ANALYSIS: DoesNotExist insert of TaskExecutorRootKey into host state plus allocation (candidate: node revision, as for allocateTask) |
| `server.repair.StoreRepairer.reconcile$1:286` | StoreRepairerSuite | NOT IN ANALYSIS by this line: analysis lists only StoreRepairer.deleteEntry (:355-361); this is the repair reconcile path (installing/proving), check whether it is a RepairProofs identity update |
| `server.repair.StoreRepairer.repairUpdate$$anonfun$2$$anonfun$1:297` | StoreRepairerSuite | NOT IN ANALYSIS by this line: see reconcile; likely RepairProofs-related |
| `vdisk.DiskCatalog$.create:77` | DiskCatalogSuite, DiskHandleSuite, TcmuIntegrationSuite (+2 more) | in analysis: absent-key node revision (Task 11) |
| `vdisk.DiskCatalog$.removeKey$1$$anonfun$1$$anonfun$1:249` | DiskCatalogSuite, DiskHandleSuite | in analysis: KeyRevision (Task 11) |

## Product sites not in the analysis (add to Task 10 or 11)

- `amoebafs.impl.simple.SimpleFileSystem$.create:76`
- `client.internal.BaseAspenClient.createAllocationGroup:358`
- `client.internal.BaseAspenClient.createHost:417`
- `client.internal.BaseAspenClient.createStorageDeviceSet:392`
- `client.internal.BaseAspenClient.createSystemDurableTask:640`
- `client.internal.BaseAspenClient.failStorageDevice:737`
- `server.host.HostServices.initializeTaskExecutor$$anonfun$1$$anonfun$1$$anonfun$2:84`
- `server.repair.StoreRepairer.reconcile$1:286`
- `server.repair.StoreRepairer.repairUpdate$$anonfun$2$$anonfun$1:297`

Task 10 additions: `HostServices.initializeTaskExecutor`, `StoreRepairer` reconcile/repairUpdate, `BaseAspenClient` admin creators, `createSystemDurableTask`. Task 11 additions: `SimpleFileSystem.create`.

## Test sites (transactions built by tests or test helpers)

- AdaptiveThreadedHostSuite: `server.host.ThreadedHostSuite.tx:30`
- BasicIntegrationSuite: `BasicIntegrationSuite.tx:175`, `BasicIntegrationSuite.tx:19`, `BasicIntegrationSuite.tx:211`, `BasicIntegrationSuite.tx:70`
- BootstrapMetadataSuite: `server.store.BootstrapMetadataSuite.insertAndGet:59`
- ClassicReedSolomonAppendRejectionSuite: `ClassicReedSolomonAppendRejectionSuite.tx:452`, `ClassicReedSolomonAppendRejectionSuite.tx:460`
- ClassicReedSolomonRangedWriteRejectionSuite: `ClassicReedSolomonRangedWriteRejectionSuite.tx:465`
- CreateSystemDurableTaskSuite: `compute.systemtask.CreateSystemDurableTaskSuite.testFun:43`
- DataObjectNodeEncodingSuite: `client.tkvl.DataObjectNodeEncodingSuite.makeNode:49`, `client.tkvl.DataObjectNodeEncodingSuite.testFun:72`
- DataObjectTKVLSuite: `client.tkvl.DataObjectTKVLSuite.newRootManager:49`, `client.tkvl.DataObjectTKVLSuite.rootHost:34`, `client.tkvl.DataObjectTKVLSuite.testFun:153`, `client.tkvl.DataObjectTKVLSuite.testFun:529`, `client.tkvl.DataObjectTKVLSuite.testFun:537`
- DirectorySuite: `amoebafs.impl.simple.DirectorySuite.testFun:471`, `amoebafs.impl.simple.DirectorySuite.testFun:511`, `amoebafs.impl.simple.DirectorySuite.tx:14`, `amoebafs.impl.simple.DirectorySuite.tx:249`, `amoebafs.impl.simple.DirectorySuite.tx:272`, `amoebafs.impl.simple.DirectorySuite.tx:286`, `amoebafs.impl.simple.DirectorySuite.tx:300`, `amoebafs.impl.simple.DirectorySuite.tx:315`, `amoebafs.impl.simple.DirectorySuite.tx:330`
- DiskCatalogSuite: `vdisk.DiskCatalogSuite.removeKey:215`
- DurableServiceSuite: `compute.DurableServiceSuite.testFun:248`, `compute.DurableServiceSuite.testFun:266`
- FsifBitmapSizeLimitSuite: `client.internal.allocation.FsifBitmapSizeLimitSuite.testFun:526`
- FsifIndexAllocationStrategySuite: `client.internal.allocation.FsifIndexAllocationStrategySuite.allocateOne:86`, `client.internal.allocation.FsifIndexAllocationStrategySuite.installStrategy:55`, `client.internal.allocation.FsifIndexAllocationStrategySuite.testFun:225`
- FullWriteThresholdTransferStarterSuite: `IntegrationTestSuite.setStoreSize:114`
- GroupObjectAllocatorSuite: `client.internal.allocation.GroupObjectAllocatorSuite.testFun:124`, `client.internal.allocation.GroupObjectAllocatorSuite.testFun:157`, `client.internal.allocation.GroupObjectAllocatorSuite.testFun:179`, `client.internal.allocation.GroupObjectAllocatorSuite.testFun:184`, `client.internal.allocation.GroupObjectAllocatorSuite.testFun:208`, `client.internal.allocation.GroupObjectAllocatorSuite.testFun:71`, `client.internal.allocation.GroupObjectAllocatorSuite.testFun:95`
- GuidedTourSuite: `examples.GuidedTourSuite.addBookmark:150`, `examples.GuidedTourSuite.buildStore:115`
- JoinFinalizationActionSuite: `client.tkvl.JoinFinalizationActionSuite.testFun:21`, `client.tkvl.JoinFinalizationActionSuite.testFun:36`
- KVObjectRootManagerSuite: `client.tkvl.KVObjectRootManagerSuite.inTx:49`, `client.tkvl.KVObjectRootManagerSuite.newTree:36`, `client.tkvl.KVObjectRootManagerSuite.rootHost:27`, `client.tkvl.KVObjectRootManagerSuite.testFun:158`
- KeyValueListNodeSplitAtSuite: `client.tkvl.KeyValueListNodeSplitAtSuite.testFun:124`, `client.tkvl.KeyValueListNodeSplitAtSuite.testFun:185`, `client.tkvl.KeyValueListNodeSplitAtSuite.testFun:235`, `client.tkvl.KeyValueListNodeSplitAtSuite.testFun:283`, `client.tkvl.KeyValueListNodeSplitAtSuite.testFun:329`, `client.tkvl.KeyValueListNodeSplitAtSuite.testFun:368`, `client.tkvl.KeyValueListNodeSplitAtSuite.testFun:413`, `client.tkvl.KeyValueListNodeSplitAtSuite.testFun:462`, `client.tkvl.KeyValueListNodeSplitAtSuite.testFun:498`, `client.tkvl.KeyValueListNodeSplitAtSuite.testFun:539`, `client.tkvl.KeyValueListNodeSplitAtSuite.testFun:60`
- KeyValueListSuite: `client.tkvl.KeyValueListSuite.testFun:110`, `client.tkvl.KeyValueListSuite.testFun:124`, `client.tkvl.KeyValueListSuite.testFun:163`, `client.tkvl.KeyValueListSuite.testFun:201`, `client.tkvl.KeyValueListSuite.testFun:215`, `client.tkvl.KeyValueListSuite.testFun:251`, `client.tkvl.KeyValueListSuite.testFun:301`, `client.tkvl.KeyValueListSuite.testFun:356`, `client.tkvl.KeyValueListSuite.testFun:402`, `client.tkvl.KeyValueListSuite.testFun:445`, `client.tkvl.KeyValueListSuite.testFun:491`, `client.tkvl.KeyValueListSuite.testFun:505`, `client.tkvl.KeyValueListSuite.testFun:545`, `client.tkvl.KeyValueListSuite.testFun:563`, `client.tkvl.KeyValueListSuite.testFun:61`, `client.tkvl.KeyValueListSuite.testFun:616`, `client.tkvl.KeyValueListSuite.testFun:628`, `client.tkvl.KeyValueListSuite.testFun:634`, `client.tkvl.KeyValueListSuite.testFun:75`, `client.tkvl.KeyValueListSuite.tx:20`
- KeyValueNodeEncodingSuite: `client.tkvl.KeyValueNodeEncodingSuite.testFun:350`, `client.tkvl.KeyValueNodeEncodingSuite.testFun:354`
- LimitedPoolFileSystemSuite: `amoebafs.impl.simple.LimitedPoolFileSystemSuite.tx:18`, `amoebafs.impl.simple.LimitedPoolFileSystemSuite.tx:24`
- MaxSliceSizePoolIntegrationSuite: `MaxSliceSizeHelpers.tx:21`
- MaxSliceSizeStoreIntegrationSuite: `MaxSliceSizeHelpers.tx:21`
- NamespacedUUIDRegistrySuite: `client.registries.NamespacedUUIDRegistrySuite.createRegistry:20`, `client.registries.NamespacedUUIDRegistrySuite.createRegistry:26`
- NarrowRandomAccessReedSolomonAppendIntegrationSuite: `AppendIntegrationTests.tx:54`, `AppendIntegrationTests.tx:64`
- NarrowRandomAccessReedSolomonRangedWriteIntegrationSuite: `RangedWriteIntegrationTests.tx:52`
- NarrowReedSolomonIntegrationSuite: `ReedSolomonIntegrationTests.testFun:256`, `ReedSolomonIntegrationTests.tx:119`, `ReedSolomonIntegrationTests.tx:147`, `ReedSolomonIntegrationTests.tx:166`, `ReedSolomonIntegrationTests.tx:213`, `ReedSolomonIntegrationTests.tx:239`, `ReedSolomonIntegrationTests.tx:274`, `ReedSolomonIntegrationTests.tx:300`, `ReedSolomonIntegrationTests.tx:329`
- NetworkCachingServiceSuite: `common.network.snapshot.NetworkCachingServiceSuite.testFun:166`, `common.network.snapshot.NetworkCachingServiceSuite.testFun:180`
- NodeAllocatorLimitSuite: `client.tkvl.NodeAllocatorLimitSuite.testFun:196`, `client.tkvl.NodeFillBehaviour.insertOnce:69`
- NodeAllocatorRandomAccessFillSuite: `client.tkvl.NodeFillBehaviour.insertOnce:69`
- PoolMigrationPlanningSuite: `IntegrationTestSuite.setStoreSize:114`, `common.rebalancing.PoolMigrationPlanningSuite.allocateTaskStateObject:170`
- PoolObjectAllocatorSuite: `client.internal.allocation.PoolObjectAllocatorSuite.testFun:135`, `client.internal.allocation.PoolObjectAllocatorSuite.testFun:151`, `client.internal.allocation.PoolObjectAllocatorSuite.testFun:168`, `client.internal.allocation.PoolObjectAllocatorSuite.testFun:182`, `client.internal.allocation.PoolObjectAllocatorSuite.testFun:208`, `client.internal.allocation.PoolObjectAllocatorSuite.testFun:229`, `client.internal.allocation.PoolObjectAllocatorSuite.testFun:248`, `client.internal.allocation.PoolObjectAllocatorSuite.testFun:295`
- QuicEndToEndSuite: `common.network.implementations.quic.QuicEndToEndSuite.org$aspen_ddp$aspen$common$network$implementations$quic$QuicEndToEndSuite$$_$attempt:134`
- RandomAccessReedSolomonAppendIntegrationSuite: `AppendIntegrationTests.tx:54`, `AppendIntegrationTests.tx:64`
- RandomAccessReedSolomonBasicIntegrationSuite: `BasicIntegrationSuite.tx:175`, `BasicIntegrationSuite.tx:19`, `BasicIntegrationSuite.tx:211`, `BasicIntegrationSuite.tx:70`
- RandomAccessReedSolomonRangedWriteIntegrationSuite: `RangedWriteIntegrationTests.tx:52`
- RebuildEndToEndSuite: `server.rebuild.RebuildEndToEndSuite.testFun:31`
- RebuildingStoreSuite: `server.rebuild.RebuildingStoreSuite.$anonfun:57`, `server.rebuild.RebuildingStoreSuite.testFun:324`
- ReedSolomonIntegrationSuite: `ReedSolomonIntegrationTests.testFun:256`, `ReedSolomonIntegrationTests.tx:119`, `ReedSolomonIntegrationTests.tx:147`, `ReedSolomonIntegrationTests.tx:166`, `ReedSolomonIntegrationTests.tx:213`, `ReedSolomonIntegrationTests.tx:239`, `ReedSolomonIntegrationTests.tx:274`, `ReedSolomonIntegrationTests.tx:300`, `ReedSolomonIntegrationTests.tx:329`
- ReedSolomonTKVLBatchSuite: `client.tkvl.TreeKindSuiteBase.inTransaction:117`, `client.tkvl.TreeKindSuiteBase.newTree:105`, `client.tkvl.TreeKindSuiteBase.rootHost:93`
- ReedSolomonTreeKindParitySuite: `client.tkvl.TreeKindSuiteBase.inTransaction:117`, `client.tkvl.TreeKindSuiteBase.newTree:105`, `client.tkvl.TreeKindSuiteBase.rootHost:93`
- RegistrySuite: `client.registries.RegistrySuite.createRegistry:20`, `client.registries.RegistrySuite.createRegistry:26`
- RepairProofsSuite: `server.repair.RepairProofsSuite.insert:32`, `server.repair.RepairProofsSuite.kvObject:24`, `server.repair.RepairProofsSuite.testFun:92`
- ReplayIntegrationSuite: `server.ReplayIntegrationSuite.testFun:105`, `server.ReplayIntegrationSuite.testFun:118`
- ReplicationAppendIntegrationSuite: `AppendIntegrationTests.tx:54`, `AppendIntegrationTests.tx:64`
- ReplicationRangedWriteIntegrationSuite: `RangedWriteIntegrationTests.tx:52`
- SetRebalanceDurableTaskSuite: `IntegrationTestSuite.setStoreSize:114`
- SharedRevisionLockIntegrationSuite: `SharedRevisionLockIntegrationSuite.tx:15`
- SimpleFileContentSuite: `amoebafs.impl.simple.SimpleFileContentSuite.boot:27`
- SimpleFileHandleSuite: `amoebafs.impl.simple.SimpleFileHandleSuite.bootFileHandle:16`
- SimpleFileSystemTestSuite: `amoebafs.impl.simple.SimpleFileSystemTestSuite.testFun:105`, `amoebafs.impl.simple.SimpleFileSystemTestSuite.testFun:84`
- SimpleRootManagerDecodeSuite: `amoebafs.impl.simple.SimpleRootManagerDecodeSuite.tx:42`
- StorageDeviceSetIntegrationSuite: `IntegrationTestSuite.setStoreSize:114`, `common.metadata.StorageDeviceSetIntegrationSuite.testFun:292`
- StoreRepairerSuite: `server.repair.StoreRepairerSuite.allocateObjectPointer:108`, `server.repair.StoreRepairerSuite.missedByTx:447`, `server.repair.StoreRepairerSuite.sharedDataObject:667`, `server.repair.StoreRepairerSuite.sharedKVObject:500`, `server.repair.StoreRepairerSuite.testFun:549`, `server.repair.StoreRepairerSuite.testFun:573`, `server.repair.StoreRepairerSuite.updateObject:121`
- SystemTaskServiceStateSuite: `compute.systemtask.SystemTaskServiceStateSuite.testFun:33`, `compute.systemtask.SystemTaskServiceStateSuite.testFun:56`
- TKVLBatchRequirementSuite: `client.tkvl.TKVLBatchRequirementSuite.testFun:185`, `client.tkvl.TreeKindSuiteBase.inTransaction:117`, `client.tkvl.TreeKindSuiteBase.newTree:105`, `client.tkvl.TreeKindSuiteBase.rootHost:93`
- TKVLBatchSuite: `client.tkvl.TreeKindSuiteBase.inTransaction:117`, `client.tkvl.TreeKindSuiteBase.newTree:105`, `client.tkvl.TreeKindSuiteBase.rootHost:93`
- TKVLSuite: `client.tkvl.TKVLSuite.buildSplitTree:424`, `client.tkvl.TKVLSuite.buildSplitTree:431`, `client.tkvl.TKVLSuite.insertSequentially:411`, `client.tkvl.TKVLSuite.testFun:115`, `client.tkvl.TKVLSuite.testFun:122`, `client.tkvl.TKVLSuite.testFun:156`, `client.tkvl.TKVLSuite.testFun:163`, `client.tkvl.TKVLSuite.testFun:172`, `client.tkvl.TKVLSuite.testFun:217`, `client.tkvl.TKVLSuite.testFun:224`, `client.tkvl.TKVLSuite.testFun:233`, `client.tkvl.TKVLSuite.testFun:24`, `client.tkvl.TKVLSuite.testFun:277`, `client.tkvl.TKVLSuite.testFun:284`, `client.tkvl.TKVLSuite.testFun:31`, `client.tkvl.TKVLSuite.testFun:332`, `client.tkvl.TKVLSuite.testFun:339`, `client.tkvl.TKVLSuite.testFun:348`, `client.tkvl.TKVLSuite.testFun:361`, `client.tkvl.TKVLSuite.testFun:558`, `client.tkvl.TKVLSuite.testFun:64`, `client.tkvl.TKVLSuite.testFun:71`, `client.tkvl.TKVLSuite.testFun:80`
- ThreadedHostSuite: `server.host.ThreadedHostSuite.tx:30`
- TierCacheTreeSuite: `client.tkvl.TreeKindSuiteBase.inTransaction:117`, `client.tkvl.TreeKindSuiteBase.newTree:105`, `client.tkvl.TreeKindSuiteBase.rootHost:93`
- TierEngineDescentSuite: `client.tkvl.TierEngineDescentSuite.batchThroughTheGap:151`, `client.tkvl.TierEngineDescentSuite.newTree:35`, `client.tkvl.TierEngineDescentSuite.newTree:40`, `client.tkvl.TierEngineDescentSuite.sequentially:71`
- TierEngineSuite: `client.tkvl.TierEngineSuite.insert:72`, `client.tkvl.TierEngineSuite.multiTierTree:78`, `client.tkvl.TierEngineSuite.multiTierTree:85`
- TieredKeyValueListSplitTreeSuite: `client.tkvl.TieredKeyValueListSplitTreeSuite.testFun:132`, `client.tkvl.TieredKeyValueListSplitTreeSuite.testFun:139`, `client.tkvl.TieredKeyValueListSplitTreeSuite.testFun:149`, `client.tkvl.TieredKeyValueListSplitTreeSuite.testFun:154`, `client.tkvl.TieredKeyValueListSplitTreeSuite.testFun:159`, `client.tkvl.TieredKeyValueListSplitTreeSuite.testFun:227`, `client.tkvl.TieredKeyValueListSplitTreeSuite.testFun:23`, `client.tkvl.TieredKeyValueListSplitTreeSuite.testFun:234`, `client.tkvl.TieredKeyValueListSplitTreeSuite.testFun:244`, `client.tkvl.TieredKeyValueListSplitTreeSuite.testFun:249`, `client.tkvl.TieredKeyValueListSplitTreeSuite.testFun:30`, `client.tkvl.TieredKeyValueListSplitTreeSuite.testFun:307`, `client.tkvl.TieredKeyValueListSplitTreeSuite.testFun:314`, `client.tkvl.TieredKeyValueListSplitTreeSuite.testFun:365`, `client.tkvl.TieredKeyValueListSplitTreeSuite.testFun:372`, `client.tkvl.TieredKeyValueListSplitTreeSuite.testFun:382`, `client.tkvl.TieredKeyValueListSplitTreeSuite.testFun:430`, `client.tkvl.TieredKeyValueListSplitTreeSuite.testFun:437`, `client.tkvl.TieredKeyValueListSplitTreeSuite.testFun:475`, `client.tkvl.TieredKeyValueListSplitTreeSuite.testFun:482`, `client.tkvl.TieredKeyValueListSplitTreeSuite.testFun:492`, `client.tkvl.TieredKeyValueListSplitTreeSuite.testFun:514`, `client.tkvl.TieredKeyValueListSplitTreeSuite.testFun:67`, `client.tkvl.TieredKeyValueListSplitTreeSuite.testFun:74`
- TransactionBuilderSuite: `(builder used directly)`
- TransactionFailureSuite: `client.TransactionFailureSuite.testFun:71`, `client.TransactionFailureSuite.testFun:93`
- TransactionMaxSizeSuite: `client.internal.transaction.TransactionMaxSizeSuite.mkBuilder:31`
- TransferStarterSuite: `IntegrationTestSuite.setStoreSize:114`
- TreeKindParitySuite: `client.tkvl.TreeKindSuiteBase.inTransaction:117`, `client.tkvl.TreeKindSuiteBase.newTree:105`, `client.tkvl.TreeKindSuiteBase.rootHost:93`
- UUIDObjectRegistrySuite: `client.registries.UUIDObjectRegistrySuite.createRegistry:21`, `client.registries.UUIDObjectRegistrySuite.createRegistry:27`
- UnackedCommitsIntegrationSuite: `server.UnackedCommitsIntegrationSuite.commitHeldBy:112`, `server.UnackedCommitsIntegrationSuite.testFun:249`, `server.UnackedCommitsIntegrationSuite.treeHoldingK:183`, `server.UnackedCommitsIntegrationSuite.treeHoldingK:187`
