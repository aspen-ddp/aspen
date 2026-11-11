### System metadata objects
These are the persisted state objects for pools, allocation groups, hosts, storage devices and device sets. Each kind
has a tree in the Radicle (`StoragePoolsTreeKey`, `AllocationGroupsTreeKey`, `HostsTreeKey`, `StorageDevicesTreeKey`,
`StorageDeviceSetsTreeKey`), and all of them live in the bootstrap pool. Every one is protobuf-encoded through
`Codec`. Changes are read-modify-write transactions guarded by a `KeyRevision` on the state key (KV objects) or by
the object revision (DataObjects), so a racing writer aborts rather than overwrites. Hosts poll these objects and act
on changes: administration means writing desired state here, not sending messages.

#### Fixed ids (`fixed_ids`)
- Radicle `UUID(0,0)`, bootstrap pool `(0,1)`, bootstrap device `(0,2)`, bootstrap host `(0,3)`, bootstrap device
  set `(0,4)`, initial metadata pool `(0,5)` (fixed because the CLI creates its store directories before
  `Bootstrap.initialize` runs), default metadata group `(0,6)`
- `FailedHostId`/`FailedStorageDeviceId` are `UUID(0,0)`: a tombstoned device has both ids zeroed, but
  `DeviceStatus.Failed` is authoritative; a zeroed id on an Operational device does not make it failed

#### StoragePoolState (KV object)
- Keys: `ConfigKey` (the encoded state), `ErrorTreeKey`, `AllocationTreeKey`, `AllocationStrategyKey`/
  `AllocationStrategyConfigKey`, `RepairPolicyKey`, `UnackedCommitsTreeKey`, plus per-store usage under
  `getStoreUsageKey(poolIndex)` (`sz<idx>`)
- `stores(i)` is a `StoreEntry(hostId, storageDeviceId)`: the pool's record of where store `i` lives. A transfer
  leaves it naming the source until the transfer completes, and the completing transaction rewrites it. Rebuild
  device selection derives the failed device from it, so select before repointing
- `maxSliceSize` (declared) and `backendConfig.maxSliceSize` are both fixed at creation. Stores enforce
  `effectiveMaxSliceSize`, the smaller of the two; most code should use `maxObjectSize`, that limit converted
  through the IDA
- `isMetadataPool` is fixed at creation. `allocationGroups` lists the groups the pool belongs to (kept in step with
  the groups' member lists by `AllocationGroupState.addPool`/`removePool`)
- `storageDeviceSet` is the set its stores are placed in; `migration` (`InProgress`, then `Complete`, which persists
  as the record of the last migration) tracks a move to another set by `MigratePoolToSetDurableTask`
- Metadata layout: an ordinary pool's ErrorTree/AllocationTree/UnackedCommits trees live in a metadata pool reached
  through a metadata group; metadata pools' trees live in the bootstrap pool; the bootstrap pool's live in itself.
  All pool state objects stay in the bootstrap pool. Metadata pools can't use FSIF
  (`client/internal/allocation/CLAUDE.md`)

#### AllocationGroupState (DataObject)
- Allocate from member pools and child groups (`Member`: type, uuid, usage and maximum size). `level` orders the
  hierarchy: a group may only contain groups of a lower level. `parentGroups` is kept in step with parents' member
  lists by `addGroup`/`removeGroup`
- `selectMemberForAllocation` picks a member weighted by free space (`maximumSize` 0 means unlimited, weight 1)
- `idaPolicy` (`IDAPolicy.ExactMatch` or `AllowAll`) and `ida` (`Some` exactly for `ExactMatch`) are fixed at
  creation. Members of an `ExactMatch` group must have an IDA `==` the group's (type and every parameter;
  `IDA.compare` orders by failure tolerance only and must not be used); otherwise `MemberIDAMismatch`
- An optional `maxSliceSize`, fixed at creation and allowed only with `ExactMatch`, is a floor: members must have no
  limit or one at least as large, and an unlimited group accepts only unlimited members
  (`MemberLimitIncompatible`). `maxSliceSize` is the source of truth for pools and groups; every `maxObjectSize` is
  derived from it and the IDA
- `isMetadataGroup` is fixed at creation. A pool or child group may join a group only when its flag equals the
  group's (`MetadataMembershipMismatch`)
- All membership checks throw before any state changes
- Usage propagation: a member's usage or size change enrolls an `UpdateAllocationGroupUsageTask` (in the same
  transaction) for its parent groups. The task reads the child's current state when it updates each group, so it
  carries no usage; it reads the group before the child so a later change collides instead of being overwritten.
  It cascades to a group's own parents only when usage or maximum size moved by more than 0.5%
- Bootstrap creates `aspen-initial-storage-pool-metadata` and `aspen-default-storage-pool-metadata`; the group id is
  in the Radicle (`DefaultStoragePoolMetadataAllocator`, `AspenClient.getDefaultMetadataGroupId`).
  `createNewStoragePool(..., metadataGroup)` defaults to it; `createNewMetadataPool` and CLI `create-metadata-pool`,
  `create-pool --metadata-group`, `create-allocation-group --metadata` build on it

#### HostState (KV object)
- Keys: `StateKey` (id, name, address, data/http/store-transfer ports, owned `storageDevices`),
  `TaskExecutorRootKey` (the host's DurableTask executor root, inserted by the Host on first start),
  `RepairLimitsKey` (read by `server.repair.HostRepairLimits`)
- `storageDevices` must agree with each device's `hostId`: adoption rewrites the device, its pool entries and both
  HostStates in one transaction (`server/host/CLAUDE.md`)

#### StorageDeviceState (KV object, `StateKey`)
- Owning `hostId`, `currentUsage` (self-reported) and `totalSize`, the device's `storageDeviceSet`, and a store map of
  `StoreEntry(status, transferDevice, transferId)`
- `StoreStatus`: Initializing, Active, TransferringIn, TransferringOut, Rebuilding, Offline. A `transferId` is shared
  by a transfer's TransferringOut and TransferringIn entries and carried in the on-disk markers, so a copy left by an
  aborted transfer is never mistaken for the current one. A TransferringIn entry does not own the store: the source
  does until the transfer completes
- `DeviceStatus`: Operational; ShuttingDown and Shutdown bracket a planned physical move; Failed is the one-way
  tombstone written by `failStorageDevice`. A failed device stays readable (its store map is the list awaiting
  rebuild) but is in no set or host and is never a destination
- `serviceStatus`: every transaction that makes an entry Active (store creation, rebuild or transfer in finishing, a
  transfer out rolled back) reads the state under a KeyRevision and writes this: Active, or Offline while the device
  is ShuttingDown/Shutdown. That is what keeps a shut-down device from ever holding an Active entry; only a restart
  or adoption turns those Offline entries Active (`withOfflineStoresActive`)
- `incomingStores` (transfers in, rebuilds in progress, and planned transfers from the set's `pendingTransfers`) are
  reserved at full size against free space, so concurrent placements cannot all pick one empty device
- `restoreAbandonedTransferSource` puts a source entry back when its destination was declared dead; `None` means a
  racing writer already did it, and writing nothing is then correct

#### StorageDeviceSetState (DataObject)
- Hierarchical grouping of physical storage, parallel to AllocationGroupState. `level` 0 holds `memberDevices`
  only; `level` >= 1 holds `memberSets` only. `parent` is documented but not enforced or maintained
- `assignedPools` lists the pools placed in it; `pendingTransfers` (store, from, to) is the set's rebalance plan
  (`common.rebalancing.SetRebalanceDurableTask`)
- Only level-0 sets hold devices: `moveDevice` moves a device between level-0 sets in one transaction (device state,
  target set, old set; self-healing if the old set did not list it) and refuses a tombstoned device
- Selection walks downward: `selectDevicesForPool` (level 1+ spreads the request round-robin across member sets),
  `selectDeviceForRebuild` (failed device hard-excluded, the pool's other devices soft-excluded, free space
  required), `selectDeviceForStore` (migration). Exclusion is soft so a set narrower than the IDA still fills.
  Non-operational devices are skipped
- `collectMemberDevices` is the "already in the target set?" check for pool migration. It walks downward like
  selection: an upward walk could miss a device selection just used and loop the migration forever

#### Management tasks (`management/`)
All three keep no progress record of their own where they can avoid it: each re-reads the objects it changes, so a
restarted instance picks up where a crashed one stopped.
- `UpdateAllocationGroupUsageTask`: usage propagation, above
- `MigratePoolToSetDurableTask`: moves a pool's stores onto its target set, as many transfers at once as
  `common.rebalancing.TransferSafety` admits. A store is migrated iff its device is in the target set; the target
  is re-read every pass, so a retarget needs no task change
- `FailedStorageDeviceDurableTask`: step 1 tombstones the device (leaves its set and host, status Failed, ids
  zeroed); step 2 moves one store per pass to a device from its pool's set, marked Rebuilding. The tombstone's store
  map is the work list. It skips entries that are stale bookkeeping (TransferringIn, or the pool no longer names this
  device for that index), since rebuilding those would orphan the live copy. It reconstructs no data: the Rebuilding
  entry tells the destination Host to rebuild, and the former host is nudged with `CheckStorageDevice`
