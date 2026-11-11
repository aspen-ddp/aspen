### Host threading model
- `HostThreading` decides where a host's work runs. It is a `Host` constructor parameter, chosen in
  code. The default strategy, which `Main` passes, is `StoreThreadPool`: each store's executor is its own
  `StoreMailbox` (a FIFO with at most one drain at a time, so the store's tasks never overlap and keep their order),
  scheduled onto one of the `aspen-store-N` daemon threads. By default the pool is fixed: up to
  `aspen host --store-threads` threads (default a quarter of the CPUs, at most 4), created lazily, each new store on
  the thread with the fewest stores; stores never move. Fewer busy threads beat many idle ones: waking a parked thread
  for each task multiplied CPU per operation, and fixed pools of 2 to 4 threads beat one per CPU and both adaptive
  policies tried (docs/host-threading-measurement.md). `StoreThreadPool.Adaptive` (code only, parked until revisited)
  runs `StoreBalancer` instead: it packs stores by load, grows on drain-wait congestion and moves stores between threads;
  a move only changes where the mailbox's next drain is queued. Its known problems are in `StoreThreadPool`'s scaladoc
  and `Todo.txt`
- The hot path (network messages, I/O completions, heartbeats, repairs) goes through `StoreRouter` straight to the
  store's executor: a concurrent map lookup on the calling thread, then the task, which runs only if that store is
  not closed when the task runs (tasks queued ahead of a close still run). Unknown stores are answered from the
  calling thread; offline ones are silent, as are all unknown ones until the startup scan's LoadStores are handled.
  A store whose load is under way is silent too: `loading` from `StoreLoader.startOpen` until install, and
  `awaitingDevice` while its device's shutdown-marker lookup (or adoption) decides. Neither mark is `offline`
- Backend opens are backend-neutral: `BackendOpener.open` (`server.store.backend`) returns a Future and decides where
  the open runs (RocksDB inline, FSIF on `FsifOpenEc`); `StoreLoader.startOpen` handles an already-completed one
  inline and otherwise posts `BackendOpened`. A rebuild's staging store opens through the same opener (passed by
  `StoreRebuildFactory.createRebuild`). The host and the rebuild see only `Backend.hasFailed`/`isWriteFrozen`,
  `BackendStateLost` and `Backend.isOutOfSpace`; only `DefaultBackendOpener` names concrete backends
- Store lifecycle: the control thread builds the `Store` and calls `router.install`, where ownership passes to the
  executor; a close removes it from the router (marking it offline) and then runs the close on its executor. Before
  install and after close the backend and directory belong to the control side, which otherwise reads only
  `storeId`, `path`, `crlSaveFile` and the thread-safe `hasFailed`/`isWriteFrozen`. `release` frees the slot
- A loading store asks for its CRL state with the non-blocking `readStoreState` from its executor
  (`StoreRouter.readAndRecover`), holding its messages until it recovers, so no store thread blocks on a CRL
- Device management (the classes `DeviceControl` wires) runs on one control thread, `aspen-host-control`,
  with no locks. Rules (scaladoc of `HostThreading.scala`): the only `given ExecutionContext` is the control context;
  never block the control thread on a Future; other threads read control state only through the `@volatile`
  `HostStoreView` (an immutable snapshot the control thread republishes on every change: `containsStore`,
  `getStoreIds`, `getDevicePath`, `isRepairable`) or the router's concurrent map
  (`hasTransactions`, `logTransactionStatus`); store and control state stay apart: control posts store work to the
  executor (`StoreRouter.onStore`) and gets results back through a Future whose callbacks name the control context
- Callbacks name the control context. Work run elsewhere (backend opens, client transactions on the host's `ec`) names
  its context; timers and other non-Future callbacks post to `controlEvents` before touching control state
- Tests use `LockstepThreading`: everything posted waits on one FIFO drained by `testingOnlyHandleEvents`, stores run
  inline, and the control context is the suite's serial `ec`, so integration suites keep their exact ordering.
  `ThreadedHostSuite` runs the TestNetwork's host on a fixed `StoreThreadPool` (`IntegrationTestSuite.testThreading`),
  and `AdaptiveThreadedHostSuite` on an adaptive one that moves stores between threads mid-test

### Host and StorageDevice Management
- Most administration is done by reading and writing host and device objects containing the desired state
- Hosts will poll these instances and act on changes
  - This approach is preferred over sending messages directly to the host, since those messages could be lost
  - An example is seeing that a DataStore is marked for transfer in or out. When seen, the host will start the process
- Each storage device has its own CRL (`<device>/crl`, opened when the device loads; `server/crl/CLAUDE.md`) shared by
  its stores; a dead CRL thread stalls that device. An unloaded device is not loaded again until its CRL has shut down
  (`LocalDevices.closingCrls`)
- Planned physical moves: `shutdownStorageDevice` makes Active stores Offline and the device ShuttingDown; its host
  closes the stores, leaving their CRL state live in the device CRL (no per-store marker or save file), writes
  `shutdown.state` (its HostId) in the device dir, and commits Shutdown. Each device dir also has `owning-host`
  (`server.DeviceMarkers`), so the 10 s scan decides without the network; only a dir with `shutdown.state` needs a
  device-state lookup. On the same host a Shutdown device waits for `restartStorageDevice`; a restart that cancels a
  shutdown in flight has the reconcile load the closed stores again (`DeviceShutdown.reloadClosedStores`). Another host
  adopts it, in one transaction rewriting the device, its pool entries and both HostStates, then rewrites
  `owning-host` and loads it: it opens the device CRL and each store recovers its transactions from it
- Adoption needs no shutdown: a drive pulled from a running or crashed host is in the state that host's crash leaves
  it in. A device whose `shutdown.state` (or, absent that, `owning-host`) names another host is looked up and adopted
  when the tree records it as owned by that host and not Failed, `<device>/crl` exists (if it holds stores), and
  every Offline or Active entry has a directory. Physical presence is trusted: there is no fencing of the old host.
  A running old host whose reconcile finds the tree naming another owner relinquishes the device
  (`DeviceShutdown.relinquishDevice`): it closes the stores, unloads the device, writes nothing, and keeps the
  directory disowned in memory. The adopter nudges it with `CheckStorageDevice`
- `crl_save.log`, `transferring-out` and `transferred-in` are for network transfers only: a transfer out closes the
  store with `extractCrl`, copying its state from the device CRL into `crl_save.log` to travel with it. Such a file is
  then the only copy of that state: if it fails to load (missing, truncated, or naming another store) the store is
  never served. It gets a `rebuild-required` marker, its entry is flipped to Rebuilding by the reconcile, and the
  directory is moved to `pending-delete/` before the rebuild. Adoption of a device does the same for a `transferring-out` store
  without a valid save file instead of refusing the device, and adopts any `rebuild-required` directory as Rebuilding
  whatever its other markers. A network transfer validates the staged copy's save file before promotion and retries on
  failure, since the source still holds the state
- A lost device CRL does the same to every store on the device: if `<device>/crl` is absent from a device holding store
  dirs, or `openCRL` throws, each store dir gets `rebuild-required`, the old directory is kept as `crl.lost-<uuid>`, and
  a fresh CRL is opened (`DeviceDiscovery.openDeviceCrl`). A transient error (I/O or VM error,
  `DeviceDiscovery.isTransient`) leaves the device unloaded for the next scan instead, until `CrlOpenAttempts` (3) in a row
- A backend fail-stop during a store's close marks it `rebuild-required` instead of closing it cleanly (no
  `crl_save.log`). Such a store is never transferred: its transfer is aborted so it is rebuilt
- A store's device-CRL entries are dropped for good (`CrashRecoveryLog.dropStore`, durable) only once it cannot be
  served: when a rebuild discards a `rebuild-required` directory (`StoreRebuilds.discardLostStore`: marker durable,
  then the entries saved into the directory's `lost-crl-state.log`, then the drop durable, then the directory moved
  to `<device>/pending-delete/` and the rebuild started), or when an unloaded store's save file still
  holds them, or when the reconcile deletes the directory of a store transferred away (`DeviceReconciler`, retried
  on failure). Without the marker on disk, entries stay in the device CRL. A transfer out reads them
  (`readStoreState`, leaving them live) into `crl_save.log` (loaded or not, `StoreTransfers.closeForTransferOut`)
  and deletes them from the device CRL (`deleteTransactions`) only once that file is durable: a crash or failed save
  before that leaves the device CRL holding them
- Nothing of a lost store is destroyed before its rebuild succeeds: its `pending-delete/` copy (data plus saved CRL
  entries) is deleted by the device's reconcile only once the store is Active again, on this device by its own
  entry, elsewhere by the pool's entry and that device's state; a failed read keeps it. A fail-stopped backend's CRL
  entries likewise stay live until that discard saves them
- A device the tree records Failed is retired by its host's reconcile (`DeviceShutdown.retireFailedDevice`): a
  durable `failed` marker (`DeviceMarkers`), then unloaded as a relinquished device is. A device with the marker is
  never loaded or claimed; nothing on it is deleted. The tombstone (`FailedStorageDeviceDurableTask`) nudges the
  former host with `CheckStorageDevice`. A repair scan runs only while the store's pool entry names this host
  (`StoreRepairer`), so a superseded copy never consumes the current copy's ErrorTree entries
- `DurableFiles` (Linux-only directory durability): `server/CLAUDE.md`
