### Server-wide rules
- Package guides: `host/` (threading implementation, device lifecycle), `store/` (Frontend, backends, cache),
  `transaction/` (Paxos/Tx protocol), `crl/`, `repair/`, `rebuild/`. Safety arguments: `docs/safety/`
- Threading: the model deliberately departs from ordinary Scala Future usage: callbacks never run on a shared pool,
  and each piece of state has a single owner thread (the control thread or one store's executor), actor-style. See
  the scaladoc of `host/HostThreading.scala`. Everything touching a `Store`, `Frontend` or `Tx` runs on that store's
  executor, so they take no locks. The exception is `Store.transactionDrivers`, a `@volatile` immutable Map so
  `hasTransactions`/`logTransactionStatus` may read it from any thread; `TransactionDriver`s keep their own locks for
  their timer threads. Details: `host/CLAUDE.md`
- `server.DurableFiles` forces files and their directories so markers and moves survive a pulled disk. That
  durability holds on Linux only, the supported host platform: directory fsync is a no-op on Windows and a plain fsync
  on macOS, each logging one warning
- `aspen host --load-jars=<version>` loads user RegisteredTypeFactory lists from every jar in
  `<host-dir>/jars/<version>/` (may be a symlink, e.g. `latest`) via `UserJarLoader`: one URLClassLoader,
  parent-first, `AspenTypeProvider` classes found with ServiceLoader. Duplicate UUIDs (including against built-ins)
  fail startup naming both classes and jars
- Periodic diagnostics: `AbortVoteReport` (`transaction/`) logs a host's abort votes by failed requirement once a
  second while they happen; `BackendTiming` (`store/backend/`) logs each host's write stages about once a second while
  writes happen: the store's view of a commit, RocksDB's pool wait and write, and FSIF's io wait, journal write and
  force, data force, copy-on-write and backpressure
