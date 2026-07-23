# CLI Listing Commands — Design

**Date:** 2026-07-23
**Status:** Approved
**TODO item:** "Add CLI listing commands" (top of `TODO.txt`)

## Goal

Add four CLI commands that list the named entities registered in an Aspen system:

- `list-pools`
- `list-hosts`
- `list-allocation-groups`
- `list-device-sets`

Each prints a two-column listing of **registered name** and **UUID**. Detailed
per-entity output is intentionally out of scope — that is left to the planned
`show-*` commands.

Supporting this requires a new registry-scanning capability
(`NamespacedUUIDRegistry.getAllEntries`) and four typed listing methods on the
`AspenClient` trait.

## Background

Named entities are stored in a single `NamespacedUUIDRegistry` rooted at
`Radicle.NamespacedRegistryKey`. It wraps a `Registry`, which wraps a
`TieredKeyValueList` (TKVL). Keys have the form `"<namespace>.<name>"` and values
are 16-byte UUID encodings. The namespaces are:

| Entity            | Namespace     | Existing lookup                       |
|-------------------|---------------|---------------------------------------|
| Storage pool      | `"pool"`      | `getStoragePoolId(name)`              |
| Host              | `"host"`      | `getHostId(name)`                     |
| Allocation group  | `"group"`     | `getAllocationGroupId(name)`          |
| Storage device set| `"device-set"`| `getStorageDeviceSetId(name)`         |

All four namespaces share the same underlying TKVL. Keys are lexically ordered,
so entries are naturally returned sorted by name.

Storage **devices** are deliberately excluded here — they are not name-registered
in the `NamespacedUUIDRegistry` (they live in `storageDevicesTree`). The
separate `list-devices` TODO item handles those and is out of scope.

## Design

The change spans three layers, bottom-up.

### 1. Registry scan layer

**`client/registries/Registry.scala`** — add a prefix scan. `tkvl` is private to
`Registry`, so the scan primitive belongs here:

```scala
def scan(prefix: String): Future[List[(Key, Value)]] =
  val minKey = Key(s"$prefix.")
  val maxKey = Key(s"$prefix/")   // '/' (0x2F) is the byte after '.' (0x2E)
  val buf = scala.collection.mutable.ListBuffer[(Key, Value)]()
  tkvl.foreachInRange(minKey, maxKey, (_, key, vs) =>
    buf += key -> vs.value
    Future.unit
  ).map(_ => buf.toList)
```

`foreachInRange` (rather than `foreach`) is used because all four namespaces
share one TKVL; a bounded range scan visits only the target namespace's entries.
The `[prefix + ".", prefix + "/")` boundary works because `/` (0x2F) is the byte
immediately after `.` (0x2E) in ASCII, and none of the namespace strings contains
a byte ≥ 0x2F.

**Implementation note:** confirm `foreachInRange`'s boundary inclusivity during
implementation. Regardless of the outcome, apply a `keyStr.startsWith(s"$prefix.")`
filter (in the layer above) as a correctness safety belt.

**`client/registries/NamespacedUUIDRegistry.scala`** — expose the decoded form,
matching the TODO's requested signature (name stripped of prefix + UUID):

```scala
def getAllEntries(namespace: String): Future[List[(String, UUID)]] =
  registry.scan(namespace).map(_.collect {
    case (key, value) if new String(key.bytes, StandardCharsets.UTF_8).startsWith(s"$namespace.") =>
      val keyStr = new String(key.bytes, StandardCharsets.UTF_8)
      keyStr.substring(namespace.length + 1) -> decodeUUID(value)
  })
```

Results are lexically sorted by key (already ordered in the TKVL).

### 2. Client API layer

**`client/AspenClient.scala`** — add four typed methods next to the existing
`getStoragePoolId`/`getHostId`/etc. Each returns its entity's typed ID wrapper
(`PoolId`, `HostId`, `AllocationGroupId`, `StorageDeviceSetId` — all
`case class X(uuid: UUID) extends AnyVal`), for consistency with the rest of the
API (e.g. `getStoragePoolId` returns `PoolId`):

```scala
def listStoragePools(): Future[List[(String, PoolId)]]
def listHosts(): Future[List[(String, HostId)]]
def listAllocationGroups(): Future[List[(String, AllocationGroupId)]]
def listStorageDeviceSets(): Future[List[(String, StorageDeviceSetId)]]
```

**`client/internal/BaseAspenClient.scala`** — implement each by delegating to the
registry with the matching namespace and wrapping the raw UUID in its typed ID:

```scala
override def listStoragePools(): Future[List[(String, PoolId)]] =
  namespacedRegistry.getAllEntries(Namespaces.Pool).map(_.map((n, u) => n -> PoolId(u)))

override def listHosts(): Future[List[(String, HostId)]] =
  namespacedRegistry.getAllEntries(Namespaces.Host).map(_.map((n, u) => n -> HostId(u)))

override def listAllocationGroups(): Future[List[(String, AllocationGroupId)]] =
  namespacedRegistry.getAllEntries(Namespaces.Group).map(_.map((n, u) => n -> AllocationGroupId(u)))

override def listStorageDeviceSets(): Future[List[(String, StorageDeviceSetId)]] =
  namespacedRegistry.getAllEntries(Namespaces.DeviceSet).map(_.map((n, u) => n -> StorageDeviceSetId(u)))
```

`NamespacedUUIDRegistry.getAllEntries` continues to return raw `(String, UUID)`
pairs; the typed-ID wrapping happens at the client-API boundary.

**Namespace constants cleanup (in scope):** the namespace strings (`"pool"`,
`"host"`, `"group"`, `"device-set"`) are currently magic-string literals repeated
across the `getRegisteredObject`/`prepareRegisterObject` call sites in
`BaseAspenClient`. Hoist them into named constants (e.g. a private `object
Namespaces` with `Pool`, `Host`, `Group`, `DeviceSet`) and use those constants at
both the existing register/lookup sites and the new list methods, so the register
and list sides cannot drift apart.

### 3. CLI layer (`cmdline/Main.scala`)

Four new subcommands, each taking only the shared `<bootstrap-config-file>` arg,
following the existing scopt pattern (e.g. `create-device-set`):

```scala
cmd("list-pools").text("Lists all storage pools").
  action((_, c) => c.copy(mode = "list-pools")).
  children(
    arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
      action((x, c) => c.copy(bootstrapConfigFile = x)).
      validate(x => if (x.exists()) success else failure(s"Config file does not exist: $x")),
  )
```

...and likewise `list-hosts`, `list-allocation-groups`, `list-device-sets`.

Dispatch entries in the `cfg.mode` match. Each passes an `id => uuid` extractor
so the shared helper can print a clean UUID regardless of the typed-ID wrapper:

```scala
case "list-pools"             => list_entries(bootstrapConfigPath, "Storage Pools",     _.listStoragePools(),       _.uuid)
case "list-hosts"             => list_entries(bootstrapConfigPath, "Hosts",             _.listHosts(),              _.uuid)
case "list-allocation-groups" => list_entries(bootstrapConfigPath, "Allocation Groups", _.listAllocationGroups(),   _.uuid)
case "list-device-sets"       => list_entries(bootstrapConfigPath, "Device Sets",       _.listStorageDeviceSets(),  _.uuid)
```

A single shared, generic helper — the four commands differ only in title, which
client method they call, and how to extract the UUID for printing:

```scala
def list_entries[A](bootstrapConfigFile: os.Path,
                    title: String,
                    fetch: AspenClient => Future[List[(String, A)]],
                    idToUuid: A => UUID): Unit =
  configureLogging()
  val (client, network, _) = createAmoebaClient(bootstrapConfigFile)
  network.startIoThread(client)
  given ExecutionContext = client.clientContext

  val f = fetch(client)
  f.onComplete:
    case scala.util.Success(entries) =>
      if entries.isEmpty then
        println(s"No $title found")
      else
        // Sort by name explicitly for a deterministic listing order, independent
        // of the underlying registry's iteration order.
        val sorted = entries.sortBy(_._1)
        val width = sorted.map(_._1.length).max
        println(title)
        sorted.foreach { (name, id) => println(s"  ${name.padTo(width, ' ')}  ${idToUuid(id)}") }
    case scala.util.Failure(err) =>
      println(s"Error listing ${title.toLowerCase}: ${err.getMessage}")

  Await.ready(f, Duration(30, SECONDS))
```

Output is name-padded two columns, explicitly sorted by name before printing.
Empty registries print a `No <title> found` message.

## Testing

- **Unit test** for `NamespacedUUIDRegistry.getAllEntries`, using the
  `TestNetwork`/`TClient` harness (the pattern used by existing registry/client
  tests):
  - Register several entries across two different namespaces.
  - Assert `getAllEntries(ns)` returns only the matching namespace's pairs, with
    names stripped of the `"<namespace>."` prefix and correct UUIDs.
  - Assert results are sorted by name.
  - Include an empty-namespace case (returns `Nil`).
- The CLI command functions follow the existing untested-command convention
  (`create_device_set` and peers have no direct CLI test); behavioral coverage
  lands at the client/registry layer.

## Files Changed

| File | Change |
|------|--------|
| `client/registries/Registry.scala` | Add `scan(prefix): Future[List[(Key, Value)]]` |
| `client/registries/NamespacedUUIDRegistry.scala` | Add `getAllEntries(namespace): Future[List[(String, UUID)]]` |
| `client/AspenClient.scala` | Add four `list*` method declarations |
| `client/internal/BaseAspenClient.scala` | Implement the four methods; hoist namespace strings into constants and reuse at existing sites |
| `cmdline/Main.scala` | Add four subcommands, dispatch cases, and the `list_entries` helper |
| test file (registry/client suite) | Add `getAllEntries` unit tests |

## Out of Scope

- `list-devices` (storage devices are not name-registered; separate TODO item).
- `show-*` detail commands (separate TODO item).
- Any richer per-entity output beyond name + UUID.
