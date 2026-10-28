# Operating Aspen Deployments

This page will likely see a lot of updates in the future. Aspen is a young system and currently
everything must be done by hand. This is sufficient for early adopters but, obviously, more
advanced solutions will be needed to reach non-trivial scales. For the moment though,
everything is managed through a simple CLI.

The CLI is structured similar to Git where there is a top-level binary and all operations
are performed via sub commands such as `aspen list-hosts`. There are quite a few commands
and the majority of them are self-explanitory so not all commands will
be covered here. `aspen <command> --help` can be used to get command-specific details when
needed. Instead, this guide will walk through the overall workflow and implementation
details to provide and understanding of how everything is intended to work together and the
process of setting up and maintaining an Aspen deployment. Again, this tooling is very much
"early days" so expect changes.

## Bootstrapping an Aspen System
Bootstrapping a new Aspen system is done by way of the `bootstrap` CLI command and the key
arguments are the IDA configuration to use for the `aspen-bootstrap` pool which will contain
the metadata needed to bring up an Aspen system. 

```shell
aspen bootstrap [options] <host-directory> <hostname> <address> <ida-type> <read-threshold> <write-threshold> <width>
```

Aspen deployments always start with three things:
1. A single host, named by the `<hostname>` argument
2. A single storage device identified by a random UUID
3. A single storage device set named `bootstrap`

`<host-directory>` and `<hostname>` work exactly as they do for `add-host`. The host directory is
the new host's root directory. It is created if it does not exist, and an existing one must not
already hold an `aspen-host-config.yaml`. The host name can be anything and does not have to match
the directory name. The examples in this guide use `bootstrap-host`. Once `bootstrap` finishes,
this host is no different from any other, so on a machine that will stay in the system
long-term, choose the name you would give it with `add-host`.

The storage device is created inside the host directory. All the DataStore instances backing the
`aspen-bootstrap` pool are initially stored on the single storage device. This is the basis from
which the deployment will grow. To expand the system, add new hosts and new storage devices.

The `<address>` argument is needed to facilitate the expansion of the system. It is combined with
the host's `http-port` (default 4751, `--http-port`) to form the first entry in the client config's
list of bootstrap hosts. The `<ida-type> <read-threshold> <write-threshold> <width>` arguments define
the IDA of the bootstrap storage pool. More information on this can be found below in the section
that describes creating new storage pools. A good general-purpose choice would be a read-threshold of
10, a write-threshold of 12, and a width of 16.

To add a new host, copy `aspen-client-config.yaml` (see below) to the
machine to be added and invoke the `aspen add-host` command. The CLI fetches the current bootstrap
config from one of the hosts listed in the client config and connects to the running Aspen system.
Nothing needs to be running on the new machine yet. It'll then create the metadata for the new host
inside the Aspen system and set up the local directories and files to support the new host.

Once a new host is added, `aspen create-storage-device` can be used to add storage devices.
When a device is created, the storage device set it belongs to must be specified. Adding
devices to the `bootstrap` device set allows the DataStores backing the `aspen-bootstrap`
pool to be spread across additional devices. The rebalancing algorithm will try to
ensure that at most one DataStore per pool is stored on a single storage device or host
machine. It will only double-up on devices and hosts if insufficient devices or hosts are
available.

## The Client Config and the Bootstrap Config

The `aspen-bootstrap-config.yaml` file is **not** static. Each host maintains a writeable copy in
its root host directory and updates it as the DataStores backing the `aspen-bootstrap` pool are
migrated throughout the system. Every host serves the current copy over plain HTTP on its
`http-port` (default 4751, `--http-port`): `GET /bootstrap` returns the Radicle copy, falling back
to the host's on-disk copy if the Radicle cannot be read within two seconds, and responds 503 if
both fail. This endpoint is unauthenticated, which is an accepted risk for now.

Clients never need a hand-maintained copy of the bootstrap config. Instead, every client command
(everything except `bootstrap` and `host`, including `amoebafs`) takes an
`aspen-client-config.yaml` file. `bootstrap` writes it with owner-only (0600) permissions to the
current working directory, or to the file named by `--output-client-config <file>`. Before creating
anything, it checks that the file's directory exists and is writable and that the file does not
already exist. It contains:

- `aspen-system-id`: the system the client belongs to
- `certificate` and `private-key`: the client's TLS identity. The QUIC network plugin (see
  Networking below) presents them on every connection.
- `bootstrap-hosts`: a list of `address:port` entries for hosts to fetch the bootstrap config from.
  Initially this is only the bootstrap host.

Because it holds a private key, the client config must be treated as a secret: anyone who obtains
it can act as the CLI. To use Aspen from another machine, copy the same file there. All clients
currently share this one identity; minting per-client certificates is future work. The certificate's
UUID identifies the client to network plugins that use TLS; it is not the client id. Each CLI
process, including `amoebafs`, connects under its own randomly generated client id.

A client tries the cached hosts in order. `--bootstrap-host <host[:port]>` (default port 4751)
puts one more host at the front of the list, which is useful when every cached host is gone. If no
host answers, the command fails with "No cached hosts could be reached" and lists each attempt. A
config fetched from a different `aspen-system-id` is a hard error. When the fetched set of hosts
differs from the cache, the client config is rewritten if it is writable; otherwise a warning is
printed to stderr. Long-running `amoebafs` mounts update the cache whenever they refresh.

Bootstrapping also creates the Aspen certificate authority (CA). Each host's config file
(`aspen-host-config.yaml`) contains that host's TLS certificate and private key, signed by the CA.
The bootstrap yaml holds only the CA certificate. The client key is not stored in the bootstrap
yaml, in the host directories, or in the Radicle.

Systems bootstrapped by earlier versions are not supported: a fresh bootstrap is required.

## Networking

Hosts and clients communicate with Netty QUIC over UDP on each host's `data-port`. Every connection
is mutual TLS against the Aspen CA: hosts present the certificate in their host config, clients the
one in the client config.

## Operation

Once the system is bootstrapped, the storage pools, allocation groups, and storage device
sets will need to be created to support the application and runtime environment. How best
to set up and manage this is currently and open-ended question. Given that Aspen is
presently in the late alpha/early-beta stage, there isn't much practical experience 
to guide decisions so a few trailblazers will be needed to figure this out.

The rest of this section walks through the everyday tasks of growing a system: running hosts, adding
hosts and storage devices, organizing devices into sets, and creating the pools and allocation groups
your application stores its data in. The examples build a small system step by step. They assume the
client config written by `bootstrap` has been copied to `~/aspen/aspen-client-config.yaml` on every
machine you run commands from. Every client command takes it as its first argument.

Most commands print the UUID of whatever they create. Names are usually more convenient, and most
commands accept either. The `list-*` and `show-*` commands are useful for checking your work as you go:

```shell
aspen list-hosts ~/aspen/aspen-client-config.yaml
aspen show-host ~/aspen/aspen-client-config.yaml bootstrap-host
aspen list-devices ~/aspen/aspen-client-config.yaml bootstrap-host
aspen list-device-sets ~/aspen/aspen-client-config.yaml
aspen list-pools ~/aspen/aspen-client-config.yaml
aspen list-allocation-groups ~/aspen/aspen-client-config.yaml
```

### Running a Host

A host is a long-running process that serves the DataStores on the storage devices it owns. Start it by
pointing `aspen host` at its host directory:

```shell
aspen host /srv/aspen/bootstrap-host
```

The host runs in the foreground until the process is killed. Run it under whatever supervises your
long-running services (systemd, a container runtime, etc.) and have that restart it if it exits.

Aspen is deliberately "crash-only": there is no clean shutdown. Stopping a host means killing the
process, and restarting it is the normal recovery path. Nothing is lost by doing so, because every
committed transaction is already on disk.

#### Hosts and your application's code

If your application defines its own FinalizationActions, DurableTasks, or DurableServices, every host
needs that code too. The hosts are the ones that run it:

* **FinalizationActions** run on the hosts that store the objects a transaction changed.
* **DurableTasks** are handed to a randomly chosen host to run.
* **DurableServices** run on whichever host claims them, and move to another host if that one goes away.

Because any host may be asked to run any of these, **every host in the system must load the same
application jars**. A host that is missing them cannot run your types.

Package your types as described in "Loading Your Types into Hosts" in the
[Usage Guide](UsageGuide.md). Then place the jars in a version directory under each host directory and
start the host with `--load-jars`:

```
/srv/aspen/bootstrap-host/jars/
    1.0.0/myapp-assembly-1.0.0.jar
    latest -> 1.0.0
```

```shell
aspen host /srv/aspen/bootstrap-host --load-jars=latest
```

Using a `latest` symlink keeps the startup command the same across releases. To roll out a new
version of your application:

1. Copy the new jars to `jars/<new-version>/` on every host.
2. Repoint the symlink on each host: `ln -sfn 1.1.0 /srv/aspen/bootstrap-host/jars/latest`
3. Restart the hosts. Jars are only read at startup.

If a jar cannot be loaded, the host refuses to start and prints the reason. Typical reasons are a
missing dependency, a missing `META-INF/services` registration, or two types sharing a UUID.

### Adding a Host

To add a machine to the system, copy the client config to it and run `add-host` **on the new machine**.
It registers the host in the system and sets up its host directory locally:

```shell
aspen add-host ~/aspen/aspen-client-config.yaml /srv/aspen/host-b host-b 10.0.0.12
```

The arguments are:

* `<host-directory>`: where the new host's files will live. It is created if it does not exist.
* `<hostname>`: a unique name for the host.
* `<address>`: the IP address or DNS name other hosts and clients use to reach this machine.
  It must be reachable from the other machines. `127.0.0.1` only works if everything runs on one machine.

The host listens on three ports, which default to 4750 (data), 4751 (HTTP bootstrap endpoint), and 4752
(store transfers). Use `--data-port`, `--http-port`, and `--store-transfer-port` to change them, for
example when running several hosts on one machine for testing:

```shell
aspen add-host ~/aspen/aspen-client-config.yaml /srv/aspen/host-c host-c 10.0.0.12 \
    --data-port 5750 --http-port 5751 --store-transfer-port 5752
```

Then start it:

```shell
aspen host /srv/aspen/host-b --load-jars=latest
```

The host directory holds the host's TLS private key in `aspen-host-config.yaml`, so keep it readable
only by the account that runs the host.

Note that a new host will have no storage devices when it first starts. Instructions on how to add
them may be found below.

### Storage Device Sets

A storage device set is a named group of storage devices. Sets let you decide where each storage pool
keeps its data. A pool is created on a set and spreads its DataStores across that set's devices. Sets are
usually organized by the kind of storage, by location, or by both. For example:

* `ssd`: the fast NVMe drives, for the upper tiers of B-trees and other hot data
* `hdd`: the large, slow drives, for bulk data

Sets have a **level**:

* **Level 0** sets contain storage devices directly. Every device belongs to exactly one level-0 set.
  Level-0 sets are also where automatic rebalancing happens: Aspen moves DataStores between the devices
  in a level-0 set to keep their usage even.
* **Level 1 and above** contain other sets of a lower level. A pool created on a higher-level set spreads
  its DataStores across all the sets inside it. For example, a level-1 set `us-hdd` that contains
  `sacramento-hdd`, `austin-hdd`, and `boston-hdd` lets one pool keep a copy of its data at each site.

`bootstrap` creates one level-0 set named `bootstrap`, which holds the bootstrap device. Create a set
with `create-device-set <name> <level> [parent-set-name]`:

```shell
# Two level-0 sets for devices
aspen create-device-set ~/aspen/aspen-client-config.yaml ssd 0
aspen create-device-set ~/aspen/aspen-client-config.yaml hdd 0

# A level-1 set, then level-0 sets created directly inside it
aspen create-device-set ~/aspen/aspen-client-config.yaml us-hdd 1
aspen create-device-set ~/aspen/aspen-client-config.yaml sacramento-hdd 0 us-hdd
aspen create-device-set ~/aspen/aspen-client-config.yaml austin-hdd 0 us-hdd
```

A parent set must have a higher level than the sets placed inside it. Create the sets before the devices
that go into them. A device that is in the wrong set can be moved later with `move-device-to-set`.

### Adding a Storage Device

A storage device is one physical drive (or volume) owned by a host. Aspen treats each device as a single
unit that either works or has failed entirely, so give it a whole drive rather than a folder shared with
other things.

Each device is a directory directly inside the host directory's `storage-devices/` folder. Normally that
directory is the drive's mount point. Prepare it first, then register it **on the host's machine** with
`create-storage-device <host-directory> <device-name> <set-name>`:

```shell
# Mount the drive where Aspen expects to find it
sudo mkdir -p /srv/aspen/host-b/storage-devices/nvme0
sudo mount /dev/nvme0n1 /srv/aspen/host-b/storage-devices/nvme0
sudo chown aspen: /srv/aspen/host-b/storage-devices/nvme0

# Register it as a member of the "ssd" set
aspen create-storage-device ~/aspen/aspen-client-config.yaml /srv/aspen/host-b nvme0 ssd
```

The directory must already exist and must not already belong to a device. The set must be a level-0 set.
A running host picks up the new device right away, and in any case within about 10 seconds, without a
restart. A host loads only the devices it owns: a device directory recording another host as its owner
is logged once and left alone, never claimed. Each device is identified by a UUID, which the command
prints and `list-devices` shows.

A new device starts out empty. Rebalancing fills it over time: it runs automatically every 8 hours by
default, and new pools created on the set will also place stores on it. To spread existing data onto
the new devices right away, rebalance the set. The command takes the set's UUID, which
`list-device-sets` shows:

```shell
aspen list-device-sets ~/aspen/aspen-client-config.yaml
aspen rebalance ~/aspen/aspen-client-config.yaml 4b0c1d8e-5f8a-4d44-9a43-1f7e2c3b9d10
```

Adding devices to the `bootstrap` set lets the DataStores of the `aspen-bootstrap` pool, which holds the
system's own metadata, spread out across machines. Do this early, so the system's metadata does not
depend on the single bootstrap device.

### Creating a Storage Pool

A storage pool is where objects are actually stored. Each pool has:

* **An IDA**, which decides how each object is protected against data loss:
  * `replication`: every DataStore holds a full copy of each object. Simple and fast, but uses the most space.
  * `reed-solomon`: erasure coding. Each object is split into pieces plus parity, so it survives lost
    stores while using much less space than replication.
  * `random-access-reed-solomon`: erasure coding laid out so that objects can be appended to, read in
    ranges, and partially rewritten efficiently. Good for large, growing data such as file contents.
* **A width**: the number of DataStores in the pool. The pool spreads its DataStores across the
  devices of its set, one per device where possible.
* **A read threshold**: for erasure coding, the number of pieces each object is split into, and so the
  number of DataStores that must be read to rebuild it. It must be a majority of the width. Replication
  ignores it, since any one copy holds the whole object, but the argument is still required.
* **A write threshold**: how many DataStores must accept a write for it to succeed. It must be at least
  the read threshold, and for replication at least a majority of the width. The gap between the write
  threshold and the width is how many DataStores can be unavailable while writes keep working.
* **A storage device set**: where the pool's DataStores live.

Choosing the right IDA requires a good understanding of the IDAs TODO(Add IDA document) and the
application's design. The chosen IDA doesn't have to be ideally matched to the initial runtime
environment. Everything will work just fine on the bootstrap host with a single storage device.
If in doubt, a good general-purpose IDA is ReedSolomon with a read-threshold of 10, a write-threshold 
of 12, and a width of 16.

The command is `create-pool <pool-name> <ida-type> <width> <read-threshold> <write-threshold>
<device-set-name> [maximum-store-size]`:

```shell
# Erasure coded across 5 SSDs: any 3 pieces rebuild an object, writes need 4
aspen create-pool ~/aspen/aspen-client-config.yaml app-index reed-solomon 16 10 12 ssd

# Three full copies on the HDD set
aspen create-pool ~/aspen/aspen-client-config.yaml app-meta replication 3 2 2 hdd

# Appendable, range-readable bulk data, with a soft 100 GiB per-store size limit
aspen create-pool ~/aspen/aspen-client-config.yaml app-bulk random-access-reed-solomon 16 10 12 hdd
```

Note that the IDA choice needs to be guided by a blend of application design choices and operational
environment considerations. Just because the chosen IDA provides random-access-reed-solomon, an
application won't automatically be able to take advantage of the random-access capabilities of the
pool unless it is designed to do so. And, conversely, using a random-access-reed-solomon IDA for
pools that will host large numbers of small objects, and KeyValue objects in particular, will almost
always be a mistake. Make sure to read the TODO(IDA guide) to understand the tradeoffs involved.

Options:

* `--max-slice-size <bytes>` caps how much of any one object a single DataStore holds, which in turn
  caps the object size the pool accepts (see "Object Size Limits" in the [Usage Guide](UsageGuide.md)).
  It is fixed when the pool is created.
* `--cell-size <bytes>` sets the row cell size for `random-access-reed-solomon` (default 16).

A pool works best when its set has at least as many devices, on as many different hosts, as its width.
If there are fewer, some devices hold more than one of the pool's DataStores, so losing one device
costs the pool more than one DataStore. A set with no devices cannot hold a pool at all. A pool can be
moved to a different set later with `migrate-pool`.

### Allocation Groups

An application can allocate new objects directly from a pool, but pools fill up and a single pool limits
how far the data can spread. An **allocation group** lets the application allocate from a named group
instead, and Aspen picks a pool inside the group for each new object. Emptier pools get proportionally
more of the new objects. This gives you room to grow: when the group is getting full, create another
pool and add it to the group. The application keeps allocating from the same group name and never needs
to know.

Like device sets, groups have a **level**:

* **Level 0** groups contain storage pools.
* **Level 1 and above** contain other groups of a lower level. For example, a level-1 group could combine
  a level-0 group of pools in each data center.

A group is normally created with an IDA, and only pools and groups with exactly that IDA (the same type,
width, and thresholds) may join it. This stops a pool with weaker protection from being added by
mistake. The command is `create-allocation-group <name> <level> <ida-type> <width> <read-threshold>
<write-threshold>`, with the IDA arguments in the same order as `create-pool`:

```shell
# A group for the erasure-coded index pools
aspen create-allocation-group ~/aspen/aspen-client-config.yaml app-index 0 reed-solomon 5 3 4
```

Options:

* `--allow-any-ida` creates a group that accepts members with any IDA. The IDA arguments are left out:

  ```shell
  aspen create-allocation-group ~/aspen/aspen-client-config.yaml scratch 0 --allow-any-ida
  ```

* `--max-slice-size <bytes>` gives the group an object size limit. Members with no limit, or with a limit
  at least this large, may join, so every member can hold any object the group accepts. A group without a
  limit accepts only members without one. It requires an IDA, so it cannot be combined with
  `--allow-any-ida`.
* `--cell-size <bytes>` is used with `random-access-reed-solomon`, as for `create-pool`.

The group's IDA, policy, and size limit are fixed when it is created.

Applications allocate from a group by looking up its id and asking the client for its allocator:

```scala
for
  groupId   <- client.getAllocationGroupId("app-index")
  allocator <- client.getAllocator(GroupObjectAllocatorId(groupId))
yield allocator
```

### Adding Pools to Allocation Groups

Add a pool to a group with `add-pool-to-group <pool-name> <group-name>`:

```shell
aspen add-pool-to-group ~/aspen/aspen-client-config.yaml app-index app-index
```

When the group's pools start filling up, create another pool and add it. New allocations start using it
right away and favor it while it is emptier than the others:

```shell
aspen create-pool ~/aspen/aspen-client-config.yaml app-index-2 reed-solomon 5 3 4 ssd
aspen add-pool-to-group ~/aspen/aspen-client-config.yaml app-index-2 app-index
```

Adding fails with a clear error if the pool's IDA does not match the group's or its size limit is not
compatible with the group's. Groups are nested the same way with `add-group-to-group <source-group> <destination-group>`.
The destination must have a higher level than the source:

```shell
aspen create-allocation-group ~/aspen/aspen-client-config.yaml app-index-global 1 reed-solomon 5 3 4
aspen add-group-to-group ~/aspen/aspen-client-config.yaml app-index app-index-global
```
