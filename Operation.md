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
aspen bootstrap [options] <target-directory> <address> <ida-type> <read-threshold> <write-threshold> <width>
```

Aspen deployments always start with three things:
1. A single host named `bootstrap-host`
2. A single storage device identified by a random UUID
3. A single storage device set named `bootstrap`

The `target-directory` provided to the command is where the host instance and storage
device will be created. All the DataStore instances backing the `aspen-bootstrap` pool
are initially stored on the single storage device. This is the basis from which the
deployment will grow. To expand the system, add new hosts and new storage devices.

The `<address>` argument is needed to facilitate the expansion of the system. It is combined with
the host's `http-port` (default 4751, `--http-port`) to form the first entry in the client config's
list of bootstrap hosts. To add a new host, copy `aspen-client-config.yaml` (see below) to the
machine to be added and invoke the `aspen add-host` command. The CLI fetches the current bootstrap
config from the bootstrap host and connects to the Aspen host process on that machine. It'll then
create the metadata for the new host inside
the Aspen system and set up the local directories and files to support the new host.

Once a new host is added, `aspen create-storage-device` can be used to add storage devices.
When a device is created, the storage device set it belongs to must be specified. Adding
devices to the `bootstrap` device set allows the DataStores backing the `aspen-bootstrap`
pool to be spread across additional devices. The rebalancing algorithm will try to
ensure that at most one DataStore per pool is stored on a single storage device or host
machine. It will only double-up on devices and hosts if insufficient devices or hosts are
available.

## The Client Config and the Bootstrap Config

The `aspen-bootstrap-config.yaml` file is **not** static. Each host maintains a writeable copy in
its root host directory and updates it as the DataStores backing the `aspen-bootsrap` pool are
migrated throughout the system. Every host serves the current copy over plain HTTP on its
`http-port` (default 4751, `--http-port`): `GET /bootstrap` returns the Radicle copy, falling back
to the host's on-disk copy if the Radicle cannot be read within two seconds, and responds 503 if
both fail. This endpoint is unauthenticated, which is an accepted risk for now.

Clients never need a hand-maintained copy of the bootstrap config. Instead, every client command
(everything except `bootstrap` and `host`, including `amoebafs`) takes an
`aspen-client-config.yaml` file. `bootstrap` writes it to `<target-directory>/aspen-client-config.yaml`
with owner-only (0600) permissions. It contains:

- `aspen-system-id`: the system the client belongs to
- `certificate` and `private-key`: the client's TLS identity. They are loaded and validated, but
  not yet used for connections, as TLS is not enforced yet.
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

## Operation

Once the system is bootstrapped, the storage pools, allocation groups, and storage device
sets will need to be created to support the application and runtime environment. How best
to set up and manage this is currently and open-ended question. Given that Aspen is
presently in the late alpha/early-beta stage, there isn't much practical experience 
to guide decisions so a few trailblazers will be needed to figure this out.
