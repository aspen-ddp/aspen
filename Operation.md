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
device will be created. All the DataStore instances backing the `aspen-bootstrap` pool will
are initially stored on the single storage device. This is the basis from which the
deployment will grow. To expand the system, add new hosts and new storage devices.

The `<address>` argument is needed to facilitate the expansion of the system. To add a
new host, copy the `bootstrap-host/aspen-bootstrap-config.yaml` file to the machine to
be added and invoke the `aspen add-host` command. The CLI will use the config yaml file to
look up the address provided to the `bootstrap` command and connect to the Aspen host
process on the bootstrap machine. It'll then create the metadata for the new host inside
the Aspen system and set up the local directories and files to support the new host.

Once a new host is added, `aspen create-storage-device` can be used to add storage devices.
When a device is created, the storage device set it belongs to must be specified. Adding
devices to the `bootstrap` device set allows the DataStores backing the `aspen-bootstrap`
pool to be spread across additional devices. The rebalancing algorithm will try to
ensure that at most one DataStore per pool is stored on a single storage device or host
machine. It will only double-up on devices and hosts if insufficient devices or hosts are
available.

One important point to note is that `aspen-bootstrap-config.yaml` file is **not** static.
Each host maintains a writeable copy of this config file in the root host directory and
updates the file as the DataStores backing the `aspen-bootsrap` pool are migrated throughout
the system. An up-to-date copy of this file is needed to invoke CLI commands and, currently,
this must be managed by hand. This is an obvious pain point and a more convenient mechanism, 
such as an API to obtain a copy of the current config from a running host, will need to 
be added in the near future.

## Operation

Once the system is bootstrapped, the storage pools, allocation groups, and storage device
sets will need to be created to support the application and runtime environment. How best
to set up and manage this is currently and open-ended question. Given that Aspen is
presently in the late alpha/early-beta stage, there isn't much practical experience 
to guide decisions so a few trailblazers will be needed to figure this out.
