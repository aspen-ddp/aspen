# vdisk end-to-end scripts

**Status: not yet run.** They need root, and were checked only with `bash -n`.

Root-only checks of `aspen vdisk-server` through the kernel: LIO, the TCMU user backstore `user:aspen`, iSCSI on
127.0.0.1, ext4 and fio. Nothing runs them automatically.

## Prerequisites
* Kernel modules `target_core_user` and `iscsi_target_mod` (`setup.sh` loads them).
* `targetcli` (targetcli-fb), `open-iscsi`, `fio`, `busctl` and `numfmt`.
* tcmu-runner stopped and disabled (`systemctl disable --now tcmu-runner`). vdisk-server replaces it, and the scripts
  refuse to run while it is active.
* A running Aspen system, a client config, and an existing disk (`aspen create-disk <client-config> vd1 10G --pool
  <pool>`).
* The `aspen` launcher's classpath cache: run `./aspen --help` once as yourself first, so that root does not invoke
  sbt. Under `sudo`, pass `JAVA_HOME` if java is not on root's PATH.

## Scripts
Each reads its settings from the environment; `common.sh` has the defaults (`DISK=vd1`, `SIZE=10G`,
`BLOCK_SIZE=4096`, `MOUNT=/mnt/$DISK`, `LOG=/var/log/aspen-vdisk-server.log`).

| Script | What it does |
|---|---|
| `setup.sh` | Installs `deploy/dbus/org.kernel.TCMUService1.conf` in `/etc/dbus-1/system.d/` and reloads D-Bus, starts `aspen vdisk-server`, checks that `busctl introspect org.kernel.TCMUService1 /org/kernel/TCMUService1/aspen` shows the handler and `targetcli ls /backstores` shows `user:aspen`, creates the backstore (`cfgstring=<disk>`: targetcli adds `aspen/`), target, LUN, ACL and portal, logs in, formats (only a blank disk, or `MKFS=1`), mounts and runs `smoke.fio`. |
| `restart-under-io.sh` | Runs `restart.fio` (synchronous writes with verify), kills vdisk-server with `kill -9` after `DELAY` seconds, restarts it, and checks that fio finishes cleanly and the filesystem stays read-write. The restart must finish within the device's `cmd_time_out` (30 s by default). |
| `grow.sh` | Grows the disk to `NEW_SIZE`, then runs `resize2fs` and a short fio run. `MODE=resize` sets the backstore's `dev_size` attribute (`targetcli ... set attribute dev_size=`, falling back to configfs `attrib/dev_size`) and saves the config; the kernel passes it to vdisk-server as a reconfig (with `ASPEN_FIRST=1`, `aspen grow-disk` runs first). `MODE=readd` re-creates the backstore at the larger size, so the add grows the disk. targetcli has no resize command for user backstores. |
| `teardown.sh` | Unmounts, logs out, deletes the target and backstore, and kills vdisk-server (`KEEP_SERVER=1` keeps it). It does not delete the Aspen disk or the D-Bus policy. |

## Example
    sudo CLIENT_CONFIG=$PWD/aspen-client-config.yaml scripts/vdisk-e2e/setup.sh
    sudo CLIENT_CONFIG=$PWD/aspen-client-config.yaml scripts/vdisk-e2e/restart-under-io.sh
    sudo CLIENT_CONFIG=$PWD/aspen-client-config.yaml NEW_SIZE=12G scripts/vdisk-e2e/grow.sh
    sudo CLIENT_CONFIG=$PWD/aspen-client-config.yaml NEW_SIZE=14G MODE=readd scripts/vdisk-e2e/grow.sh
    sudo scripts/vdisk-e2e/teardown.sh

The vdisk-server log, `/var/log/aspen-vdisk-server.log` by default, records each device add, reconfig and failure.
`dmesg` shows the kernel's side.
