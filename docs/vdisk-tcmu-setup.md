# Serving Aspen virtual disks over iSCSI

This guide exports an Aspen virtual disk as an iSCSI LUN through LIO and the kernel's TCMU user backstore. The path
is:

    initiator -> LIO (kernel) -> target_core_user ring (/dev/uioN) -> aspen vdisk-server -> Aspen

There is no tcmu-runner. `aspen vdisk-server` drives TCMU directly and provides the `org.kernel.TCMUService1` D-Bus
service that targetcli uses to find the `user:aspen` backstore type. It runs as root.

`scripts/vdisk-e2e/setup.sh` does steps 2 to 6 and `teardown.sh` undoes them; see `scripts/vdisk-e2e/README.md`.

**Status: the root end-to-end run (steps 2 to 6, the e2e scripts, growing and daemon restart) has not been
performed yet.** Everything below is checked only against the kernel's `target_core_user.h`, the tcmu-runner and
targetcli-fb sources, and the non-root tests. Items marked *verify on first run* could not be checked without root.

## Prerequisites
* Linux with the kernel modules `target_core_user` and `iscsi_target_mod`
  (`sudo modprobe target_core_user iscsi_target_mod`).
* `targetcli` (targetcli-fb), `open-iscsi` (`iscsiadm`) and `fio`.
* **tcmu-runner stopped and disabled**: `sudo systemctl disable --now tcmu-runner`. Both want the D-Bus name
  `org.kernel.TCMUService1` and the TCMU devices, so they cannot run on the same host. `vdisk-server` refuses to start
  while the name is owned.
* A running Aspen system and a client config (`aspen-client-config.yaml`).
* An allocation group with a maximum slice size, holding one or more storage pools. A disk's objects are spread
  across the group's pools, so it can grow larger than any one pool. For example, with an existing device set
  `<devset>`:

      aspen create-pool <client-config> vdisk-pool-1 random-access-reed-solomon 16 10 12 <devset> \
          --backend fsif --max-object-size 5120000 --segment-count <n>
      aspen show-pool <client-config> vdisk-pool-1        # note its "Max Slice Size"
      aspen create-allocation-group <client-config> vdisk-group 0 random-access-reed-solomon 16 10 12 \
          --max-slice-size <pool's max slice size>
      aspen add-pool-to-group <client-config> vdisk-pool-1 vdisk-group

  The group's IDA must match its pools', and its `--max-slice-size` must be no larger than any member's: it is a
  floor every member meets. Add more pools the same way as the disk grows. (The client config file is the first
  argument of every command that talks to a running system. `create-pool` also requires `--segment-count`, the
  objects each store can hold.)

## 1. Create the disk
    aspen create-disk <client-config> vd1 10G --group vdisk-group

The size must be a multiple of the logical block size (4096 by default; `--block-size 512` is available).
`aspen list-disks <client-config>` shows it. The kernel's TCMU device must use the same block size, which step 4 sets
with `hw_block_size`.

## 2. Install the D-Bus policy
    sudo install -m 0644 deploy/dbus/org.kernel.TCMUService1.conf /etc/dbus-1/system.d/
    sudo busctl call org.freedesktop.DBus /org/freedesktop/DBus org.freedesktop.DBus ReloadConfig

The policy lets root own `org.kernel.TCMUService1` and lets anyone (targetcli) call it. A distribution tcmu-runner
package's `/etc/dbus-1/system.d/tcmu-runner.conf` may already allow the same; leaving both installed is harmless.

## 3. Start the daemon
    sudo aspen vdisk-server <client-config> --network-cache-dir /var/lib/aspen/vdisk-cache

It must run as root (it maps the TCMU rings, writes configfs and owns the D-Bus name) and exits with
`Error: vdisk-server must run as root` otherwise. The `aspen` launcher passes `--enable-native-access=ALL-UNNAMED`,
which the Foreign Function & Memory calls need. Run `./aspen --help` once as yourself first, so the launcher's
classpath cache exists and root does not invoke sbt; under `sudo`, pass `JAVA_HOME` if java is not on root's PATH.

At startup it opens the TCMU netlink socket, takes the D-Bus name, serves any `user:aspen` devices that already exist
(see "Daemon restart"), and then prints `Serving virtual disks through TCMU`. It exits with an error if
`target_core_user` is not loaded or the D-Bus name is already owned
(`org.kernel.TCMUService1 is already owned on the system bus (is tcmu-runner running?)`).

Check that targetcli sees the backstore type:

    busctl introspect org.kernel.TCMUService1 /org/kernel/TCMUService1/aspen   # CheckConfig, ConfigDesc
    sudo targetcli ls /backstores                                               # shows user:aspen

## 4. Create the backstore, target, LUN, ACL and portal
The cfgstring is just the disk name: targetcli adds the `aspen/` prefix, so the kernel's cfgstring is `aspen/vd1`.
(Typing `cfgstring=aspen/vd1` would look for a disk named `aspen/vd1`.) targetcli asks vdisk-server's `CheckConfig`
first, which refuses an unknown disk. The old `vd1@<socket>` form is rejected.

    sudo targetcli /backstores/user:aspen create name=vd1 size=10G cfgstring=vd1 control=hw_block_size=4096

* **Block size.** The kernel block size (`hw_block_size`, 512 unless set) must equal the disk's logical block size
  (4096 by default), or the create fails.
* **Size.** A targetcli size equal to the Aspen size is the normal case. A smaller size is served as given (the
  initiator sees only that much). A **larger size grows the Aspen disk** to match when the device is added; the log
  shows a warning with both sizes. Grow cannot be undone, so check the size.

`control=` is a comma-separated list of target_core_user options written to the device's configfs `control` file
before it is enabled. Larger transfers need `hw_max_sectors`: the default of 128 sectors caps each command at 512 KiB
with 4 KiB blocks. For example `control=hw_block_size=4096,hw_max_sectors=2048` allows 8 MiB (*verify on first run*;
the kernel may also cap it). The other commands are:

    sudo targetcli /iscsi create iqn.2026-10.org.aspen-ddp:vd1
    sudo targetcli /iscsi/iqn.2026-10.org.aspen-ddp:vd1/tpg1/luns create /backstores/user:aspen/vd1
    sudo targetcli /iscsi/iqn.2026-10.org.aspen-ddp:vd1/tpg1/acls create <initiator-iqn>
    sudo targetcli /iscsi/iqn.2026-10.org.aspen-ddp:vd1/tpg1/portals create 127.0.0.1
    sudo targetcli saveconfig

On a loopback test the ACL is the initiator's name in `/etc/iscsi/initiatorname.iscsi`. Older targetcli versions may
need `set attribute authentication=0 generate_node_acls=0` on the TPG (`/iscsi/<iqn>/tpg1`) before the initiator can
log in.

## 5. Log in, format, mount
    sudo iscsiadm -m discovery -t st -p 127.0.0.1
    sudo iscsiadm -m node -T iqn.2026-10.org.aspen-ddp:vd1 -p 127.0.0.1 --login
    lsblk                      # find the new disk, e.g. /dev/sdX
    sudo mkfs.ext4 /dev/sdX
    sudo mkdir -p /mnt/vd1
    sudo mount /dev/sdX /mnt/vd1

## 6. Exercise it
    sudo fio scripts/vdisk-e2e/smoke.fio --directory=/mnt/vd1
    # then, for example, unpack and build a Linux kernel tree under /mnt/vd1

Expected: individual writes well under 100 ms, a FLUSH within a couple of seconds, and no stalls. Check `dmesg` after
the first ext4 mount (lazy-init zeroout).

## Behavior to know about
* **Write cache and FUA.** The device reports a volatile write cache (WCE=1) and advertises FUA (DPOFUA in MODE
  SENSE), so Linux initiators send real FUA writes instead of WRITE + SYNCHRONIZE CACHE. A WRITE completes from
  memory; data is durable at SYNCHRONIZE CACHE or a FUA write. Unflushed data is lost if the daemon crashes.
* **UNMAP and WRITE SAME.** The device reports thin provisioning (LBPME, LBPRZ: unmapped blocks read as zero) with an
  unmap granularity of one block object. UNMAP releases whole block objects and zeroes partial ranges. Linux issues
  WRITE ZEROES as WRITE SAME(16) with the UNMAP bit; when its start and length are aligned to one block object it is
  served as an UNMAP, otherwise as ordinary writes of zeros. FORMAT UNIT unmaps the whole device. COMPARE AND WRITE
  is supported.
* **Growing.** Disks only grow; a smaller size fails. Any of these works:
  * Set the backstore's size: `sudo targetcli /backstores/user:aspen/vd1 set attribute dev_size=<bytes>` (if
    targetcli refuses, write the bytes to `/sys/kernel/config/target/core/user_<N>/vd1/attrib/dev_size`; the
    `user_<N>` directory containing `vd1` is yours). The kernel sends vdisk-server a reconfig, which commits the grow
    in Aspen before replying; the initiator then gets a CAPACITY DATA HAS CHANGED unit attention
    (*verify on first run*). targetcli has no resize command for user backstores.
  * Run `aspen grow-disk <client-config> vd1 <new-size>` first, then set `dev_size` as above. Until then the device
    keeps serving its old size, and a daemon restart in between still opens it (a kernel size smaller than the Aspen
    size is fine).
  * Re-create the backstore at the larger size: the add grows the Aspen disk to match.

  Then rescan on the initiator (`sudo iscsiadm -m session --rescan`) and run `sudo resize2fs /dev/sdX`. Run
  `sudo targetcli saveconfig` so a restored configuration has the new size. `scripts/vdisk-e2e/grow.sh` does all
  three (`MODE=resize`, `ASPEN_FIRST=1`, `MODE=readd`).
* **Locking.** The device takes the disk's ownership (fence) when it is added, which fences out any other server of the
  same disk (single path). ALUA/multipath failover is a later part.
* **Daemon restart.** vdisk-server is crash-only: stop it with `kill -9`; there is no clean shutdown. While it is
  down the kernel keeps the devices and queues commands, and the initiator stalls. On restart it finds the existing
  `aspen` UIO devices, resets each ring (the kernel fails the queued commands back for retry), takes ownership again
  and serves. Writes that were not yet flushed are lost, as on power loss. The restart must finish within the
  device's `cmd_time_out` (below), or the kernel fails the outstanding commands and the filesystem sees I/O errors.
  `scripts/vdisk-e2e/restart-under-io.sh` tests this.
* **Aspen stalls.** The kernel fails a command vdisk-server has not completed within the TCMU `cmd_time_out` (default
  30 s, in `/sys/kernel/config/target/core/user_<N>/vd1/attrib/cmd_time_out`), so a long Aspen stall turns into I/O
  errors rather than a hang. Raise it if Aspen may stall or restart for longer. Set it before the LUN is created,
  since the kernel refuses the change while the device is exported (*verify on first run*), and consider the
  initiator's own SCSI command timeout (`/sys/block/sdX/device/timeout`, 30 s by default) too.

## Teardown
    sudo umount /mnt/vd1
    sudo scripts/vdisk-e2e/teardown.sh

This logs out, deletes the target and backstore, and kills vdisk-server (`KEEP_SERVER=1` keeps it). It does not
delete the Aspen disk (`aspen delete-disk <client-config> vd1`) or the D-Bus policy.

## Troubleshooting
* **The vdisk-server log** records each device add, remove and reconfig, and every failure. A failed add names the
  step and errno, for example `tcmu: adding device vd1 (uio0, disk vd1) failed at check geometry: errno 22:
  configfs block size 512 does not match the disk's 4096` (the backstore was created without
  `control=hw_block_size=4096`). errno 2 (`ENOENT`) means the disk does not exist; 110 (`ETIMEDOUT`) means Aspen did
  not open, hand over or grow the disk within 60 s. A remove whose release takes longer than 60 s closes the disk
  without releasing it and still succeeds.
* **`targetcli ls /backstores` has no `user:aspen`.** vdisk-server is not running, or the D-Bus policy is not
  installed (vdisk-server then fails to start with the D-Bus error).
* **vdisk-server will not start.** "already owned on the system bus": tcmu-runner (or another vdisk-server) is
  running. "resolve TCM-USER family": `target_core_user` is not loaded. "must run as root": run it with sudo.
* **`dmesg`** shows kernel iSCSI/SCSI errors, ext4 errors, and TCMU timeouts.
* **A command fails with sense data and the log shows a ring dump** (`malformed-entry`, `ring-corrupt`,
  `command-error` and so on). That command failed and the device kept serving. Repeats of the same kind within a minute
  are only counted, then summarized in one warning line; the first, full line has the details and, for an unexpected
  exception (`command-error`), its stack trace. Every line names the device, its UIO minor and its disk.
