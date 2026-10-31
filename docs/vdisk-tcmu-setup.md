# Serving Aspen virtual disks over iSCSI

This guide exports an Aspen virtual disk as an iSCSI LUN through LIO and tcmu-runner. The path is:

    initiator -> LIO (kernel) -> tcmu-runner -> handler_aspen.so -> /run/aspen/vdisk.sock -> aspen vdisk-server -> Aspen

Steps 3 to 6 need root. `native/tcmu-aspen/e2e/setup.sh` does steps 4 and 5 (target setup and login) and
`teardown.sh` undoes them.

**Status: the root end-to-end run (steps 3 to 6, the e2e scripts, growing and daemon restart) has not been
performed yet.** Everything below is checked only against the tcmu-runner 1.5.4 and targetcli-fb 3.0.1 sources and the
non-root tests. Items marked *verify on first run* could not be checked without root.

## Prerequisites
* Kernel modules `target_core_user` and `iscsi_target_mod` (`sudo modprobe target_core_user iscsi_target_mod`).
* tcmu-runner 1.5.4, active (`systemctl status tcmu-runner`).
* `targetcli` (targetcli-fb), `open-iscsi` (`iscsiadm`), `fio`, and build tools (`make`, `gcc`). The tcmu-runner headers come from the
  submodule.
* A running Aspen system and a client config (`aspen-client-config.yaml`).
* A storage pool with a maximum object size. For example, with an existing device set `<devset>`:

      aspen create-pool <client-config> vdisk-pool random-access-reed-solomon 16 10 12 <devset> \
          --backend fsif --max-object-size 5120000 --segment-count <n>

  (The client config file is the first argument of every command that talks to a running system. `create-pool` also
  requires `--segment-count`, the objects each store can hold.)

## 1. Create the disk
    aspen create-disk <client-config> vd1 10G --pool vdisk-pool

The size must be a multiple of the logical block size (4096 by default; `--block-size 512` is available).
`aspen list-disks <client-config>` shows it. The kernel's tcmu device must use the same block size, which step 4 sets
with `hw_block_size`.

## 2. Start the daemon
    aspen vdisk-server <client-config> --socket /run/aspen/vdisk.sock --network-cache-dir /var/lib/aspen/vdisk-cache

The socket is created mode 0600, so run the daemon as the user tcmu-runner runs as (normally root). Its parent
directory is created mode 0700 if missing. An existing parent must be owned by the daemon's user and not writable by
its group or others, or the daemon refuses to start, since another user could otherwise plant a socket there first.
It also fails to start if another daemon holds the socket.

## 3. Build and install the handler
    git submodule update --init submodules/tcmu-runner
    make -C native/tcmu-aspen
    sudo make -C native/tcmu-aspen install
    sudo systemctl restart tcmu-runner

The handler lands in `/usr/lib/x86_64-linux-gnu/tcmu-runner/handler_aspen.so`.

## 4. Create the backstore, target, LUN, ACL and portal
The size given to targetcli must equal the disk's Aspen size exactly (`10G` here), and the kernel block size
(`hw_block_size`, 512 unless set) must equal the disk's logical block size (4096 by default), or OPEN fails with a size
mismatch. The cfgstring is `<disk>[@<socket path>]`.

    sudo targetcli /backstores/user:aspen create name=vd1 size=10G cfgstring=vd1@/run/aspen/vdisk.sock \
        control=hw_block_size=4096

`control=` is a comma-separated list of target_core_user options written to the device's configfs `control` file
before it is enabled (targetcli-fb 3.0.1 `create` takes `control=` and `hw_max_sectors=`). Larger transfers need
`hw_max_sectors`: the default of 128 sectors caps each command at 512 KiB with 4 KiB blocks. For example
`control=hw_block_size=4096,hw_max_sectors=2048` allows 8 MiB (*verify on first run*; the kernel may also cap it).
The other commands are:

    sudo targetcli /iscsi create iqn.2026-10.org.aspen-ddp:vd1
    sudo targetcli /iscsi/iqn.2026-10.org.aspen-ddp:vd1/tpg1/luns create /backstores/user:aspen/vd1
    sudo targetcli /iscsi/iqn.2026-10.org.aspen-ddp:vd1/tpg1/acls create <initiator-iqn>
    sudo targetcli /iscsi/iqn.2026-10.org.aspen-ddp:vd1/tpg1/portals create 127.0.0.1
    sudo targetcli saveconfig

On a loopback test the ACL is the initiator's name in `/etc/iscsi/initiatorname.iscsi`. `setup.sh` does this step and
step 5 (set `BLOCK_SIZE` and `CONTROL` to change its `control=` string).
Older targetcli versions may need `set attribute authentication=0 generate_node_acls=0` on the TPG
(`/iscsi/<iqn>/tpg1`) before the initiator can log in.

## 5. Log in, format, mount
    sudo iscsiadm -m discovery -t st -p 127.0.0.1
    sudo iscsiadm -m node -T iqn.2026-10.org.aspen-ddp:vd1 -p 127.0.0.1 --login
    lsblk                      # find the new disk, e.g. /dev/sdX
    sudo mkfs.ext4 /dev/sdX
    sudo mkdir -p /mnt/vd1
    sudo mount /dev/sdX /mnt/vd1

## 6. Exercise it
    sudo fio native/tcmu-aspen/e2e/smoke.fio --directory=/mnt/vd1
    # then, for example, unpack and build a Linux kernel tree under /mnt/vd1

Expected: individual writes well under 100 ms, a FLUSH within a couple of seconds, and no stalls.

## Behavior to know about
* **Write cache.** The device reports a volatile write cache (WCE=1) and honors FUA. A WRITE completes from memory;
  data is durable at FLUSH or FUA. Unflushed data may be lost if the daemon crashes.
* **FUA never arrives from Linux initiators (known limitation).** tcmu-runner 1.5.4's MODE SENSE leaves the DPOFUA bit
  at 0, so the Linux `sd` driver never sets FUA in a CDB: it emulates each FUA write (REQ_FUA) as a WRITE followed by
  SYNCHRONIZE CACHE, which the daemon serves as a FLUSH of the whole write cache. The handler's FUA path is correct
  but unused with Linux initiators. Keep this in mind for the part-4 latency measurements: an fsync-heavy workload
  pays a full FLUSH where a FUA write would only commit the written blocks.
* **UNMAP** releases whole block objects and zeroes partial ranges. Linux issues WRITE ZEROES as WRITE SAME(16) with
  the UNMAP bit. When its start and length are aligned to one block object it is passed on as an UNMAP; otherwise
  tcmu-runner writes zeros through ordinary WRITEs, which is correct but releases no objects.
* **Growing** (not yet tested end-to-end). Find the device's `user_<N>` directory with
  `ls /sys/kernel/config/target/core/` (each `user_<N>` holds the backstores of one user HBA; the one containing `vd1`
  is yours), then write the new size in bytes to its control file:
  `echo dev_size=<bytes> | sudo tee /sys/kernel/config/target/core/user_<N>/vd1/control`. tcmu-runner then calls the
  handler's reconfig, which sends RESIZE, and the daemon commits the grow in Aspen. Running `aspen grow-disk` first is
  also fine: a RESIZE to a size that is already committed succeeds and the daemon adopts it. Likewise, if a resize
  committed in Aspen but its reply was lost, repeating the same resize succeeds. Then rescan on the initiator
  (`sudo iscsiadm -m session --rescan`) and run `sudo resize2fs /dev/sdX`. Disks only grow: a smaller size fails.
  Until the tcmu size is raised to match, a reopen of a disk grown in Aspen fails with a size mismatch, so do the
  resize promptly after `grow-disk`.
* **Locking.** In this part the plugin takes the disk lock on open (single path). ALUA/multipath failover is a later part.
* **Daemon restart.** Restarting `vdisk-server` while the disk is mounted makes I/O return BUSY and tells tcmu-runner the
  connection is lost. tcmu-runner then disables the target portal group, which drops the iSCSI session, and keeps
  reopening the device until the daemon is back, when it re-enables the TPG. Unflushed data may be lost.
  I/O resumes, without ext4 errors, only if the initiator logs back in within open-iscsi's
  `node.session.timeo.replacement_timeout` (default 120 s, in `/etc/iscsi/iscsid.conf` or per node with
  `iscsiadm -m node -T <iqn> -o update -n node.session.timeo.replacement_timeout -v <s>`); after that the initiator
  fails outstanding I/O and ext4 sees errors.
* **Aspen stalls.** The kernel fails a command tcmu-runner has not completed within the tcmu `cmd_time_out` (default
  30 s, in `/sys/kernel/config/target/core/user_<N>/vd1/attrib/cmd_time_out`), so a long Aspen stall turns into I/O
  errors rather than a hang. Raise it if Aspen may stall for longer. Set it before the LUN is created, since the
  kernel refuses the change while the device is exported (*verify on first run*), and consider the initiator's own
  SCSI command timeout (`/sys/block/sdX/device/timeout`, 30 s by default) too.

## Teardown
    sudo umount /mnt/vd1
    sudo native/tcmu-aspen/e2e/teardown.sh

This logs out and deletes the target and backstore. It does not stop the daemon or tcmu-runner and does not delete the
Aspen disk (`aspen delete-disk vd1`).

## Troubleshooting
* **`journalctl -u tcmu-runner`** shows handler load errors and OPEN failures (`handler_aspen` messages, `-EINVAL`,
  `-ENOENT`). If the `user:aspen` backstore type does not exist, the handler did not load: reinstall and restart.
* **`dmesg`** shows kernel iSCSI/SCSI errors, ext4 errors, and tcmu timeouts.
* **Socket permissions.** The socket is 0600 and owned by the daemon's user. If the handler cannot connect (EACCES),
  run the daemon as the same user as tcmu-runner. A missing socket means the daemon is not running or `--socket`
  differs from the cfgstring path.
* **Size mismatch.** OPEN fails with `-EINVAL` when the targetcli size or block size differs from the disk's. The
  journal line carries the daemon's message, for example `OPEN vd1 failed with status 7: disk is 10737418240 bytes
  with 4096 byte blocks, client reported 10737418240 bytes with 512 byte blocks`. A 512 byte block size there means
  the backstore was created without `control=hw_block_size=4096`. Compare `aspen list-disks <client-config>` with the
  backstore and recreate the backstore with the exact size and block size (or, after `aspen grow-disk`, resize it as
  under Growing). `-ENOENT` means the daemon does not know the disk name.
* **Everything returns BUSY.** The daemon is down or restarting; start it and tcmu-runner reopens the device.
