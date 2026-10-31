# Serving Aspen virtual disks over iSCSI

This guide exports an Aspen virtual disk as an iSCSI LUN through LIO and tcmu-runner. The path is:

    initiator -> LIO (kernel) -> tcmu-runner -> handler_aspen.so -> /run/aspen/vdisk.sock -> aspen vdisk-server -> Aspen

Steps 3 to 6 need root. `native/tcmu-aspen/e2e/setup.sh` and `teardown.sh` do steps 4 to 6 and the undo.

## Prerequisites
* Kernel modules `target_core_user` and `iscsi_target_mod` (`sudo modprobe target_core_user iscsi_target_mod`).
* tcmu-runner 1.5.4, active (`systemctl status tcmu-runner`).
* `targetcli` (targetcli-fb), `open-iscsi` (`iscsiadm`), `fio`, and build tools (`make`, `gcc`, libtcmu headers come
  from the submodule).
* A running Aspen system and a client config (`aspen-client-config.yaml`).
* A storage pool with a maximum object size. For example, with an existing device set `<devset>`:

      aspen create-pool vdisk-pool random-access-reed-solomon 16 10 12 <devset> \
          --backend fsif --max-object-size 5120000 --segment-count <n> <client-config>

  (`create-pool` also requires `--segment-count`, the objects each store can hold.)

## 1. Create the disk
    aspen create-disk vd1 10G --pool vdisk-pool <client-config>

The size must be a multiple of the logical block size (4096 by default; `--block-size 512` is available).
`aspen list-disks` shows it.

## 2. Start the daemon
    aspen vdisk-server --socket /run/aspen/vdisk.sock --network-cache-dir /var/lib/aspen/vdisk-cache <client-config>

The socket is created mode 0600 and its parent directory is created if missing, so run the daemon as the user
tcmu-runner runs as (normally root). It fails to start if another daemon holds the socket.

## 3. Build and install the handler
    git submodule update --init submodules/tcmu-runner
    make -C native/tcmu-aspen
    sudo make -C native/tcmu-aspen install
    sudo systemctl restart tcmu-runner

The handler lands in `/usr/lib/x86_64-linux-gnu/tcmu-runner/handler_aspen.so`.

## 4. Create the backstore, target, LUN, ACL and portal
The size given to targetcli must equal the disk's Aspen size exactly (`10G` here), or OPEN fails with a size mismatch.
The cfgstring is `<disk>[@<socket path>]`.

    sudo targetcli /backstores/user:aspen create name=vd1 size=10G cfgstring=vd1@/run/aspen/vdisk.sock
    sudo targetcli /iscsi create iqn.2026-10.org.aspen-ddp:vd1
    sudo targetcli /iscsi/iqn.2026-10.org.aspen-ddp:vd1/tpg1/luns create /backstores/user/vd1
    sudo targetcli /iscsi/iqn.2026-10.org.aspen-ddp:vd1/tpg1/acls create <initiator-iqn>
    sudo targetcli /iscsi/iqn.2026-10.org.aspen-ddp:vd1/tpg1/portals create 127.0.0.1
    sudo targetcli saveconfig

(Depending on the targetcli version, the backstore path in the LUN command is `/backstores/user:aspen/vd1`.) On a
loopback test the ACL is the initiator's name in `/etc/iscsi/initiatorname.iscsi`. `setup.sh` does all of this.

## 5. Log in, format, mount
    sudo iscsiadm -m discovery -t st -p 127.0.0.1
    sudo iscsiadm -m node -T iqn.2026-10.org.aspen-ddp:vd1 -p 127.0.0.1 --login
    lsblk                      # find the new disk, e.g. /dev/sdX
    sudo mkfs.ext4 /dev/sdX
    sudo mount /dev/sdX /mnt/vd1

## 6. Exercise it
    sudo fio native/tcmu-aspen/e2e/smoke.fio --directory=/mnt/vd1
    # then, for example, unpack and build a Linux kernel tree under /mnt/vd1

Expected: individual writes well under 100 ms, a FLUSH within a couple of seconds, and no stalls.

## Behavior to know about
* **Write cache.** The device reports a volatile write cache (WCE=1) and honors FUA. A WRITE completes from memory;
  data is durable at FLUSH or FUA. Unflushed data may be lost if the daemon crashes.
* **UNMAP** releases whole block objects and zeroes partial ranges.
* **Growing.** `aspen grow-disk` is not enough on its own: use targetcli resize
  (`sudo targetcli /backstores/user:aspen/vd1 set attribute dev_size=...` / `resize`), which commits the new size to
  Aspen. Disks only grow.
* **Locking.** In this part the plugin takes the disk lock on open (single path). ALUA/multipath failover is a later part.
* **Daemon restart.** Restarting `vdisk-server` while the disk is mounted makes I/O return BUSY; tcmu-runner then
  reopens the device and I/O resumes. There should be no ext4 errors in `dmesg`, though unflushed data may be lost.

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
* **Size mismatch.** OPEN fails with `-EINVAL` when the targetcli size differs from the disk's Aspen size. Compare
  `aspen list-disks` with the backstore size and recreate the backstore with the exact size. `-ENOENT` means the daemon
  does not know the disk name.
* **Everything returns BUSY.** The daemon is down or restarting; start it and tcmu-runner reopens the device.
