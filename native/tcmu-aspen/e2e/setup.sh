#!/usr/bin/env bash
# Creates the tcmu aspen backstore, iSCSI target, LUN, ACL and portal, then logs in.
# Run as root. Idempotent. Variables: DISK, SIZE, BLOCK_SIZE (the disk's logical block size), CONTROL (the
# target_core_user control string; defaults to hw_block_size=$BLOCK_SIZE), IQN, SOCKET, INITIATOR_IQN.
set -euo pipefail

DISK="${DISK:-vd1}"
SIZE="${SIZE:-10G}"
BLOCK_SIZE="${BLOCK_SIZE:-4096}"
CONTROL="${CONTROL:-hw_block_size=$BLOCK_SIZE}"
IQN="${IQN:-iqn.2026-10.org.aspen-ddp:${DISK}}"
SOCKET="${SOCKET:-/run/aspen/vdisk.sock}"
INITIATOR_IQN="${INITIATOR_IQN:-$(sed -n 's/^InitiatorName=//p' /etc/iscsi/initiatorname.iscsi)}"

[[ $EUID -eq 0 ]] || { echo "run as root" >&2; exit 1; }
[[ -n "$INITIATOR_IQN" ]] || { echo "set INITIATOR_IQN" >&2; exit 1; }
[[ -S "$SOCKET" ]] || { echo "vdisk-server socket $SOCKET not found; start 'aspen vdisk-server'" >&2; exit 1; }

if ! targetcli "/backstores/user:aspen/$DISK" ls >/dev/null 2>&1; then
  targetcli "/backstores/user:aspen" create "name=$DISK" "size=$SIZE" "cfgstring=$DISK@$SOCKET" \
    "control=$CONTROL"
fi
if ! targetcli "/iscsi/$IQN" ls >/dev/null 2>&1; then
  targetcli /iscsi create "$IQN"
fi
TPG="/iscsi/$IQN/tpg1"
if ! targetcli "$TPG/luns/lun0" ls >/dev/null 2>&1; then
  targetcli "$TPG/luns" create "/backstores/user:aspen/$DISK"
fi
if ! targetcli "$TPG/acls/$INITIATOR_IQN" ls >/dev/null 2>&1; then
  targetcli "$TPG/acls" create "$INITIATOR_IQN"
fi
if ! targetcli "$TPG/portals/127.0.0.1:3260" ls >/dev/null 2>&1; then
  # the default 0.0.0.0:3260 portal, if present, would conflict
  targetcli "$TPG/portals/0.0.0.0:3260" delete >/dev/null 2>&1 || true
  targetcli "$TPG/portals" create 127.0.0.1
fi
targetcli saveconfig

iscsiadm -m discovery -t st -p 127.0.0.1 >/dev/null
if ! iscsiadm -m session 2>/dev/null | grep "$IQN" >/dev/null; then
  iscsiadm -m node -T "$IQN" -p 127.0.0.1 --login
fi
echo "logged in to $IQN; find the disk with lsblk"
