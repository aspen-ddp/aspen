#!/usr/bin/env bash
# Unmounts, logs out and deletes the iSCSI target and the aspen backstore, then kills vdisk-server. Run as root.
# Idempotent. Does not delete the Aspen disk (aspen delete-disk) or the installed D-Bus policy.
# Variables: as setup.sh (DISK, IQN, MOUNT). KEEP_SERVER=1 leaves vdisk-server running.
set -euo pipefail

# shellcheck source=common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

KEEP_SERVER="${KEEP_SERVER:-0}"

require_root

if mountpoint -q "$MOUNT"; then
  umount "$MOUNT"
fi
logout
iscsiadm -m node -T "$IQN" -p 127.0.0.1 -o delete >/dev/null 2>&1 || true

if targetcli "/iscsi/$IQN" ls >/dev/null 2>&1; then
  targetcli /iscsi delete "$IQN"
fi
# Deleted before vdisk-server is killed, so that its remove releases the disk lock
delete_backstore
targetcli saveconfig
echo "removed $IQN and backstore $DISK"

if [[ $KEEP_SERVER != 1 ]]; then
  kill_server
fi
