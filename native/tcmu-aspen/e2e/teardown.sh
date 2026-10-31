#!/usr/bin/env bash
# Logs out and deletes the iSCSI target and the aspen backstore. Run as root, with the
# filesystem unmounted. Idempotent. Does not stop the daemon or tcmu-runner.
set -euo pipefail

DISK="${DISK:-vd1}"
IQN="${IQN:-iqn.2026-10.org.aspen-ddp:${DISK}}"

[[ $EUID -eq 0 ]] || { echo "run as root" >&2; exit 1; }

if iscsiadm -m session 2>/dev/null | grep "$IQN" >/dev/null; then
  iscsiadm -m node -T "$IQN" -p 127.0.0.1 --logout
fi
iscsiadm -m node -T "$IQN" -p 127.0.0.1 -o delete >/dev/null 2>&1 || true

if targetcli "/iscsi/$IQN" ls >/dev/null 2>&1; then
  targetcli /iscsi delete "$IQN"
fi
if targetcli "/backstores/user:aspen/$DISK" ls >/dev/null 2>&1; then
  targetcli "/backstores/user:aspen" delete "$DISK"
fi
targetcli saveconfig
echo "removed $IQN and backstore $DISK"
