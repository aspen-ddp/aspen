#!/usr/bin/env bash
# Serves an existing Aspen disk over iSCSI through vdisk-server's native TCMU frontend, then formats, mounts and
# runs the fio smoke job against it. Run as root. Idempotent: steps already done are skipped, and an existing
# filesystem is kept unless MKFS=1.
#
# Variables (see common.sh for defaults): CLIENT_CONFIG (required), DISK, SIZE (must equal the disk's Aspen size, or
# be larger to grow the disk on add), BLOCK_SIZE (the disk's logical block size), CONTROL (the target_core_user
# control string), IQN, INITIATOR_IQN, MOUNT, ASPEN (the aspen launcher), NETWORK_CACHE_DIR, LOG, MKFS, SMOKE (0
# skips fio).
set -euo pipefail

# shellcheck source=common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

MKFS="${MKFS:-0}"
SMOKE="${SMOKE:-1}"

require_root
INITIATOR_FILE=/etc/iscsi/initiatorname.iscsi
if [[ -z ${INITIATOR_IQN:-} ]]; then
  [[ -r $INITIATOR_FILE ]] || die "$INITIATOR_FILE not found (is open-iscsi installed?); or set INITIATOR_IQN"
  INITIATOR_IQN="$(sed -n 's/^InitiatorName=//p' "$INITIATOR_FILE")"
  [[ -n $INITIATOR_IQN ]] || die "no InitiatorName= in $INITIATOR_FILE; set INITIATOR_IQN"
fi
refuse_tcmu_runner

modprobe target_core_user
modprobe iscsi_target_mod

# The bus policy that lets root own org.kernel.TCMUService1 and targetcli call it
install -m 0644 "$REPO/deploy/dbus/org.kernel.TCMUService1.conf" /etc/dbus-1/system.d/
busctl call org.freedesktop.DBus /org/freedesktop/DBus org.freedesktop.DBus ReloadConfig

start_server
check_dbus

create_backstore "$SIZE"
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
  # The default portal targetcli adds (0.0.0.0:3260, or [::0]:3260 on newer versions) listens on every address, so it
  # conflicts. configfs names portals <ip>:<port>, with IPv6 addresses in brackets.
  for np in "/sys/kernel/config/target/iscsi/$IQN/tpgt_1/np/"*; do
    [[ -e $np ]] || continue
    portal="$(basename "$np")"
    ip="${portal%:*}"
    ip="${ip#[}"
    targetcli "$TPG/portals" delete "${ip%]}" "${portal##*:}"
  done
  targetcli "$TPG/portals" create 127.0.0.1
fi
targetcli saveconfig

login
dev="$(wait_for_device)"
echo "logged in to $IQN: $dev"

if [[ $MKFS == 1 || -z "$(blkid -o value -s TYPE "$dev" || true)" ]]; then
  mkfs.ext4 -F "$dev"
fi
mkdir -p "$MOUNT"
if ! mountpoint -q "$MOUNT"; then
  mount "$dev" "$MOUNT"
fi
echo "mounted $dev on $MOUNT"

if [[ $SMOKE == 1 ]]; then
  fio "$E2E_DIR/smoke.fio" --directory="$MOUNT"
fi
