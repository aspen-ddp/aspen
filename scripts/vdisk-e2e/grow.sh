#!/usr/bin/env bash
# Grows the served disk to NEW_SIZE and the ext4 filesystem on it. Run as root after setup.sh.
#
# MODE=resize (default): write the new size to the backstore's configfs attrib/dev_size. The kernel sends
#   vdisk-server a reconfig, which grows the Aspen disk. With ASPEN_FIRST=1, `aspen grow-disk` grows the Aspen disk
#   first, and the reconfig then adopts the committed size. (targetcli has no resize command for user backstores.)
# MODE=readd: log out, delete the backstore and create it again with size NEW_SIZE. The add finds the targetcli
#   size larger than the Aspen disk and grows the disk to match (grow on add).
#
# Variables: as setup.sh, plus NEW_SIZE (required; K, M, G or T binary suffixes, as aspen and targetcli read them),
# MODE and ASPEN_FIRST.
set -euo pipefail

# shellcheck source=common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

NEW_SIZE="${NEW_SIZE:-}"
MODE="${MODE:-resize}"
ASPEN_FIRST="${ASPEN_FIRST:-0}"

require_root
[[ -n $NEW_SIZE ]] || die "set NEW_SIZE"
bytes="$(numfmt --from=iec "$NEW_SIZE")"
mountpoint -q "$MOUNT" || die "$MOUNT is not mounted; run setup.sh first"
server_pid >/dev/null || die "vdisk-server is not running; run setup.sh first"

case $MODE in
  resize)
    if [[ $ASPEN_FIRST == 1 ]]; then
      [[ -n $CLIENT_CONFIG ]] || die "set CLIENT_CONFIG"
      "$ASPEN" grow-disk "$CLIENT_CONFIG" "$DISK" "$NEW_SIZE"
    fi
    dir="$(backstore_dir)" || die "no configfs directory for backstore $DISK"
    echo "$bytes" > "$dir/attrib/dev_size"
    iscsiadm -m node -T "$IQN" -p 127.0.0.1 --rescan >/dev/null
    ;;
  readd)
    umount "$MOUNT"
    logout
    delete_backstore
    create_backstore "$NEW_SIZE"
    targetcli "/iscsi/$IQN/tpg1/luns" create "/backstores/user:aspen/$DISK"
    targetcli saveconfig
    login
    mount "$(wait_for_device)" "$MOUNT"
    ;;
  *)
    die "MODE must be resize or readd"
    ;;
esac

dev="$(wait_for_device)"
deadline=$((SECONDS + 30))
until [[ "$(blockdev --getsize64 "$dev")" == "$bytes" ]]; do
  ((SECONDS < deadline)) || die "$dev is $(blockdev --getsize64 "$dev") bytes, not $bytes"
  sleep 0.5
done
resize2fs "$dev"
echo "grew $DISK to $bytes bytes; $(df -h --output=size "$MOUNT" | tail -n 1 | tr -d ' ') on $MOUNT"
fio "$E2E_DIR/smoke.fio" --directory="$MOUNT" --runtime=15
