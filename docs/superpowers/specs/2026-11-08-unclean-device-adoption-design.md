# Adopting a storage device without a clean shutdown

Date: 2026-11-08. Addresses the top item of Todo.txt. Builds on
`2026-10-28-physical-device-move-design.md` and `2026-11-07-per-device-crl-and-commit-ordering-design.md`, which made
this possible and listed it as a non-goal.

## Why

Every storage device now carries its own CRL (`<device>/crl`), so a drive holds all of its stores' transaction and
Paxos state. A drive pulled from a running or crashed host, then plugged into another host, is in exactly the state the
original host's crash would leave it in. Restarting the original host would just load it. Today another host refuses
it forever: `tryLoadDevice` refuses a directory whose `owning-host` names another host and that has no `shutdown.state`,
and `handleMarkerResolved` refuses a device pulled while `ShuttingDown`. The common case should be: yank the drive,
plug it in elsewhere, and have it adopted and back in service.

## Goals

- A device moved without a shutdown, from a crashed or a running host, is adopted by the host it is plugged into and
  put back into service with its in-flight transactions recovered from its CRL.
- A device pulled while `ShuttingDown` is adopted the same way.
- An old host that is still running stops serving the device once it sees it has been adopted.

## Non-goals

- Fencing the old host. Physical presence is enough (user's decision). A drive found in this host's
  `storage-devices/` is assumed to be unreachable by its old host. Dual-ported or SAN-attached drives, and cloned
  disks, are not defended against.
- Off-device CRLs (for example HDD CRLs on NVMe). Recorded in Backlog.txt; see "Off-device CRLs" below for the rule
  this design reserves for them.
- Any new repair mechanism. Updates missed while the drive is in transit are recorded and repaired like those missed
  during any host outage.

## Anti-goals

- Adopting a device whose CRL is not on it.
- Adopting a device the tree records as Failed, or as owned by a host other than the one its `owning-host` names (or
  this host).
- An old host writing anything for a device it no longer owns.

## Design

### Which devices are adopted

The rule is the same whether or not the device carries `shutdown.state`. A device is adopted when **the previous
host** (the host named by `shutdown.state` if present, otherwise by `owning-host`) is not this host, and:

1. the tree's `StorageDeviceState` is not Failed and its `hostId` is the previous host;
2. `<device>/crl` is a directory, or the device holds no store directories;
3. every Offline or Active entry has a store directory on the device (the existing `Unclean` refusal).

Shutdown is no longer special. A cleanly shut-down device meets these conditions just as an unclean one does. A
missing or malformed `owning-host` with no `shutdown.state` is still refused.

The CRL gate (2) makes the feature correct once CRLs can live off the device. A device whose shutdown was copying its
CRL onto the drive when the drive was pulled has no `<device>/crl` and is not adopted; it goes back to its host. A CRL
that is present but unreadable follows the existing lost-CRL rule after adoption (every store `rebuild-required`).

A device with no `shutdown.state` whose tree entry names **this** host was adopted earlier but its `owning-host` was
not yet rewritten (a crash between the commit and the rewrite). It is brought into service with the owner rewritten.
Any other combination is warned about once and retried silently on later scans.

### Discovery

A foreign `owning-host` goes through the same asynchronous lookup as `shutdown.state`: one lookup in flight per
directory, the device's stores marked awaiting their device, and the result handled as an event (the
`ShutdownMarkerResolved` event is generalised to `DeviceOwnershipResolved`, which records whether the host it names
came from `shutdown.state`).

### The adoption transaction

`DeviceMetadata.adoptDevice` is unchanged except for its precondition. It used to require `Shutdown` and the previous
host. It now requires "not Failed, owned by the previous host". As before: Offline entries become Active, status
becomes Operational, `hostId` becomes this host, pool entries and both HostStates are rewritten, the radicle is
updated for bootstrap stores, and stores whose state is lost are marked Rebuilding. Active, Rebuilding and
Transferring entries are left as a crash leaves them, and their existing recovery paths resume them. A ShuttingDown
device's shutdown is implicitly cancelled.

After the commit the old host gets a best-effort `CheckStorageDevice` nudge, and `StoreMoved` is gossiped as today.

### The old host

`DeviceReconciler.reconcileDeviceState` used to log and leave a loaded device that the tree records as owned by
another host. It now **relinquishes** the device:
- it stops the device's work (as the shutdown's first step);
- it closes its loaded stores and marks them offline, without gossiping `StoreOffline` (the stores live elsewhere now);
- it unloads the device and its CRL;
- it writes no marker, `shutdown.state` or tree state.

The directory is remembered in memory as disowned, so later scans do not load it again while its `owning-host` still
names this host (only possible if the drive is somehow still attached). Disowned entries are forgotten when the
directory disappears.

A running host whose drive is pulled sees its backends fail. Its existing fail-stop paths try to write
`rebuild-required` onto a drive that is gone, and they fail harmlessly. Without the marker, nothing flips a tree entry.
If an entry is flipped to Rebuilding anyway, the adopter rebuilds that store. That is wasteful but correct.

### Needs no new code

- Transactions in flight are recovered from the device CRL, and idle ones get drivers after `txHeartbeatTimeout`.
- Transactions that commit while the drive is in transit mark its stores in the ErrorTree (`errorTimeout`), and
  missed-update repair catches them up once the stores are served again.
- A host whose drive has vanished cannot vote for that drive's stores: votes follow a forced CRL write.

### Off-device CRLs (Backlog)

Shutting down a device whose CRL lives elsewhere must copy the CRL into `<device>/crl.incoming-<uuid>`, force it, and
atomically rename it to `<device>/crl` as its final step before `shutdown.state`. The gate above then refuses any
device pulled before that rename.

## Tests

- An Operational device moved without a shutdown, carrying an unresolved prepared transaction, is adopted. The
  transaction is recovered and resolves. Pools, both HostStates and `owning-host` name the new host.
- A device pulled while ShuttingDown is adopted.
- No `<device>/crl` with stores present: refused for both unclean and shut-down devices, and adopted once it appears.
- Refused: Failed; a tree owner that is neither the previous host nor this host; a missing `owning-host`.
- A crash between the adoption commit and the `owning-host` rewrite is finished by the next scan.
- A running old host relinquishes the device on its next check, writes nothing, and does not load it again.
