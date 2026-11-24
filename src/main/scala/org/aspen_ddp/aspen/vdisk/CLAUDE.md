### VDisks
- A virtual block device on Aspen. `DiskCatalog` names disks in registry namespace `vdisk`; CLI `create-disk`,
  `list-disks`, `grow-disk`, `delete-disk`. Full design: `DESIGN.md` here; setup: `docs/vdisk-tcmu-setup.md`
- Per disk: a KV config object (geometry plus a `DataObjectTKVL` block map) and a DataObject fence. The owner takes
  ownership by overwriting the fence and every commit carries a `SharedRevisionLock` on it, so a newer owner fences
  out older ones
- Write-back: WRITE completes from memory; durable at FLUSH/FUA. Block objects are the largest multiple of the
  logical block size <= the allocator's `maxObjectSize`
- A block's clean (`CleanBlock`) and dirty (`SegmentSnapshot`) pieces are kept sorted and non-overlapping, and are
  never coalesced: random 4 KiB writes or fetches leave a block as ~1000 pieces. Per-request and per-committed-write
  work (`VirtualDisk.gaps`, `covers`, `assemble`) must look only at the pieces in range (binary search), never pass
  over or sort all of them: that cost 60-70 us per cache-hit read and most of the server's CPU under random writes
- The block map's `LruTierCache(anyRoot = true)` relies on it being the single writer; `BlockMap.refresh` clears it
  (`client/tkvl/CLAUDE.md`)
- `vdisk-server` drives the kernel's TCMU interface directly through FFM (`vdisk.tcmu`: netlink, configfs, the
  `/dev/uioN` ring, SCSI emulation in `vdisk.tcmu.scsi`; Java parts under `src/main/java/.../vdisk`) and owns
  `org.kernel.TCMUService1` on the system D-Bus so targetcli sees `user:aspen`. It runs as root and refuses to start
  while tcmu-runner holds that name. `LockableBlockDevice` is the frontend seam
