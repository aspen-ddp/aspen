package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.server.StorageDeviceConfig
import org.aspen_ddp.aspen.server.store.backend.fsif.{DeviceJournalFiles, FsifDeviceJournal, FsifJournalBridge}

/** The device's FSIF journal on disk. Runs only as a job on the device's lane. Task 10 gives it the device-load open;
 *  until then `ensure` is a stub over the temporary FsifJournalBridge (Ruling R14). */
object FsifJournalDisk:
  /** Lazy creation for the first FSIF create: `io.fsifJournal`'s journal, the device's journal at `devicePath` opened
   *  (and first created, sized by `cfg`'s `fsif-journal-*` keys) when the slot is empty. An IOException when no usable
   *  journal is open. Stub: when `cfg` sets neither key, the bridge's small temporary default sizing, not
   *  `DeviceJournalFiles.sizingFor`'s 4 files of 256 MiB */
  def ensure(io: DeviceIo, devicePath: os.Path, cfg: StorageDeviceConfig): FsifDeviceJournal =
    FsifJournalBridge.ensure(io, devicePath,
      if cfg.fsifJournalFiles.isEmpty && cfg.fsifJournalFileSizeMb.isEmpty then FsifJournalBridge.defaultSizing
      else DeviceJournalFiles.sizingFor(cfg))
