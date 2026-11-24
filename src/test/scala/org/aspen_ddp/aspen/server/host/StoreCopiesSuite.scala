package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.FileBasedTests

class StoreCopiesSuite extends FileBasedTests:

  test("a copy whose stat fails is not reported deleted, and stays in place (SL-W24)"):
    val device = tdir / "device"
    val locked = tdir / "locked"
    os.makeDir.all(device)
    os.makeDir.all(locked / "copy")
    os.write(locked / "copy" / "marker", "")
    // The copy is reached through a link into a directory without search permission: its stat fails (EACCES)
    os.symlink(device / "copy", locked / "copy")
    os.perms.set(locked, "rw-------")
    val deleted =
      try CopyDisk.delete(device, device / "copy", "a test")
      finally os.perms.set(locked, "rwx------")
    deleted should be(false)
    os.exists(locked / "copy" / "marker") should be(true)

  test("a copy known absent is gone"):
    os.makeDir.all(tdir / "device")
    CopyDisk.delete(tdir / "device", tdir / "device" / "copy", "a test") should be(true)
