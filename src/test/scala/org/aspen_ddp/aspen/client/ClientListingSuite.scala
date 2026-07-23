package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.IntegrationTestSuite

class ClientListingSuite extends IntegrationTestSuite:

  atest("listStorageDeviceSets returns created device sets by name, sorted"):
    for
      idB <- client.createStorageDeviceSet("set-b", 0, None)
      idA <- client.createStorageDeviceSet("set-a", 0, None)
      entries <- client.listStorageDeviceSets()
    yield
      // Only the two sets created here should be present; assert both are listed
      // with their typed ids, sorted by name.
      val relevant = entries.filter { (name, _) => name == "set-a" || name == "set-b" }
      relevant should be (List("set-a" -> idA, "set-b" -> idB))
