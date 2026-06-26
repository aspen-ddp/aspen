package org.aspen_ddp.aspen.compute

import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState, RegisteredTypeFactory}
import org.aspen_ddp.aspen.common.objects.KeyValueObjectPointer

trait DurableServiceFactory extends RegisteredTypeFactory:
  /** Construct and immediately start a service instance from persisted state.
   *  The service begins operating upon return. */
  def createService(
    client: AspenClient,
    statePointer: KeyValueObjectPointer,
    state: KeyValueObjectState
  ): DurableService
