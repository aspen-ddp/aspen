package org.aspen_ddp.aspen.client.internal.read

import java.util.UUID

import org.aspen_ddp.aspen.client.{ObjectState, ReadError}
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.network.ReadResponse
import org.aspen_ddp.aspen.common.objects.ObjectPointer
import org.aspen_ddp.aspen.common.store.StoreId

trait ObjectReader {
  val pointer: ObjectPointer

  val allStores: Set[StoreId] = pointer.hostingStores.toSet

  def receivedResponseFrom(storeId: StoreId): Boolean

  def noResponses: Set[StoreId]

  def rereadCandidates: Map[StoreId, HLCTimestamp]

  def result: Option[Either[ReadError, ObjectState]]

  def receiveReadResponse(response:ReadResponse): Option[Either[ReadError, ObjectState]]

  def numResponses: Int

  def receivedResponsesFromAllStores: Boolean = numResponses == pointer.ida.width

  def debugLogStatus(header: String, log: String => Unit): Unit
}

