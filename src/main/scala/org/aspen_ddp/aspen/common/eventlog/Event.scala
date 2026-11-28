package org.aspen_ddp.aspen.common.eventlog

import java.util.UUID

import org.aspen_ddp.aspen.client.RegisteredTypeFactory
import org.aspen_ddp.aspen.common.HLCTimestamp

/** A decoded system event. */
trait Event:
  def timestamp: HLCTimestamp
  def typeUUID: UUID

  /** Human readable description. Empty means display falls back to the entry's byte count. */
  def describe: String = ""

/** Decodes the entries of one event type into Events. */
trait EventFactory extends RegisteredTypeFactory:
  def createEvent(e: EventLogEntry): Event

/** Receives events as the event log service moves them into the log.
  *
  * Observers are best-effort: events committed just before a crash or service move are never observed.
  * `observe` must be fast and must not block.
  */
trait EventObserver:
  /** The event types of interest; None observes every type. */
  def observedTypes: Option[Set[UUID]]
  def observe(e: Event): Unit

/** Creates EventObservers from their persisted configuration.
  *
  * Observers are best-effort: events committed just before a crash or service move are never observed.
  * Created observers' `observe` must be fast and must not block.
  */
trait EventObserverFactory extends RegisteredTypeFactory:
  def createEventObserver(typeUUID: UUID, configData: Array[Byte]): EventObserver
